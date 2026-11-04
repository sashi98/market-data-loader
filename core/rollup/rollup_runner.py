# core/rollup/rollup_runner.py
#
# Shared orchestration for STEP 5 (Weekly Bhav Copy Runner) and STEP 6
# (Monthly Bhav Copy Runner). The only real difference between weekly
# and monthly is which period_type/table names get passed in via
# `config` -- see either runner module's own CONFIG dict.
#
# BLIND-UPSERT REDESIGN 2026-09-06 -- per Sashikant's own confirmed
# call (review point 3): this module no longer runs its own internal
# freshness check (get_metadata_freshness() against the most-recently-
# fully-closed period) or discovers its own start point via
# fetch_earliest_trade_date(). Both the skip-vs-run decision AND the
# recompute window are now supplied entirely by
# bhavcopy_scheduler_main.py: it checks gap[exchange] OR this table's
# own freshness itself (via _run_rollup_step()), and always passes
# start_date=DEFAULT_START_DATE, end_date=LATEST_TRADE_DATE when it
# decides to run at all. This module is now a pure "recompute every
# period between these two dates, for this ONE exchange" blind
# executor.
#
# Single exchange per call now (was a `for exchange in EXCHANGES:` loop
# internally) -- the scheduler calls this once per exchange, same as
# STEP 2/3/4.
#
# INCLUDES THE CURRENTLY-OPEN PERIOD 2026-09-06 -- per Sashikant's own
# confirmed call (review point 3): "current week/month must be computed
# till the LATEST_TRADE_DATE." The old pending_period_starts() helper
# deliberately EXCLUDED the still-open current period (it only walked
# periods that had ALREADY fully closed as of `today`); that exclusion
# is no longer what's wanted here, so period discovery is done directly
# in this module now (_periods_between()) rather than via that helper.
# _roll_up_one_period() caps each period's own last_calendar_day at
# end_date (never at that period's natural, full calendar end) -- for
# every period except the very last one this is a no-op (the period has
# already fully closed, so its natural end is <= end_date anyway); for
# the last, still-open period, this is exactly what makes it compute
# "through LATEST_TRADE_DATE" rather than through its own future,
# not-yet-reached natural end. This also means bhav_copy_w_metadata /
# bhav_copy_m_metadata's own period_end_date column always reflects
# "how far this rollup has actually been computed," so
# get_metadata_freshness()'s existing MAX(period_end_date) query is
# directly reusable, unchanged, as the scheduler's own "is this table's
# target stale" check (see bhavcopy_scheduler_main.py's
# _run_rollup_step()) -- no new freshness-query code needed for this
# redesign.
#
# FULL RECOMPUTE, UNCHANGED FROM THE 2026-08-30 DESIGN -- every run
# still walks EVERY period in [start_date, end_date] from scratch,
# never resuming from a cursor: a corporate action discovered today can
# retroactively change adjustment factors for any historical date, so
# an incremental "only new periods" walk could leave an already-rolled-
# up week/month stale forever, never revisited again.

# MODE OF RECOMPUTE 2026-11-03 (TMT-MDL-US-0005) -- run() now takes mode_of_recompute:
#   FULL         as before: every period in [start_date, end_date] is recomputed for every stock.
#   INCREMENTAL  (a) list-A: only the periods from this table's latest stored period through end_date
#                are recomputed (normally just the current week / month, two periods on the first trading
#                day of a new one, more after missed runs); (b) list-B: the stocks STEP 4 queued in
#                recompute_queue for this target (corporate action, new stock, back-dated data, ...) are
#                rebuilt over their whole history. A queued '*' (whole exchange) or an empty table falls
#                back to FULL for this exchange.
#   SHADOW       FULL is written; the INCREMENTAL result is computed in memory first and compared with
#                what FULL wrote -> recompute_shadow_check + a verdict line.
# The queue entries for this target are cleared only after the run succeeded, so a crash simply leaves
# them to be picked up again next run. Before clearing, they are queued for this timeframe's RSI
# (DOWNSTREAM_OF_ROLLUP), which runs later in the cycle. The prior-close lookup is batched (one query per period instead of
# one per stock per period), which speeds up FULL too.

import time
from datetime import datetime as _dt, timedelta

import pandas as pd

from core.date_format import fmt_date

from core.db_client import get_connection, DbConnectionError
from core.recompute import mode as recompute_mode
from core.recompute import queue as recompute_queue
from core.recompute import shadow
from core.rollup.period import period_start, next_period_start
from core.rollup.rollup_calculator import compute_rollup, _percent_change
from core.rollup.rollup_persistence import (
    fetch_daily_rows, fetch_daily_rows_for_securities, fetch_prior_close_lookup, fetch_latest_period_start,
    fetch_rollup_rows_by_keys, upsert_rollup_rows, upsert_metadata, record_run_audit, RollupPersistenceError,
)

LIST_B_BATCH_SIZE = 300
STORED_PRICE_DECIMALS = 2   # bhav_copy_w / bhav_copy_m prices are NUMERIC(20, 2)

SHADOW_VALUE_COLUMNS = ["open", "high", "low", "close", "last", "prev_close", "tot_trd_qty", "tot_trd_val",
                        "total_trades", "ltp_percent_change"]
SHADOW_TOLERANCES = {"open": 0.0051, "high": 0.0051, "low": 0.0051, "close": 0.0051, "last": 0.0051,
                     "prev_close": 0.0051, "tot_trd_qty": 1.0, "tot_trd_val": 0.0051, "total_trades": 0.5,
                     "ltp_percent_change": 0.0101}


def _periods_between(start_date, end_date, period_type):
    """
    Every period-start from start_date's own period through end_date's
    own period, INCLUSIVE on both ends -- deliberately includes the
    final (possibly still-open) period, unlike the old
    pending_period_starts() helper. Ordered ascending (oldest first).
    """
    if start_date > end_date:
        return []
    periods = []
    p = period_start(start_date, period_type)
    last = period_start(end_date, period_type)
    while p <= last:
        periods.append(p)
        p = next_period_start(p, period_type)
    return periods


def _compute_one_period(conn, config, exchange, period_trade_date, end_date, carried_closes=None):
    """
    Rolls up ONE period for every stock (no writes). carried_closes {security_id: close} overrides the
    stored prior close (SHADOW: the previous in-memory period, not yet in the table).
    Returns (rollup_df, daily_row_count, last_calendar_day) or None when there is nothing / a failure.
    """
    period_type = config["period_type"]
    label = config["label"]
    natural_last_calendar_day = next_period_start(period_trade_date, period_type) - timedelta(days=1)
    # Cap at end_date -- see this module's header, "INCLUDES THE CURRENTLY-OPEN PERIOD."
    last_calendar_day = min(natural_last_calendar_day, end_date)

    try:
        df = fetch_daily_rows(conn, exchange, from_date=period_trade_date, to_date=last_calendar_day)
    except RollupPersistenceError as e:
        print(f"    [{label}/{exchange}] FAILED fetching daily rows for {fmt_date(period_trade_date)}: {e}")
        return None
    if df.empty:
        print(f"    [{label}/{exchange}] {fmt_date(period_trade_date)}: no eligible daily rows yet -- skipping.")
        return None

    security_ids = df["security_id"].unique().tolist()
    carried_closes = carried_closes or {}
    keys = [(sid, exchange, period_trade_date) for sid in security_ids if sid not in carried_closes]
    try:
        prior_close_lookup = fetch_prior_close_lookup(conn, config["rollup_table"], keys)
    except RollupPersistenceError as e:
        print(f"    [{label}/{exchange}] FAILED fetching prior-close lookup for {fmt_date(period_trade_date)}: {e}")
        return None
    for sid, close in carried_closes.items():
        prior_close_lookup[(sid, exchange, period_trade_date)] = close

    return compute_rollup(df, period_type, prior_close_lookup=prior_close_lookup), len(df), last_calendar_day


def _roll_up_one_period(conn, config, exchange, period_trade_date, end_date):
    label = config["label"]
    run_started_at = time.time()
    computed = _compute_one_period(conn, config, exchange, period_trade_date, end_date)
    if computed is None:
        return False
    rollup_df, daily_count, last_calendar_day = computed

    try:
        written_count = upsert_rollup_rows(conn, config["rollup_table"], rollup_df)
    except RollupPersistenceError as e:
        print(f"    [{label}/{exchange}] FAILED upserting {fmt_date(period_trade_date)}: {e}")
        return False

    processing_time_ms = int((time.time() - run_started_at) * 1000)
    try:
        upsert_metadata(conn, config["metadata_table"], period_trade_date, exchange, written_count, last_calendar_day,
                         processing_time_ms=processing_time_ms, status="SUCCESS")
        record_run_audit(conn, config["run_audit_table"], exchange, period_trade_date,
                          _dt.fromtimestamp(run_started_at), _dt.now(), written_count, daily_count, status="SUCCESS")
    except RollupPersistenceError as e:
        print(f"    [{label}/{exchange}] WARNING: rollup written, but metadata/run_audit record failed: {e}")
        return False

    print(f"    [{label}/{exchange}] {fmt_date(period_trade_date)}: rolled up {written_count} rows "
          f"from {daily_count} daily rows ({rollup_df['security_id'].nunique()} securities), "
          f"through {fmt_date(last_calendar_day)}.")
    return True


def _roll_up_periods(conn, config, exchange, periods, end_date):
    """Writes each period in order; False on the first failure (later periods need its closes)."""
    for period_trade_date in periods:
        if not _roll_up_one_period(conn, config, exchange, period_trade_date, end_date):
            return False
    return True


def _compute_periods_in_memory(conn, config, exchange, periods, end_date):
    """SHADOW: what _roll_up_periods() would write, without writing (each period's closes carried forward)."""
    frames, carried = [], {}
    for period_trade_date in periods:
        computed = _compute_one_period(conn, config, exchange, period_trade_date, end_date, carried_closes=carried)
        if computed is None:
            continue
        rollup_df = computed[0]
        frames.append(rollup_df)
        carried.update({sid: round(float(c), STORED_PRICE_DECIMALS)
                        for sid, c in zip(rollup_df["security_id"], rollup_df["close"]) if pd.notna(c)})
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def _rebuild_securities(conn, config, exchange, security_ids, start_date, end_date, write):
    """
    list-B: every period of the given stocks, from the whole daily history at once. prev_close is the
    previous period's close as STORED (rounded to the table's 2 decimals) -- exactly what FULL's
    period-by-period lookup reads back -- so both paths give the same row.
    write=True upserts and returns the row count; write=False returns the DataFrame (SHADOW).
    """
    label = config["label"]
    sids = sorted(security_ids)
    frames, written = [], 0
    from_date = period_start(start_date, config["period_type"])
    for i in range(0, len(sids), LIST_B_BATCH_SIZE):
        batch = sids[i:i + LIST_B_BATCH_SIZE]
        df = fetch_daily_rows_for_securities(conn, exchange, batch, from_date=from_date, to_date=end_date)
        if df.empty:
            continue
        rollup_df = compute_rollup(df, config["period_type"])
        stored_close = rollup_df["close"].astype(float).round(STORED_PRICE_DECIMALS)
        rollup_df["prev_close"] = stored_close.groupby([rollup_df["security_id"], rollup_df["exchange"]]).shift(1)
        rollup_df["ltp_percent_change"] = _percent_change(rollup_df["close"].astype(float), rollup_df["prev_close"])
        if write:
            written += upsert_rollup_rows(conn, config["rollup_table"], rollup_df)
        else:
            frames.append(rollup_df)
    if write:
        print(f"    [{label}/{exchange}] list-B: rebuilt {len(sids):,} stock(s) over their whole history "
              f"({written:,} row(s)).")
        return written
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def _shadow_compare(conn, config, exchange, candidate, end_date, list_a_count, list_b_count):
    target = config["target"]
    actual = pd.DataFrame()
    if not candidate.empty:
        try:
            actual = fetch_rollup_rows_by_keys(conn, config["rollup_table"], exchange,
                                               candidate["security_id"].tolist(), candidate["trade_date"].tolist())
        except RollupPersistenceError as e:
            print(f"  [WARN] SHADOW {target}/{exchange}: {e}")
            return
    result = shadow.compare(candidate, actual, ["security_id", "trade_date"], SHADOW_VALUE_COLUMNS, SHADOW_TOLERANCES)
    print(f"  {shadow.record(conn, end_date, target, exchange, result, list_a_count, list_b_count)}")


def _run_for_one_exchange(env_values, config, exchange, start_date, end_date, mode):
    label = config["label"]
    target = config["target"]
    period_type = config["period_type"]
    try:
        conn = get_connection(env_values)
    except DbConnectionError as e:
        print(f"  [{label}/{exchange}] FAILED to open connection: {e}")
        return False

    try:
        all_periods = _periods_between(start_date, end_date, period_type)
        if not all_periods:
            print(f"  [{label}/{exchange}] Nothing to compute for [{fmt_date(start_date)}, {fmt_date(end_date)}].")
            return True

        try:
            queued = recompute_queue.fetch_queued(conn, exchange, target)
            latest = fetch_latest_period_start(conn, config["rollup_table"], exchange)
        except (recompute_queue.RecomputeQueueError, RollupPersistenceError) as e:
            print(f"  [{label}/{exchange}] FAILED reading recompute_queue / latest period: {e}")
            return False

        whole_exchange = recompute_queue.ALL in queued or latest is None
        list_b = set() if whole_exchange else set(queued.keys())
        window = [] if whole_exchange else [p for p in all_periods if p >= latest]

        candidate = pd.DataFrame()
        if mode == recompute_mode.SHADOW:
            if whole_exchange:
                print(f"  [{label}/{exchange}] SHADOW: whole exchange queued ({queued.get(recompute_queue.ALL, 'empty table')}) "
                      f"-- INCREMENTAL would rebuild everything too; nothing to compare.")
            else:
                print(f"  [{label}/{exchange}] SHADOW: computing INCREMENTAL in memory "
                      f"({len(window)} period(s) + {len(list_b):,} list-B stock(s)) ...")
                window_rows = _compute_periods_in_memory(conn, config, exchange, window, end_date)
                if not window_rows.empty and list_b:
                    window_rows = window_rows[~window_rows["security_id"].isin(list_b)]
                rebuilt = _rebuild_securities(conn, config, exchange, list_b, start_date, end_date, write=False) \
                    if list_b else pd.DataFrame()
                candidate = pd.concat([f for f in (window_rows, rebuilt) if not f.empty], ignore_index=True) \
                    if not (window_rows.empty and rebuilt.empty) else pd.DataFrame()

        if mode in (recompute_mode.FULL, recompute_mode.SHADOW) or whole_exchange:
            why = "" if mode != recompute_mode.INCREMENTAL else \
                f" (INCREMENTAL fell back: {'whole exchange queued' if latest is not None else 'table empty'})"
            print(f"  [{label}/{exchange}] FULL recompute{why} -- {len(all_periods)} period(s) "
                  f"from {fmt_date(all_periods[0])} through {fmt_date(end_date)} (current period included).")
            ok = _roll_up_periods(conn, config, exchange, all_periods, end_date)
        else:
            print(f"  [{label}/{exchange}] INCREMENTAL -- {len(window)} period(s) from {fmt_date(window[0]) if window else '-'} "
                  f"through {fmt_date(end_date)}, then {len(list_b):,} list-B stock(s) in full.")
            ok = _roll_up_periods(conn, config, exchange, window, end_date)
            if ok and list_b:
                try:
                    _rebuild_securities(conn, config, exchange, list_b, start_date, end_date, write=True)
                except RollupPersistenceError as e:
                    print(f"    [{label}/{exchange}] FAILED rebuilding list-B: {e}")
                    ok = False

        if not ok:
            return False

        if mode == recompute_mode.SHADOW and not whole_exchange:
            n_a = candidate["security_id"].nunique() - len(list_b) if not candidate.empty else 0
            _shadow_compare(conn, config, exchange, candidate, end_date, max(n_a, 0), len(list_b))

        if queued:
            try:
                # Hand the rebuilt stocks on to this timeframe's RSI before forgetting them here.
                downstream = recompute_queue.DOWNSTREAM_OF_ROLLUP.get(target)
                if downstream:
                    recompute_queue.enqueue(conn, exchange, [downstream], queued,
                                            detail=f"{label} rollup {fmt_date(end_date)}")
                recompute_queue.clear(conn, exchange, target, queued.keys())
            except recompute_queue.RecomputeQueueError as e:
                print(f"  [WARN] {label}/{exchange}: rollup done, but recompute_queue not cleared ({e}) "
                      f"-- those stocks will simply be rebuilt again next run.")
        return True
    finally:
        conn.close()


def run(env_values, config, exchange, start_date, end_date, mode_of_recompute=recompute_mode.FULL):
    """
    Runs `config`'s rollup (WEEKLY or MONTHLY) for ONE exchange in the given mode_of_recompute -- see this
    module's header. Returns True on success.
    """
    mode = recompute_mode.parse_mode(mode_of_recompute)
    label = config["label"]
    print(f"  -- {label.capitalize()} rollup [{exchange}] (mode_of_recompute={mode}, "
          f"{fmt_date(start_date)} to {fmt_date(end_date)}) --")
    return _run_for_one_exchange(env_values, config, exchange, start_date, end_date, mode)
