# core/rsi/rsi_incremental.py
#
# Incremental single-date RSI14 update -- Story 2 of the Indicators
# Framework (docs/epics/indicators-framework/indicators-framework-epic-v1.html).
# Given rsi14d_workbook's CURRENT state, computes and upserts exactly ONE
# new trade_date for every isin, rather than the full-history recompute
# rsi14d_loader.py does.
#
# SCOPE -- this module does NOT do multi-day catch-up or decide "which
# date is next". Story 3/4's indicators_listener.py already walks
# forward one real trading day at a time (per bhav_copy_metadata's own
# SUCCESS history) and calls run_incremental_update() exactly once per
# date, in order -- "which date, and is it the right next one" is
# entirely the caller's problem, not this module's.
#
# CONTINUITY FIX (see core/rsi/rsi_continuity.py) -- this module DOES
# now do per-isin GAP detection, which is different from the listener's
# date sequencing above: the listener guarantees it calls this function
# once for every real trading date in order, but a single isin can still
# be absent from a given date's eligible rows (suspension, or simply no
# EQUITY-eligible row that day) while the market as a whole trades
# normally. When such an isin's next eligible row finally appears, this
# module compares its last known eligible trade_date against the shared
# trading calendar and reseeds (restarts the Wilder average from the
# last 14 eligible closes) instead of stepping across the gap as if it
# were one ordinary day -- the same mechanism that was silently
# corrupting MEIL's RSI. An ordinary weekend/holiday is never a gap:
# the comparison is in real trading SESSIONS elapsed, not calendar days.
#
# ERROR-HANDLING PHILOSOPHY -- two different severities, deliberately:
#   - A single isin having no prior rsi14d_workbook row, missing
#     close/prev_close for this date, an unresolvable gap-reseed (fewer
#     than RSI_PERIOD eligible rows available even after a gap), or
#     (should never happen) accumulating more than RSI_PERIOD-1 pre-seed
#     rows: all SOFT skips. Logged, counted, the rest of the date's
#     isins still get processed and written normally. One flaky isin
#     should not block RSI for the entire market.
#   - No bhav_copy data at all for the requested trade_date, or any
#     actual DB error: a REAL failure -- raises RsiPersistenceError.
#     This is what Story 4's catch-up thread is meant to catch and turn
#     into the DEACTIVE + indicators_open_failures path.

import pandas as pd

from core.rsi.rsi_math import RSI_PERIOD, compute_gain_loss, seed, step, compute_rsi14
from core.date_format import fmt_date
from core.rsi.rsi_persistence import upsert_rsi14d_workbook, RsiPersistenceError
from core.rsi.rsi_continuity import (
    fetch_trading_calendar, build_calendar_index, is_gap,
    fetch_recent_eligible_closes, RsiContinuityError,
)

# REPOINTED 2026-08-24 (security_id migration, see
# claude/bhav-copy-adjusted-clean-price-series-design-2026-08-23.md in
# the project docs) -- keyed by security_id now, not isin. A stock
# whose isin changed mid-history (e.g. MWL's SME-to-mainboard
# migration) keeps stepping its Wilder average forward across that
# change instead of losing its prior-row lookup the day the isin flips.
LATEST_PER_SECURITY_SQL = """
    SELECT DISTINCT ON (security_id, exchange) security_id, isin, exchange, series, symbol, trade_date AS latest_trade_date, avg_gain, avg_loss
      FROM rsi14d_workbook
     ORDER BY security_id, exchange, trade_date DESC
"""

PRIOR_HISTORY_SQL = """
    SELECT security_id, exchange, gain, loss
      FROM rsi14d_workbook
     WHERE (security_id, exchange) IN %(security_exchange_pairs)s
     ORDER BY security_id, exchange, trade_date ASC
"""

# REPOINTED 2026-08-24 -- reads bhav_copy_adjusted directly. No
# adjustment-factor join or eligibility/tiebreak filtering needed here
# anymore: loaders/bhav_copy_adjustment_loader.py already baked all of
# that into bhav_copy_adjusted (one clean row per security_id+exchange
# per trade_date) when it built/refreshed that table.
CLOSES_FOR_DATE_SQL = """
    SELECT security_id, source_isin AS isin, exchange, series, symbol, close, prev_close
      FROM bhav_copy_adjusted
     WHERE trade_date = %(trade_date)s
"""


def _fetch_closes_for_date(conn, trade_date):
    try:
        df = pd.read_sql(CLOSES_FOR_DATE_SQL, conn, params={"trade_date": trade_date})
    except Exception as e:
        raise RsiPersistenceError(f"Failed to fetch bhav_copy_adjusted closes for {fmt_date(trade_date)}: {e}")

    if df.empty:
        raise RsiPersistenceError(
            f"No bhav_copy_adjusted data at all for trade_date={fmt_date(trade_date)} -- has "
            f"bhav_copy_adjustment_loader.py been run for this date yet? Cannot compute an "
            f"incremental update for a date with zero data."
        )
    return df


def _fetch_latest_per_security(conn):
    try:
        df = pd.read_sql(LATEST_PER_SECURITY_SQL, conn)
    except Exception as e:
        raise RsiPersistenceError(f"Failed to fetch latest rsi14d_workbook row per security: {e}")
    return df.set_index(["security_id", "exchange"])


def _fetch_prior_history(conn, security_exchange_pairs):
    """
    security_exchange_pairs: list of (security_id, exchange) tuples
    still pre-seed. Returns dict (security_id, exchange) -> (gains,
    losses), oldest-to-newest.

    KNOWN SIMPLIFICATION: this accumulates every prior row regardless of
    whether a gap occurred mid-accumulation (a security that hasn't
    reached its first full seed yet is a much rarer case than a gap on
    an already-seeded security, the case the reseed path below
    handles). Not gap-aware during initial accumulation.
    """
    if not security_exchange_pairs:
        return {}
    try:
        df = pd.read_sql(
            PRIOR_HISTORY_SQL, conn,
            params={"security_exchange_pairs": tuple(security_exchange_pairs)},
        )
    except Exception as e:
        raise RsiPersistenceError(f"Failed to fetch prior pre-seed history: {e}")

    history = {pair: ([], []) for pair in security_exchange_pairs}
    for (security_id, exchange), group in df.groupby(["security_id", "exchange"], sort=False):
        history[(security_id, exchange)] = (list(group["gain"]), list(group["loss"]))
    return history


def _reseed_from_gap(conn, security_id, exchange, trade_date):
    """
    Restarts the Wilder average from scratch using the last RSI_PERIOD
    closes up to and including trade_date, instead of stepping across a
    detected gap. Returns (gain, loss, avg_gain, avg_loss, rsi14) for
    trade_date, or None if fewer than RSI_PERIOD rows are available yet
    (falls back to pre-seed treatment by the caller).
    """
    window_df = fetch_recent_eligible_closes(conn, security_id, exchange, trade_date, RSI_PERIOD)
    if len(window_df) < RSI_PERIOD:
        return None

    gains, losses = [], []
    for row in window_df.itertuples(index=False):
        g, l = compute_gain_loss(row.close, row.prev_close)
        if g is None:
            return None  # a null prev_close inside the window -- can't seed cleanly, stay pre-seed
        gains.append(g)
        losses.append(l)

    avg_gain, avg_loss = seed(gains, losses)
    rsi14 = compute_rsi14(avg_gain, avg_loss)
    return gains[-1], losses[-1], avg_gain, avg_loss, rsi14


def run_incremental_update(conn, trade_date):
    closes_df = _fetch_closes_for_date(conn, trade_date)
    latest_df = _fetch_latest_per_security(conn)

    try:
        calendar_dates = fetch_trading_calendar(conn, end_date=trade_date)
        calendar_index = build_calendar_index(calendar_dates)
    except RsiContinuityError as e:
        raise RsiPersistenceError(f"Failed to build trading calendar for gap detection: {e}")

    # FIX 2026-08-30 -- a key can be "still pre-seed" two different
    # ways: it already has a NULL-avg_gain row in rsi14d_workbook (the
    # original case this list covered), OR it has NEVER had ANY row in
    # rsi14d_workbook at all. The second case used to be treated as
    # "skip forever" (see the removed `skipped_no_workbook` branch
    # below) -- which was fine when this module only ran on top of an
    # already-seeded workbook from rsi14d_loader.py's one-time bulk
    # backfill (see that file's own header, now in _to_delete/). Now
    # that the scheduler redesign retired that bulk backfill and calls
    # run_incremental_update() directly from an EMPTY rsi14d_workbook
    # starting 2024-01-01, "never had a row yet" is the ordinary,
    # expected state for every single security on this run -- not an
    # error condition. Confirmed against a live run: with the old
    # logic, every date came back `written: 0` and rsi14d_workbook
    # never gained a single row, no matter how many trading dates were
    # walked -- a brand-new security had no way to ever get its first
    # row written.
    still_pre_seed_pairs = [
        (row.security_id, row.exchange) for row in closes_df.itertuples(index=False)
        if (row.security_id, row.exchange) not in latest_df.index
           or pd.isna(latest_df.loc[(row.security_id, row.exchange), "avg_gain"])
    ]
    prior_history = _fetch_prior_history(conn, still_pre_seed_pairs)

    counts = {
        "updated": 0, "seeded": 0, "still_pre_seed": 0, "reseeded_after_gap": 0,
        # Kept at a permanent 0 post-fix -- no code path increments it
        # anymore (a brand-new security now enters pre-seed accumulation
        # below instead of being skipped). Left in the dict so any
        # caller/log parser keyed on this exact set of fields doesn't
        # break.
        "skipped_no_workbook": 0, "skipped_missing_price": 0,
        "skipped_invariant_violation": 0, "skipped_reseed_insufficient_history": 0,
    }
    result_rows = []

    for row in closes_df.itertuples(index=False):
        security_id, isin, exchange, series, symbol, close, prev_close = (
            row.security_id, row.isin, row.exchange, row.series, row.symbol, row.close, row.prev_close
        )
        key = (security_id, exchange)

        if close is None or pd.isna(close) or prev_close is None or pd.isna(prev_close):
            counts["skipped_missing_price"] += 1
            continue

        if key in latest_df.index:
            latest_avg_gain = latest_df.loc[key, "avg_gain"]
            latest_avg_loss = latest_df.loc[key, "avg_loss"]
            latest_trade_date = latest_df.loc[key, "latest_trade_date"]
        else:
            # Brand-new security -- no rsi14d_workbook row at all yet,
            # not even a pre-seed one. Treated exactly like an existing
            # pre-seed row with zero prior history (falls into the
            # `else` branch just below), NOT skipped. See the fix note
            # above still_pre_seed_pairs.
            latest_avg_gain = None
            latest_avg_loss = None
            latest_trade_date = None

        if latest_avg_gain is not None and not pd.isna(latest_avg_gain):
            if is_gap(calendar_index, latest_trade_date, trade_date):
                reseeded = _reseed_from_gap(conn, security_id, exchange, trade_date)
                if reseeded is None:
                    counts["skipped_reseed_insufficient_history"] += 1
                    continue
                gain, loss, avg_gain, avg_loss, rsi14 = reseeded
                counts["reseeded_after_gap"] += 1
            else:
                gain, loss = compute_gain_loss(close, prev_close)
                avg_gain, avg_loss = step(latest_avg_gain, latest_avg_loss, gain, loss)
                rsi14 = compute_rsi14(avg_gain, avg_loss)
                counts["updated"] += 1
        else:
            prior_gains, prior_losses = prior_history.get(key, ([], []))
            prior_count = len(prior_gains)

            if prior_count > RSI_PERIOD - 1:
                counts["skipped_invariant_violation"] += 1
                continue

            gain, loss = compute_gain_loss(close, prev_close)

            if prior_count == RSI_PERIOD - 1:
                avg_gain, avg_loss = seed(prior_gains + [gain], prior_losses + [loss])
                rsi14 = compute_rsi14(avg_gain, avg_loss)
                counts["seeded"] += 1
            else:
                avg_gain, avg_loss, rsi14 = None, None, None
                counts["still_pre_seed"] += 1

        result_rows.append({
            "security_id": security_id, "isin": isin, "exchange": exchange, "series": series, "symbol": symbol,
            "trade_date": trade_date,
            "gain": gain, "loss": loss,
            "avg_gain": avg_gain, "avg_loss": avg_loss, "rsi14": rsi14,
        })

    result_df = pd.DataFrame(result_rows, columns=[
        "security_id", "isin", "exchange", "series", "symbol", "trade_date", "gain", "loss", "avg_gain", "avg_loss", "rsi14"
    ])
    written = upsert_rsi14d_workbook(conn, result_df) if not result_df.empty else 0

    counts["written"] = written
    return counts


def get_current_max_date(conn):
    """
    Returns rsi14d_workbook's current MAX(trade_date) across all isins,
    or None if the table has no rows at all (Part 1's bulk backfill has
    never been run -- the incremental path has nothing to build on top
    of yet).

    Used ONLY for auto-bootstrapping indicators_workbook_metadata's
    cursor the first time rsi14d runs under the Indicators Framework --
    its IWM row starts out with latest_trade_date NULL (see
    core/indicators/dispatch.py's bootstrap contract and
    indicators_listener.py).
    """
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT MAX(trade_date) FROM rsi14d_workbook")
            row = cur.fetchone()
    except Exception as e:
        raise RsiPersistenceError(f"Failed to fetch rsi14d_workbook's current max date: {e}")
    return row[0] if row else None

