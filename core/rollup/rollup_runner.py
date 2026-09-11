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

import time
from datetime import datetime as _dt, timedelta

from core.date_format import fmt_date

from core.db_client import get_connection, DbConnectionError
from core.rollup.period import period_start, next_period_start
from core.rollup.rollup_calculator import compute_rollup
from core.rollup.rollup_persistence import (
    fetch_daily_rows, fetch_prior_close_lookup, upsert_rollup_rows,
    upsert_metadata, record_run_audit, RollupPersistenceError,
)


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


def _roll_up_one_period(conn, config, exchange, period_trade_date, end_date):
    period_type = config["period_type"]
    rollup_table = config["rollup_table"]
    metadata_table = config["metadata_table"]
    run_audit_table = config["run_audit_table"]
    label = config["label"]

    natural_next_period_start = next_period_start(period_trade_date, period_type)
    natural_last_calendar_day = natural_next_period_start - timedelta(days=1)
    # Cap at end_date -- see this module's own header, "INCLUDES THE
    # CURRENTLY-OPEN PERIOD." A no-op for every already-fully-closed
    # period; only bites for the final, still-open one.
    last_calendar_day = min(natural_last_calendar_day, end_date)

    run_started_at = time.time()
    try:
        df = fetch_daily_rows(conn, exchange, from_date=period_trade_date, to_date=last_calendar_day)
    except RollupPersistenceError as e:
        print(f"    [{label}/{exchange}] FAILED fetching daily rows for {fmt_date(period_trade_date)}: {e}")
        return False

    if df.empty:
        print(f"    [{label}/{exchange}] {fmt_date(period_trade_date)}: no eligible daily rows yet -- skipping.")
        return False

    security_ids = df["security_id"].unique().tolist()
    keys = [(security_id, exchange, period_trade_date) for security_id in security_ids]
    try:
        prior_close_lookup = fetch_prior_close_lookup(conn, rollup_table, keys)
    except RollupPersistenceError as e:
        print(f"    [{label}/{exchange}] FAILED fetching prior-close lookup for {fmt_date(period_trade_date)}: {e}")
        return False

    rollup_df = compute_rollup(df, period_type, prior_close_lookup=prior_close_lookup)

    try:
        written_count = upsert_rollup_rows(conn, rollup_table, rollup_df)
    except RollupPersistenceError as e:
        print(f"    [{label}/{exchange}] FAILED upserting {fmt_date(period_trade_date)}: {e}")
        return False

    processing_time_ms = int((time.time() - run_started_at) * 1000)
    try:
        upsert_metadata(conn, metadata_table, period_trade_date, exchange, written_count, last_calendar_day,
                         processing_time_ms=processing_time_ms, status="SUCCESS")
        record_run_audit(conn, run_audit_table, exchange, period_trade_date,
                          _dt.fromtimestamp(run_started_at), _dt.now(), written_count, len(df), status="SUCCESS")
    except RollupPersistenceError as e:
        print(f"    [{label}/{exchange}] WARNING: rollup written, but metadata/run_audit record failed: {e}")
        return False

    print(f"    [{label}/{exchange}] {fmt_date(period_trade_date)}: rolled up {written_count} rows "
          f"from {len(df)} daily rows ({len(security_ids)} securities), through {fmt_date(last_calendar_day)}.")
    return True


def _run_for_one_exchange(env_values, config, exchange, start_date, end_date):
    label = config["label"]
    period_type = config["period_type"]
    try:
        conn = get_connection(env_values)
    except DbConnectionError as e:
        print(f"  [{label}/{exchange}] FAILED to open connection: {e}")
        return

    try:
        periods = _periods_between(start_date, end_date, period_type)
        if not periods:
            print(f"  [{label}/{exchange}] Nothing to compute for [{fmt_date(start_date)}, {fmt_date(end_date)}].")
            return

        print(f"  [{label}/{exchange}] FULL recompute -- {len(periods)} period(s) "
              f"from {fmt_date(periods[0])} through {fmt_date(end_date)} (current period included).")
        for period_trade_date in periods:
            if not _roll_up_one_period(conn, config, exchange, period_trade_date, end_date):
                break
    finally:
        conn.close()


def run(env_values, config, exchange, start_date, end_date):
    """
    Runs `config`'s rollup (WEEKLY or MONTHLY, whichever config
    describes -- see either runner module's own CONFIG dict) for ONE
    exchange, blindly recomputing every period in [start_date,
    end_date] -- see this module's own header for the redesign.
    """
    label = config["label"]
    print(f"  -- {label.capitalize()} rollup [{exchange}] ({fmt_date(start_date)} to {fmt_date(end_date)}) --")
    _run_for_one_exchange(env_values, config, exchange, start_date, end_date)
