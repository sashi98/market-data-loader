# bhavcopy_scheduler_main.py
#
# Central scheduler. Runs ONCE per cycle, where a cycle spans AMH/BMH
# together (16:00 IST through 09:00 IST the next calendar day),
# orchestrating the full 10-step Execution Flow:
#
#   1. Scheduler Runner        -- this file's own once-per-cycle loop +
#                                  maintenance_status ON.
#   2. Daily Bhav Copy Runner     -- runners/price_actions/bhav_copy_d_unadjusted_price_runner.py
#   3. Corporate Action Runner    -- runners/price_actions/corporate_actions_runner.py
#   4. Price Adjustment Runner    -- runners/price_actions/bhav_copy_d_price_adjustment_runner.py
#   5. Weekly Bhav Copy Runner    -- runners/price_actions/bhav_copy_w_runner.py
#   6. Monthly Bhav Copy Runner   -- runners/price_actions/bhav_copy_m_runner.py
#   7. Indicators Registry Update -- this file's own _activate_indicators().
#   8. Indicator Runner Loop      -- this file's own _run_indicator(),
#                                     called once per activated indicator
#                                     in a plain sequential loop (no
#                                     threads), via the per-indicator
#                                     runner modules under runners/technicals/.
#   9. Maintenance Status Reset   -- this file's own check_and_process()
#                                     finally block.
#  10. Summary Printer            -- this file's own print_summary().
#
# BLIND-UPSERT REDESIGN 2026-09-06 -- Sashikant's own direction: pull the
# "should this runner even run" decision entirely OUT of each runner and
# into this file, so every runner module becomes a pure "fetch this
# range for this exchange, compute, upsert" executor with NO freshness
# check and NO internal per-exchange loop of its own (STEPS 2-6; STEP 7/8
# stay the exceptions described below, since indicators are not
# exchange-scoped in the schema). This replaces the 2026-08-30 design's
# per-runner is_fresh_through()/get_metadata_freshness()/_is_source_fresh()
# calls, each of which independently decided skip-vs-run against its OWN
# table. That per-step design happened to be robust to a mid-chain
# failure (a runner whose own target table never advanced would just keep
# retrying every cycle, regardless of what upstream did) but spread the
# actual "run or not" decision across half a dozen modules. The new
# design centralizes that decision here, and recovers the same
# retry-safety for STEP 4/5/6/7 the OLD design had for free, by checking
# each step's OWN target's freshness explicitly (see _own_target_stale()
# and _activate_indicators()) rather than relying purely on the shared
# per-exchange bhav-copy gap signal.
#
# NEW VARIABLES, this cycle's own state (computed once per cycle in
# check_and_process(), threaded explicitly into every runner call rather
# than each runner recomputing its own idea of "now"):
#   - DEFAULT_START_DATE  -- 2024-01-01, constant. The one backfill
#     anchor every full-recompute step (4/5/6/7/8) is blindly told to
#     start from -- no runner discovers this itself anymore.
#   - ceiling_date        -- unchanged concept from the 2026-08-30 design:
#     the latest CALENDAR date whose bhav copy could plausibly already be
#     published (today if AMH, yesterday if BMH). A raw calendar date --
#     may land on a weekend or a holiday.
#   - LATEST_TRADE_DATE   -- NEW. ceiling_date walked backward (via
#     core/trading_calendar.py's already-existing, already-holiday-aware
#     compute_trading_date_range(), one trading day) until it lands on an
#     actual trading session -- a weekday that is not an exchange
#     holiday. This is the single date every step's own recompute range
#     ends at, and every freshness comparison is measured against. Fixes
#     a real bug in the 2026-08-30 design: that design's per-*d-indicator
#     freshness check compared bhav_copy_adjusted_metadata against the
#     raw ceiling_date directly, which is never satisfiable on a cycle
#     that happens to run on a weekend (no bhavcopy will ever exist for
#     a Saturday/Sunday) -- daily indicators sat needlessly skipped every
#     time the scheduler happened to run on a non-trading day, even with
#     nothing genuinely missing.
#   - freshness[exchange] -- MAX(trade_date) from bhav_copy_metadata,
#     SUCCESS rows only, PER EXCHANGE (not combined). Combining both
#     exchanges into one MAX(), as the prior design's
#     get_latest_success_date_overall() did, is itself a latent bug: if
#     one exchange ever lagged the other, the combined MAX would still
#     advance from the exchange that's current, silently leaving the
#     lagging exchange's own gap unrevisited forever. Per-exchange
#     freshness closes that.
#   - gap[exchange]       -- freshness[exchange] is None, or is behind
#     LATEST_TRADE_DATE. True means this exchange has new data to catch
#     up on; drives STEP 2/3's skip-vs-run per exchange directly, and
#     STEP 4/5/6's skip-vs-run together with that step's own target
#     freshness (see _own_target_stale()).
#
# WINDOW CLOSE TIME REVERTED TO 09:00 IST -- the 2026-08-30 redesign had
# widened this to 15:30 specifically to widen how late a missed AMH could
# still be caught up same-cycle. Sashikant's own confirmed intent this
# pass: AMH (16:00-23:59) / BMH (00:00-09:00) are the two sessions, full
# stop -- whichever one the machine happens to be on for is the cycle
# that runs. This is a deliberate narrowing back to the original spec,
# not an oversight.
#
# STEP 2/3 KEEP AN INCREMENTAL WINDOW, STEP 4/5/6/7/8 DO NOT -- per
# exchange, STEP 2 (raw bhav copy) and STEP 3 (corporate actions) are
# still bounded to [freshness[exchange]+1 (or DEFAULT_START_DATE if
# freshness[exchange] is None), LATEST_TRADE_DATE] -- there is no reason
# to re-download years of raw bhav copy or re-request the full corporate
# actions history every single cycle. STEP 3 specifically no longer
# re-requests the FULL [DEFAULT_START_DATE, LATEST_TRADE_DATE] window on
# every run the way the 2026-08-30 design did (that was there to catch a
# corporate action amended after the fact, or a late cross-exchange
# match) -- Sashikant's own confirmed call: corporate actions involve no
# computation of their own, so an amendment landing outside this cycle's
# incremental window is an accepted trade-off, not a regression to work
# around here. STEP 4 (price adjustment), STEP 5/6 (weekly/monthly
# rollup), and STEP 7/8 (indicators) all remain ALWAYS a full blind
# recompute over [DEFAULT_START_DATE, LATEST_TRADE_DATE] whenever they
# run at all -- a later corporate action can retroactively change
# history, so anything less than a full recompute there risks leaving
# stale values behind. STEP 5/6 additionally now compute the CURRENT,
# still-open week/month too (bounded to LATEST_TRADE_DATE, not the
# period's own natural end) -- Sashikant's own confirmed call -- see
# core/rollup/rollup_runner.py's own header for the mechanics.
#
# STEP 7 IS NOT GATED ON gap[] AT ALL -- indicators_registry /
# indicators_workbook_metadata (IWM) have exactly one row per
# indicator_id, no exchange column (Sashikant's own confirmed call: no
# schema change here). An indicator activates purely on its OWN IWM row:
# latest_trade_date IS NULL (never completed a run) or behind
# LATEST_TRADE_DATE -- see _activate_indicators() below. This is
# deliberately independent of gap[] -- an indicator whose own workbook
# fell behind for any reason (a previous run's failure, most commonly)
# re-activates on its own terms, even on a cycle where neither exchange's
# raw bhav copy brought anything new.
#
# STEP 8 UNCHANGED IN SHAPE -- each indicator runner module already does
# a full blind recompute of BOTH exchanges together (loops EXCHANGES
# internally) with no freshness check of its own; the only change here is
# what date window gets passed in (DEFAULT_START_DATE/LATEST_TRADE_DATE,
# not the old propagated STEP 2 range).
#
# ORIGINAL HEADER, retained for history (2026-08-30 redesign -- the
# once-per-cycle mechanism, the state file, the module split, and the
# maintenance-mode bracketing are ALL unchanged by this pass):
#
# ONCE-PER-CYCLE 2026-08-30 (sixth pass) -- the scheduler used to just
# check "am I inside the window?" every CHECK_INTERVAL_SECONDS and run
# check_and_process() every single time it was, for as long as the
# window stayed open. Confirmed with Sashikant: the scheduler should run
# EXACTLY ONCE per cycle, whenever it first gets the chance -- if the
# machine was on for AMH, that's the run; if it was off for AMH and only
# turned on partway through BMH, THAT'S the run instead, catching up on
# however many days were missed (STEP 2's own date-range logic already
# bridges multi-day gaps automatically, unaffected by this change). A
# cycle is identified by the calendar date of its own 16:00 AMH start;
# "have I already run THIS cycle" is tracked in a small local state file
# (SCHEDULER_STATE_FILE_PATH) -- not the DB -- specifically so it
# survives the whole process restarting, without needing a Liquibase
# migration coordinated with the Java app for something this
# scheduler-internal. See _cycle_id_for()/_load_last_completed_cycle_id()/
# _save_last_completed_cycle_id() and run()'s own loop for the mechanics.
#
# MODULARIZED 2026-08-30 (fifth pass) -- STEPS 2-6 used to be one shared
# module (runners/price_actions/bhavcopy_listener.py's own
# check_and_process()) covering all five steps together. Split into five
# single-purpose runner modules, one per algo step (Sashikant's own
# "modularize, clean code/SOLID" direction) -- this file calls each one
# directly in sequence itself. bhavcopy_listener.py and
# bhav_copy_d_loader.py are both retired -- see _to_delete/.
#
# MAINTENANCE MODE: unchanged by either rewrite -- the WHOLE cycle is
# bracketed. Set True at the very start of check_and_process(), ALWAYS
# cleared back to False in a finally block regardless of how the cycle
# ends.
#
# Run manually for testing/development (single check-and-process cycle,
# ignores the active-window gate below):
#   python bhavcopy_scheduler_main.py --once
#
# Real, ongoing usage (no args):
#   python bhavcopy_scheduler_main.py
#
# IST is a FIXED UTC+5:30 offset with no DST -- exact for IST
# specifically, no zoneinfo/tzdata dependency needed.
#
# CHECK INTERVAL: kept at 15 minutes, but only used to RETRY a cycle that
# failed to even get started (auth/maintenance-mode failure) -- a cycle
# that ran successfully sleeps all the way until the NEXT cycle's own
# 16:00 open, not 15 minutes later.

import argparse
import json
import sys
import time
from datetime import date, datetime, timedelta, timezone
from datetime import time as dtime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from core.env_validator import load_and_validate_env, EnvValidationError
from core.logging_setup import start_run_logging
from core.db_client import get_connection, DbConnectionError
from core.indicators.persistence import (
    fetch_iwm_freshness, activate_indicator, deactivate_indicator, IndicatorsPersistenceError,
)
from core.stock_universe.persistence import set_scheduler_running, StockUniversePersistenceError
from core.rollup.rollup_persistence import get_metadata_freshness, RollupPersistenceError
from core.trading_calendar import compute_trading_date_range, TradingCalendarError
from core.date_format import fmt_date, fmt_datetime
from core import auth_client
from runners.price_actions import (
    bhav_copy_d_unadjusted_price_runner, corporate_actions_runner,
    bhav_copy_d_price_adjustment_runner, bhav_copy_w_runner, bhav_copy_m_runner,
)
from runners.technicals import rsi14d_runner, rsi14w_runner, rsi14m_runner, ma9_runner, ma50_runner, ma200_runner

import logging

logger = logging.getLogger("bhavcopy_scheduler_main")

IST = timezone(timedelta(hours=5, minutes=30))

# Active window: 16:00 IST (AMH start) through 09:00 IST the next
# calendar day (BMH end) -- reverted to the original 09:00 close (see
# this file's own header, "WINDOW CLOSE TIME REVERTED").
WINDOW_START_HOUR_IST = 16
WINDOW_END_HOUR_IST = 9
WINDOW_END_MINUTE_IST = 0

CHECK_INTERVAL_SECONDS = 900

# Local file tracking which cycle was last successfully run -- see this
# file's own header ("ONCE-PER-CYCLE") for why this is a local file, not
# a DB table. Stores {"last_completed_cycle_date": "YYYY-MM-DD"}.
SCHEDULER_STATE_FILE_PATH = Path(__file__).resolve().parent / ".scheduler_cycle_state.json"

EXCHANGES = ["NSE", "BSE"]

# The one backfill anchor every full-recompute step blindly starts from
# -- see this file's own header, "NEW VARIABLES".
DEFAULT_START_DATE = date(2024, 1, 1)

# STOCK UNIVERSE READINESS GATE (2026-09-11, FIXED 2026-09-13) --
# confirmed with Sashikant: before this scheduler ever enters STEP 1's
# AMH/BMH cycle logic, it must first confirm the stock universe has
# been fully enriched at least once in this environment. That means
# TWO things, both checked by _stock_universe_ready(): every required
# (data_source, segment) row in stock_universe_metadata is
# status='success' (written the moment the RAW CSV upload/parse
# succeeds), AND the separate stock_universe_update_listener.py
# process's own enrichment pass over that exact batch has actually
# FINISHED (its stock_universe_enrichment_run cursor has caught up) --
# the first condition alone was the original 2026-09-11 gate, and was
# found 2026-09-13 to fire far too early, since raw upload success and
# "enrichment listener is done" can be up to ~175 minutes apart (see
# _stock_universe_ready's own docstring for the full mechanics). No
# coupling exists between the two processes beyond reading the same
# tables the listener writes, so on a brand-new environment this
# scheduler could otherwise race ahead of stock universe enrichment and
# run STEP 2 onward against an empty/partial/still-enriching universe.
# Checked exactly ONCE at process startup (not re-checked every cycle)
# -- see _wait_for_stock_universe_ready().
REQUIRED_STOCK_UNIVERSE_SOURCES = [
    ("nse_data", "CASH"),
    ("nse_sme_data", "CASH"),
    ("bse_data", "CASH"),
    ("bse_sme_data", "CASH"),
    ("nse_fno_data", "FO"),
]

# indicator_id -> its own runner module's run(conn, indicator_id,
# start_date, end_date) function -- STEP 8's per-indicator dispatch.
RUNNERS = {
    "rsi14d": rsi14d_runner.run,
    "rsi14w": rsi14w_runner.run,
    "rsi14m": rsi14m_runner.run,
    "ma9d": ma9_runner.run, "ma9w": ma9_runner.run, "ma9m": ma9_runner.run,
    "ma50d": ma50_runner.run, "ma50w": ma50_runner.run, "ma50m": ma50_runner.run,
    "ma200d": ma200_runner.run, "ma200w": ma200_runner.run, "ma200m": ma200_runner.run,
}


def _cycle_id_for(now):
    """
    The date identifying which cycle `now` falls in -- the calendar date
    of that cycle's OWN 16:00 AMH start -- or None if `now` isn't inside
    any cycle window at all (the dead zone between this morning's 09:00
    BMH close and today's 16:00 AMH open).
    """
    t = now.time()
    if t >= dtime(WINDOW_START_HOUR_IST, 0):
        return now.date()
    if t < dtime(WINDOW_END_HOUR_IST, WINDOW_END_MINUTE_IST):
        return now.date() - timedelta(days=1)
    return None


def _seconds_until_next_window_open(now):
    """Seconds until the next upcoming 16:00 IST, strictly after `now`."""
    next_open = now.replace(hour=WINDOW_START_HOUR_IST, minute=0, second=0, microsecond=0)
    if next_open <= now:
        next_open += timedelta(days=1)
    return max((next_open - now).total_seconds(), 0)


def _load_last_completed_cycle_id():
    """
    Returns the date of the last cycle this process (or a prior run of
    it, possibly days ago) successfully completed, or None if the state
    file doesn't exist yet or is unreadable/corrupt.
    """
    try:
        with open(SCHEDULER_STATE_FILE_PATH, "r") as f:
            data = json.load(f)
        return datetime.strptime(data["last_completed_cycle_date"], "%Y-%m-%d").date()
    except (FileNotFoundError, json.JSONDecodeError, KeyError, ValueError):
        return None


def _save_last_completed_cycle_id(cycle_id):
    try:
        with open(SCHEDULER_STATE_FILE_PATH, "w") as f:
            json.dump({"last_completed_cycle_date": cycle_id.strftime("%Y-%m-%d")}, f)
    except OSError as e:
        logger.warning(f"  [WARN] Could not persist cycle-state file ({SCHEDULER_STATE_FILE_PATH}): {e} -- "
              f"if this process restarts before the next cycle, it may re-run this one.")


def _ceiling_date(now):
    """
    The most recent calendar date whose bhav copy could plausibly
    already be published. At/after 16:00 IST (AMH) that's today; before
    16:00 IST (the whole BMH morning span) it's still yesterday. A raw
    calendar date -- may land on a weekend or holiday; see
    _latest_trade_date() for the trading-day-aware value everything else
    in this file actually uses.
    """
    if now.time() >= dtime(WINDOW_START_HOUR_IST, 0):
        return now.date()
    return now.date() - timedelta(days=1)


def _latest_trade_date(ceiling_date, tmt_app_base_url, token):
    """
    ceiling_date walked backward to the nearest actual trading
    session -- a weekday that is not an exchange holiday. Reuses
    core/trading_calendar.py's compute_trading_date_range(), already
    used elsewhere in this codebase (STEP 2's own per-date walk) and
    already holiday-aware via core/holiday_client.py's TMT
    /api/holidays/sync/{year} call -- no new holiday-lookup
    infrastructure needed. Asking for exactly 1 trading date ending at
    ceiling_date walks backward past however many weekend/holiday days
    sit between ceiling_date and the last real session, then stops.

    Raises TradingCalendarError if the holiday calendar can't be fetched
    -- caller decides how to handle that (see check_and_process()).
    """
    result = compute_trading_date_range(ceiling_date, 1, tmt_app_base_url, token)
    return result["trading_date_list"][0]


def _bhav_copy_freshness_per_exchange(conn):
    """
    MAX(trade_date) from bhav_copy_metadata, SUCCESS rows only, PER
    EXCHANGE -- deliberately not combined into one MAX() the way the
    2026-08-30 design's get_latest_success_date_overall() did (see this
    file's own header for why that was itself a latent bug). None for an
    exchange with no SUCCESS row at all yet.
    """
    freshness = {}
    with conn.cursor() as cur:
        for exchange in EXCHANGES:
            cur.execute(
                "SELECT MAX(trade_date) FROM bhav_copy_metadata WHERE exchange = %s AND upload_status = 'SUCCESS'",
                (exchange,),
            )
            row = cur.fetchone()
            freshness[exchange] = row[0] if row and row[0] is not None else None
    return freshness


def _gap_detected(freshness_date, latest_trade_date):
    """True if this exchange's raw bhav copy has never succeeded, or is behind latest_trade_date."""
    return freshness_date is None or freshness_date < latest_trade_date


def _incremental_start(freshness_date):
    """DEFAULT_START_DATE if this exchange has never succeeded, else the day after its last success."""
    return DEFAULT_START_DATE if freshness_date is None else freshness_date + timedelta(days=1)


def _bhav_copy_adjusted_freshness(conn, exchange):
    """
    latest_trade_date from bhav_copy_adjusted_metadata for ONE exchange's
    SUCCESS row (one row per exchange on this table, not an audit log --
    same convention core/price_series/adjusted_series.py's own
    is_fresh_through() already relies on). None if that exchange has
    never succeeded.
    """
    with conn.cursor() as cur:
        cur.execute(
            "SELECT latest_trade_date FROM bhav_copy_adjusted_metadata WHERE exchange = %s AND run_status = 'SUCCESS'",
            (exchange,),
        )
        row = cur.fetchone()
        return row[0] if row and row[0] is not None else None


def _own_target_stale(freshness_date, latest_trade_date):
    """True if a step's own target table has never succeeded, or is behind latest_trade_date."""
    return freshness_date is None or freshness_date < latest_trade_date


def _run_rollup_step(env_values, runner_module, gap, latest_trade_date):
    """
    STEP 5 / STEP 6 -- per exchange, gated on gap[exchange] OR this
    step's OWN rollup metadata table being itself behind
    latest_trade_date (retry-safety for a previous run's own failure,
    same shape as STEP 4 below). Always a full blind recompute over
    [DEFAULT_START_DATE, latest_trade_date] when it runs at all,
    including the current still-open week/month -- see
    core/rollup/rollup_runner.py's own header.
    """
    config = runner_module.CONFIG
    label = config["label"]
    metadata_table = config["metadata_table"]
    for exchange in EXCHANGES:
        try:
            conn = get_connection(env_values)
            try:
                own_freshness = get_metadata_freshness(conn, metadata_table, exchange)
            finally:
                conn.close()
        except (DbConnectionError, RollupPersistenceError) as e:
            logger.info(f"  [{label}/{exchange}] Could not check {metadata_table} freshness ({e}) -- running anyway.")
            own_freshness = None

        if not gap[exchange] and not _own_target_stale(own_freshness, latest_trade_date):
            logger.info(f"  [{label}/{exchange}] SKIPPED -- no gap and already current through {own_freshness}.")
            continue

        runner_module.run(env_values, exchange, DEFAULT_START_DATE, latest_trade_date)


def _activate_indicators(env_values, latest_trade_date):
    """
    STEP 7 -- Indicators Registry Update. REDESIGNED 2026-09-06:
    activation is driven PURELY by indicators_workbook_metadata's (IWM)
    own freshness -- NOT by gap[] or any source table's freshness at all
    (see this file's own header, "STEP 7 IS NOT GATED ON gap[] AT ALL").
    indicators_registry/IWM have one row per indicator_id, no exchange
    column, by Sashikant's own confirmed call -- there is no natural
    per-exchange scoping to gate on here in the first place.

    An indicator activates if its own IWM row has latest_trade_date IS
    NULL (registered but has never completed a run) or behind
    latest_trade_date. This makes indicator recompute self-healing on
    its own terms: a run that failed partway one cycle never advances
    its own IWM row, so it re-activates the very next cycle regardless
    of whether either exchange's raw bhav copy happened to bring
    anything new that day.

    Returns the list of indicator_ids activated this cycle.
    """
    try:
        conn = get_connection(env_values)
    except DbConnectionError as e:
        logger.error(f"  [FAILED] Could not connect to DB to activate indicators: {e}")
        return []

    activated = []
    try:
        try:
            iwm_latest_by_indicator = fetch_iwm_freshness(conn)
        except IndicatorsPersistenceError as e:
            logger.error(f"  [FAILED] Could not fetch indicators_workbook_metadata: {e}")
            return []

        for indicator_id, iwm_latest in iwm_latest_by_indicator.items():
            if not _own_target_stale(iwm_latest, latest_trade_date):
                continue

            try:
                activate_indicator(conn, indicator_id)
            except IndicatorsPersistenceError as e:
                logger.error(f"  [{indicator_id}] Failed to activate: {e}")
                continue

            activated.append(indicator_id)
            reason = "never completed a run" if iwm_latest is None else f"behind ({fmt_date(iwm_latest)})"
            logger.info(f"  [{indicator_id}] ACTIVATED -- {reason}, recomputing through {fmt_date(latest_trade_date)}.")
    finally:
        conn.close()

    return activated


def _run_indicator(env_values, indicator_id, start_date, end_date):
    """
    STEP 8 -- called once per activated indicator, in order, from a
    plain sequential loop (no threads -- an earlier threaded version had
    several MA/RSI runners activate together and each pull an unbounded
    multi-million-row fetch into memory at once, which crashed Postgres;
    once every runner's own fetch became properly bounded/chunked, a
    thread pool that only ever ran one worker at a time added nothing
    but complexity). Opens its own connection per call so one
    indicator's dead/rolled-back connection never leaks into the next
    indicator's call.

    Every runner dispatched from here already does a full blind
    recompute of BOTH exchanges (loops EXCHANGES internally) with no
    freshness check of its own -- see this file's own header, "STEP 8
    UNCHANGED IN SHAPE". start_date/end_date are now DEFAULT_START_DATE
    and LATEST_TRADE_DATE respectively, not the old propagated STEP 2
    range.

    On success, deactivates the indicator in indicators_registry. On
    failure, the runner itself has ALREADY called record_failure()
    (which also sets DEACTIVE) before returning False -- this function
    must not double up on that.
    """
    runner = RUNNERS.get(indicator_id)
    if runner is None:
        logger.info(f"  [{indicator_id}] No runner registered -- skipping this cycle.")
        return

    try:
        conn = get_connection(env_values)
    except DbConnectionError as e:
        logger.error(f"  [{indicator_id}] Failed to open a connection: {e}")
        return

    try:
        completed_cleanly = runner(conn, indicator_id, start_date, end_date)
        if completed_cleanly:
            deactivate_indicator(conn, indicator_id)
            logger.info(f"  [{indicator_id}] Complete -- deactivated in indicators_registry.")
    except IndicatorsPersistenceError as e:
        logger.error(f"  [{indicator_id}] Bookkeeping error: {e}")
    except Exception as e:
        # Catching broadly here keeps one indicator's unexpected failure
        # scoped to just this indicator_id, instead of taking the whole
        # sequential loop (and every indicator after it, this cycle and
        # every cycle after) down with it.
        logger.error(f"  [{indicator_id}] UNEXPECTED FAILURE (not handled by the runner's own error "
              f"handling) -- skipping this indicator for the rest of this cycle: {e}")
    finally:
        conn.close()


def print_summary(cycle_summary, activated_indicators, elapsed_seconds):
    """STEP 10 -- Summary Printer."""
    logger.info("\n" + "=" * 60)
    logger.info("  STEP 10 -- Summary")
    logger.info("=" * 60)
    logger.info(f"  Ceiling date this cycle:  {cycle_summary['ceiling_date']}")
    logger.info(f"  Latest trade date:        {cycle_summary['latest_trade_date']}")
    for exchange in EXCHANGES:
        logger.info(f"  [{exchange}] gap detected: {cycle_summary['gap'][exchange]}")
    logger.info(f"  Indicators activated:     {len(activated_indicators)} ({', '.join(activated_indicators) or 'none'})")
    logger.info(f"  Elapsed:                  {elapsed_seconds:.1f}s")
    logger.info("=" * 60)


def _set_maintenance(env_values, is_running):
    """
    Short-lived connection just for this one maintenance_status write.
    Returns True on success, False (already printed) on any failure.

    UPDATED 2026-09-13 -- writes scheduler_running specifically (via
    set_scheduler_running(), renamed from the old shared
    set_maintenance_running()), never the listener's own
    enrichment_running -- see that function's own docstring in
    core/stock_universe/persistence.py for why maintenance_status was
    split into independent per-process flags. This function's own name
    and behavior are otherwise unchanged: it still just marks THIS
    process's own cycle as running/not-running.
    """
    try:
        conn = get_connection(env_values)
    except DbConnectionError as e:
        logger.error(f"  [FAILED] Could not connect to DB to update maintenance status: {e}")
        return False
    try:
        set_scheduler_running(conn, is_running)
        return True
    except StockUniversePersistenceError as e:
        logger.error(f"  [FAILED] Could not update maintenance status: {e}")
        return False
    finally:
        conn.close()


def check_and_process(env_values):
    """
    One full scheduler cycle -- STEPS 2 through 10. Returns True if the
    cycle actually ran (got past auth + entering maintenance mode +
    resolving LATEST_TRADE_DATE, regardless of what any individual
    step's own outcome was), False if it never even got started (auth
    failure, couldn't set maintenance_status=True, or a holiday-calendar
    lookup failure prevented LATEST_TRADE_DATE from resolving at all) --
    see run()'s own loop for why that distinction matters: only a True
    return marks this cycle's own id as done in the persisted state
    file, so a failure this early gets retried at the next
    CHECK_INTERVAL_SECONDS poll instead of being silently skipped for
    the rest of the day.
    """
    cycle_start = time.time()
    now = datetime.now(IST)
    ceiling_date = _ceiling_date(now)

    logger.info(f"Scheduler cycle starting at {fmt_datetime(now)} (ceiling date: {fmt_date(ceiling_date)}).")

    # Authenticate BEFORE entering maintenance mode -- tmt's own
    # maintenance filter checks maintenance_status on EVERY request,
    # login included, so logging in after flipping the flag would lock
    # the scheduler out on its own first step.
    try:
        token = auth_client.login(
            env_values["TMT_APP_BASE_URL"], env_values["TMT_ADMIN_USER_ID"], env_values["TMT_ADMIN_PASSWORD"]
        )
    except auth_client.AuthError as e:
        logger.error(f"  [FAILED] Could not authenticate -- skipping this cycle: {e}")
        return False

    tmt_app_base_url = env_values["TMT_APP_BASE_URL"]
    try:
        latest_trade_date = _latest_trade_date(ceiling_date, tmt_app_base_url, token)
    except TradingCalendarError as e:
        logger.error(f"  [FAILED] Could not resolve the latest trade date (holiday calendar lookup failed): {e} -- "
              f"skipping this cycle.")
        return False

    if not _set_maintenance(env_values, True):
        logger.error("  [FAILED] Could not enter maintenance mode -- skipping this cycle rather than running unprotected.")
        return False

    # CRITICAL: this finally block is what guarantees maintenance mode
    # never stays stuck on.
    try:
        logger.info(f"  Latest trade date this cycle: {fmt_date(latest_trade_date)}.")

        try:
            conn = get_connection(env_values)
            try:
                freshness = _bhav_copy_freshness_per_exchange(conn)
            finally:
                conn.close()
        except DbConnectionError as e:
            logger.error(f"  [FAILED] Could not read bhav_copy_metadata freshness: {e} -- skipping this cycle's runners.")
            freshness = {exchange: None for exchange in EXCHANGES}

        gap = {exchange: _gap_detected(freshness[exchange], latest_trade_date) for exchange in EXCHANGES}
        for exchange in EXCHANGES:
            status = f"GAP -- will process through {fmt_date(latest_trade_date)}" if gap[exchange] else "up to date"
            logger.info(f"  [{exchange}] bhav_copy_metadata freshness: {fmt_date(freshness[exchange]) or 'none yet'} -- {status}.")

        download_dir = env_values["DATA_MARKET_DATA_LOADER_BHAV_COPY_DOWNLOAD_DIR"]

        # STEP 2 -- Daily Bhav Copy Runner, per exchange, blind upsert
        # over its own incremental [start, latest_trade_date] window.
        logger.info("\nSTEP 2 -- Daily Bhav Copy Runner")
        for exchange in EXCHANGES:
            if not gap[exchange]:
                logger.info(f"  [{exchange}] SKIPPED -- no gap detected.")
                continue
            incremental_start = _incremental_start(freshness[exchange])
            bhav_copy_d_unadjusted_price_runner.run(
                env_values, tmt_app_base_url, token, download_dir,
                exchange, incremental_start, latest_trade_date, ceiling_date,
            )

        # STEP 3 -- Corporate Action Runner, per exchange, same
        # incremental window -- no full-history recompute (Sashikant's
        # own confirmed call: no computation happens here, so an
        # amendment landing outside this window is an accepted
        # trade-off).
        logger.info("\nSTEP 3 -- Corporate Action Runner")
        for exchange in EXCHANGES:
            if not gap[exchange]:
                logger.info(f"  [{exchange}] SKIPPED -- no gap detected.")
                continue
            incremental_start = _incremental_start(freshness[exchange])
            corporate_actions_runner.run(env_values, exchange, incremental_start, latest_trade_date)

        # STEP 4 -- Price Adjustment Runner, per exchange. Gated on
        # gap[exchange] OR this exchange's OWN bhav_copy_adjusted_metadata
        # being itself behind latest_trade_date (retry-safety: a
        # previous run's own failure re-triggers here even when raw
        # bhavcopy is otherwise already current). Always a full blind
        # recompute of [DEFAULT_START_DATE, latest_trade_date] when it
        # runs at all.
        logger.info("\nSTEP 4 -- Price Adjustment Runner")
        for exchange in EXCHANGES:
            try:
                conn = get_connection(env_values)
                try:
                    own_freshness = _bhav_copy_adjusted_freshness(conn, exchange)
                finally:
                    conn.close()
            except DbConnectionError as e:
                logger.info(f"  [{exchange}] Could not check bhav_copy_adjusted_metadata freshness ({e}) -- running anyway.")
                own_freshness = None

            if not gap[exchange] and not _own_target_stale(own_freshness, latest_trade_date):
                logger.info(f"  [{exchange}] SKIPPED -- no gap and bhav_copy_adjusted already current through {own_freshness}.")
                continue

            try:
                bhav_copy_d_price_adjustment_runner.run(env_values, exchange, DEFAULT_START_DATE, latest_trade_date)
            except bhav_copy_d_price_adjustment_runner.PriceAdjustmentError as e:
                logger.error(f"  [FAILED] Price adjustment for {exchange} failed: {e}")

        # STEP 5 / STEP 6 -- Weekly / Monthly rollup, per exchange, same
        # gap-OR-own-target-stale gate as STEP 4. Always full blind
        # recompute of [DEFAULT_START_DATE, latest_trade_date], including
        # the current still-open week/month.
        logger.info("\nSTEP 5 -- Weekly Bhav Copy Runner")
        _run_rollup_step(env_values, bhav_copy_w_runner, gap, latest_trade_date)
        logger.info("\nSTEP 6 -- Monthly Bhav Copy Runner")
        _run_rollup_step(env_values, bhav_copy_m_runner, gap, latest_trade_date)

        # STEP 7 -- Indicators Registry Update -- pure IWM-freshness
        # gate, no reference to gap[] at all (see this file's own
        # header and _activate_indicators()'s own docstring).
        logger.info("\nSTEP 7 -- Indicators Registry Update")
        activated_indicators = _activate_indicators(env_values, latest_trade_date)

        # STEP 8 -- Indicator Runner Loop.
        if activated_indicators:
            logger.info("\nSTEP 8 -- Indicator Runner Loop")
            for indicator_id in activated_indicators:
                _run_indicator(env_values, indicator_id, DEFAULT_START_DATE, latest_trade_date)

        cycle_summary = {
            "ceiling_date": ceiling_date,
            "latest_trade_date": latest_trade_date,
            "gap": gap,
        }
        print_summary(cycle_summary, activated_indicators, time.time() - cycle_start)
    finally:
        if not _set_maintenance(env_values, False):
            logger.error("  [FAILED] Could not clear maintenance status -- MANUAL INTERVENTION NEEDED: "
                  "the app may be stuck showing the maintenance banner to every user until this is fixed.")
    return True


def _stock_universe_ready(env_values):
    """
    True only if BOTH:
      1. every required (data_source, segment) combination in
         REQUIRED_STOCK_UNIVERSE_SOURCES has at least one row with
         status = 'success' in stock_universe_metadata, AND
      2. the SEPARATE stock_universe_update_listener.py process has
         actually FINISHED enriching that exact batch -- not just
         started it, and not still be mid-batch.

    FIXED (2026-09-13) -- Sashikant's own observation: this check
    originally looked at stock_universe_metadata.status alone, which
    StockUniverseServiceHandler#saveOrUpdate() (Java side) sets to
    'success' the moment the RAW CSV is parsed and persisted -- BEFORE
    stock_universe_update_listener.py's own enrichment pass (yfinance/
    NSE/BSE-official lookups per ISIN, up to ~175 min for a full batch)
    has even started. So this gate could -- and did -- declare the
    stock universe "ready" while enrichment was still running or had
    not started at all, letting STEP 2 onward proceed against an
    unenriched/partially-enriched universe.

    Condition 2 mirrors exactly what stock_universe_update_listener.py's
    own poll_once() uses to decide "is there new work to enrich" (see
    that file's core/stock_universe/persistence.py --
    fetch_latest_metadata_status/get_max_metadata_id/
    fetch_last_processed_cursor): take the highest stock_universe_metadata
    id among the 5 required (data_source, segment) rows, and compare it
    against the listener's own persisted cursor -- the
    last_processed_metadata_id of its most recent stock_universe_enrichment_run
    row with status IN ('SUCCESS', 'PARTIAL'). The listener only writes
    that row once a REAL (non---limit) enrichment batch has fully
    completed (see run_enrichment_batch's own docstring there), so
    cursor >= max_metadata_id means enrichment for this exact upload is
    done, not just in progress.

    Deliberately duplicates these two small queries here rather than
    importing core.stock_universe.persistence from the listener's own
    package -- this file's own header already documents "no coupling
    exists between the two processes," and that stays true: this only
    reads the same two tables the listener already writes to, it never
    reaches into the listener's code.

    Returns False (never raises) on a DB error -- treated the same as
    "not ready yet" so a transient connection failure just gets retried
    on the next poll, same degrade-and-continue convention this file
    already uses elsewhere (see _bhav_copy_freshness_per_exchange's own
    caller in check_and_process()).
    """
    try:
        conn = get_connection(env_values)
        try:
            missing = []
            max_metadata_id = 0
            with conn.cursor() as cur:
                for data_source, segment in REQUIRED_STOCK_UNIVERSE_SOURCES:
                    cur.execute(
                        "SELECT id FROM stock_universe_metadata WHERE data_source = %s AND segment = %s "
                        "AND status = 'success' ORDER BY id DESC LIMIT 1",
                        (data_source, segment),
                    )
                    row = cur.fetchone()
                    if row is None:
                        missing.append(f"{data_source}/{segment}")
                    else:
                        max_metadata_id = max(max_metadata_id, row[0])

                if missing:
                    logger.info(f"  Stock universe not ready yet -- still waiting on: {', '.join(missing)}.")
                    return False

                # All 5 uploads are in -- now check whether enrichment has
                # actually caught up to this batch, same cursor comparison
                # stock_universe_update_listener.py's own poll_once() does.
                cur.execute(
                    "SELECT last_processed_metadata_id FROM stock_universe_enrichment_run "
                    "WHERE status IN ('SUCCESS', 'PARTIAL') ORDER BY id DESC LIMIT 1"
                )
                cursor_row = cur.fetchone()
                enrichment_cursor = cursor_row[0] if cursor_row else 0
        finally:
            conn.close()
    except DbConnectionError as e:
        logger.warning(f"  [WARN] Could not check stock_universe_metadata/enrichment readiness: {e} -- treating as not ready.")
        return False

    if enrichment_cursor < max_metadata_id:
        logger.info(f"  Stock universe uploads are all present (max metadata id {max_metadata_id}), but the "
                    f"enrichment listener hasn't finished processing this batch yet (cursor at "
                    f"{enrichment_cursor}) -- waiting for it to complete.")
        return False

    return True


def _wait_for_stock_universe_ready(env_values):
    """
    STEP 0 -- blocks, polling every CHECK_INTERVAL_SECONDS, until the
    stock universe has been fully enriched at least once in this
    environment (see REQUIRED_STOCK_UNIVERSE_SOURCES above for what
    "fully enriched" means). Called exactly ONCE from run(), right after
    env validation succeeds and before either the --once path or the
    main AMH/BMH loop -- so both paths are gated the same way, but a
    cycle already in progress is never interrupted by this check.
    Polls indefinitely, no timeout -- same pattern as run()'s own
    window-wait loop; there's no sensible timeout for "how long can
    initial stock universe enrichment take."
    """
    if _stock_universe_ready(env_values):
        logger.info("Stock universe readiness check passed -- all required sources present. Proceeding.")
        return

    logger.info(f"Stock universe not yet fully enriched -- waiting before starting the scheduler "
                f"(re-checking every {CHECK_INTERVAL_SECONDS // 60} min)...")
    while not _stock_universe_ready(env_values):
        time.sleep(CHECK_INTERVAL_SECONDS)
    logger.info("Stock universe readiness check passed -- all required sources present. Proceeding.")


def _log_cycle_idle_message(cycle_id, now, sleep_seconds):
    """
    The one line an operator glancing at the log actually needs once
    there's nothing left to do until the next window: which cycle is
    done, how long until anything runs again, the actual calendar dates
    of the next AMH/BMH windows (not just their times), and a
    plain-English reminder of the once-per-day design so a long idle
    gap in the log doesn't read as the scheduler being stuck. Shared by
    both the "just finished this cycle" and "already ran this cycle,
    nothing to do on this restart" paths in run()'s main loop -- from
    an operator's point of view both mean the exact same thing: the
    cycle is done, go idle until the next window. Window-close time
    here (09:00 IST) matches WINDOW_END_HOUR_IST above, not the earlier
    15:30 that was reverted -- see this file's own header, "WINDOW
    CLOSE TIME REVERTED TO 09:00 IST".

    `now` is the moment `sleep_seconds` (from
    _seconds_until_next_window_open()) was measured from -- adding them
    gives the exact next-AMH-open date; BMH's date is always the day
    after that (BMH is that same cycle's overnight continuation, 00:00
    IST the calendar day right after its own AMH open).
    """
    hours_until_next_run = round(sleep_seconds / 3600, 1)
    amh_date = (now + timedelta(seconds=sleep_seconds)).date()
    bmh_date = amh_date + timedelta(days=1)
    logger.info(
        f"  Cycle for {fmt_date(cycle_id)} completed — system will remain idle "
        f"until the next run window opens in ~{hours_until_next_run} hours. The "
        f"next scheduler run will occur during After Market Hours (AMH) on "
        f"{fmt_date(amh_date)} between 4:00 PM – 11:59 PM IST, or if missed, "
        f"during Before Market Hours (BMH) on {fmt_date(bmh_date)} between "
        f"12:00 AM – 9:00 AM IST. Each cycle executes only once per day, valid "
        f"from today's AMH (4:00 PM IST) until tomorrow's BMH (9:00 AM IST)."
    )


def run():
    """Standard entry point -- STEP 1, Scheduler Runner."""
    parser = argparse.ArgumentParser(description="Bhav copy central scheduler")
    parser.add_argument("--once", action="store_true",
                         help="Run a single check-and-process cycle immediately (ignoring the active-window "
                              "gate) and exit. Useful for a first test run.")
    args = parser.parse_args()

    with start_run_logging("bhavcopy_scheduler_main"):
        try:
            env_values = load_and_validate_env()
        except EnvValidationError as e:
            logger.error(f"[FAILED] {e}")
            sys.exit(1)

        _wait_for_stock_universe_ready(env_values)

        if args.once:
            logger.info("Bhavcopy scheduler -- running a single check-and-process cycle (--once), then exiting.")
            ran_ok = check_and_process(env_values)
            if ran_ok:
                cycle_id = _cycle_id_for(datetime.now(IST))
                if cycle_id is not None:
                    _save_last_completed_cycle_id(cycle_id)
            return

        logger.info(f"Bhavcopy scheduler starting -- ONE run per cycle (16:00-09:00 IST next day), "
              f"retrying every {CHECK_INTERVAL_SECONDS // 60} min only if a cycle fails to even start. "
              f"Ctrl+C to stop.")
        last_completed_cycle_id = _load_last_completed_cycle_id()
        if last_completed_cycle_id is not None:
            logger.info(f"  Last completed cycle on record: {fmt_date(last_completed_cycle_id)}.")
        try:
            while True:
                now = datetime.now(IST)
                cycle_id = _cycle_id_for(now)

                if cycle_id is None:
                    sleep_seconds = _seconds_until_next_window_open(now)
                    logger.info(f"  Between cycles (this morning's 09:00 close, today's 16:00 open) -- sleeping "
                          f"{round(sleep_seconds / 60, 1)} min.")
                    time.sleep(sleep_seconds)
                    continue

                if cycle_id == last_completed_cycle_id:
                    sleep_seconds = _seconds_until_next_window_open(now)
                    _log_cycle_idle_message(cycle_id, now, sleep_seconds)
                    time.sleep(sleep_seconds)
                    continue

                logger.info("=" * 60)
                logger.info(f"  Bhavcopy scheduler -- running cycle {fmt_date(cycle_id)} (triggered at {fmt_datetime(now)})")
                logger.info("=" * 60)
                ran_ok = check_and_process(env_values)

                if ran_ok:
                    last_completed_cycle_id = cycle_id
                    _save_last_completed_cycle_id(cycle_id)
                    now_after_run = datetime.now(IST)
                    sleep_seconds = _seconds_until_next_window_open(now_after_run)
                    _log_cycle_idle_message(cycle_id, now_after_run, sleep_seconds)
                    time.sleep(sleep_seconds)
                else:
                    logger.error(f"  Cycle {fmt_date(cycle_id)} failed to even start -- "
                          f"retrying in {CHECK_INTERVAL_SECONDS // 60} min, still within this same cycle's window.")
                    time.sleep(CHECK_INTERVAL_SECONDS)
        except KeyboardInterrupt:
            logger.info("\nShutting down.")


if __name__ == "__main__":
    run()
