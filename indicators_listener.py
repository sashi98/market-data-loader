# indicators_listener.py
#
# Indicators Framework's polling listener -- Stories 3 and 4
# (docs/epics/indicators-framework/indicators-framework-epic-v1.html).
# Runs indefinitely, polling for newly-complete BhavCopy trade_dates and
# launching one catch-up thread per eligible ACTIVE indicator.
#
# Run manually for testing/development:
#   python indicators_listener.py
#
# Each catch-up thread:
#   - Opens its OWN db connection. psycopg2 connections are not
#     thread-safe to share across threads -- every thread here gets its
#     own get_connection() call, closed when the thread finishes.
#   - The FIRST time it sees a NULL IWM cursor for an indicator,
#     auto-bootstraps it via that indicator's own dispatch.bootstrap()
#     function (e.g. rsi14d's reads rsi14d_workbook's own current
#     MAX(trade_date) rather than assuming any starting point) --
#     see core/indicators/dispatch.py for why this stays generic instead
#     of hardcoding indicator-specific knowledge into this listener.
#   - Walks forward ONE real trading day at a time, sourced directly
#     from bhav_copy_metadata's own SUCCESS history (not a separate
#     calendar), from the cursor through BhavCopy's latest complete date.
#   - On success for a date: advances IWM, clears any matching
#     indicators_open_failures row, continues to the next date.
#   - On failure for a date: records the failure, flips the indicator to
#     DEACTIVE, stops immediately -- no further dates attempted, no
#     automatic retry. Stays broken until a human investigates and
#     flips it back to ACTIVE (Sashikant's call).

import sys
import time
import threading
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from core.env_validator import load_and_validate_env, EnvValidationError
from core.db_client import get_connection, DbConnectionError
from core.logging_setup import start_run_logging
from core.indicators.dispatch import get_handlers
from core.indicators.persistence import (
    fetch_active_indicators, fetch_iwm_cursor, fetch_complete_trade_dates_after,
    record_success, record_bootstrap, record_failure, IndicatorsPersistenceError,
)

POLL_INTERVAL_SECONDS = 60


def catch_up_indicator(env_values, indicator_id):
    """
    Runs in its own thread -- opens its own connection, walks this one
    indicator forward as far as BhavCopy's data allows, stops on the
    first failure or once it is fully caught up.
    """
    handlers = get_handlers(indicator_id)
    if handlers is None:
        print(f"  [{indicator_id}] No dispatch entry yet -- loader not built, skipping this cycle.")
        return

    try:
        conn = get_connection(env_values)
    except DbConnectionError as e:
        print(f"  [{indicator_id}] Failed to open a connection: {e}")
        return

    try:
        cursor_date = fetch_iwm_cursor(conn, indicator_id)

        if cursor_date is None:
            bootstrap_date = handlers.bootstrap(conn)
            if bootstrap_date is None:
                print(f"  [{indicator_id}] No prior workbook data at all -- run its Part 1 "
                      f"bulk backfill before this listener can catch it up. Skipping this cycle.")
                return
            record_bootstrap(conn, indicator_id, bootstrap_date)
            print(f"  [{indicator_id}] Auto-bootstrapped IWM cursor to {bootstrap_date}.")
            cursor_date = bootstrap_date

        pending_dates = fetch_complete_trade_dates_after(conn, cursor_date)
        if not pending_dates:
            print(f"  [{indicator_id}] Already caught up through {cursor_date}.")
            return

        for trade_date in pending_dates:
            try:
                summary = handlers.run(conn, trade_date)
            except Exception as e:
                print(f"  [{indicator_id}] FAILED on {trade_date}: {e}")
                record_failure(conn, indicator_id, trade_date, str(e))
                print(f"  [{indicator_id}] Flipped to DEACTIVE -- stopping, no further dates attempted.")
                return

            record_success(conn, indicator_id, trade_date)
            print(f"  [{indicator_id}] {trade_date} complete -- {summary}")

        print(f"  [{indicator_id}] Caught up through {pending_dates[-1]}.")

    except IndicatorsPersistenceError as e:
        print(f"  [{indicator_id}] Bookkeeping error: {e}")
    finally:
        conn.close()


def poll_once(env_values):
    print("=" * 60)
    print("  Indicators listener -- poll cycle starting")
    print("=" * 60)

    try:
        conn = get_connection(env_values)
        active_indicators = fetch_active_indicators(conn)
        conn.close()
    except (DbConnectionError, IndicatorsPersistenceError) as e:
        print(f"  [FAILED] Could not fetch active indicators: {e}")
        return

    print(f"  Active indicators this cycle: {active_indicators}")

    threads = []
    for indicator_id in active_indicators:
        t = threading.Thread(
            target=catch_up_indicator, args=(env_values, indicator_id), name=f"catchup-{indicator_id}"
        )
        t.start()
        threads.append(t)

    for t in threads:
        t.join()

    print("  Poll cycle complete.")
    print("=" * 60)


def run():
    """Standard entry point -- also callable via main.py, matching every other loader under loaders/."""
    with start_run_logging("indicators_listener"):
        try:
            env_values = load_and_validate_env()
        except EnvValidationError as e:
            print(f"[FAILED] {e}")
            sys.exit(1)

        print(f"Indicators listener starting -- polling every {POLL_INTERVAL_SECONDS}s. Ctrl+C to stop.")
        try:
            while True:
                poll_once(env_values)
                time.sleep(POLL_INTERVAL_SECONDS)
        except KeyboardInterrupt:
            print("\nShutting down -- any in-flight catch-up threads will finish their current "
                  "date's write before exiting (writes are idempotent, safe to interrupt between dates).")


if __name__ == "__main__":
    run()
