# corporate_actions_listener.py
#
# Corporate Actions Adjustment epic -- unattended polling listener.
# Runs indefinitely, periodically re-downloading NSE + BSE corporate
# actions for a rolling calendar window and running them through
# core/corporate_actions/pipeline.py's shared parse -> persist ->
# reconcile -> targeted-RSI-reprocess logic -- the SAME logic
# loaders/corporate_actions_loader.py uses for a manual, one-off,
# user-picked date range. This listener is what makes "a new
# corporate action showed up" something that gets acted on without you
# having to remember to run the interactive loader yourself.
#
# Run manually for testing/development, or leave running in a terminal/
# service the same way you'd run indicators_listener.py:
#   python corporate_actions_listener.py
#
# DESIGN, contrasted with indicators_listener.py (RSI's listener):
#   - indicators_listener.py walks forward one REAL TRADING DAY at a
#     time, sourced from bhav_copy_metadata's own SUCCESS history, and
#     needs a persistent per-indicator cursor (indicators_workbook_
#     metadata) because re-processing an already-done trade_date would
#     be wasted work. Corporate actions have no such structure -- NSE/
#     BSE report them against arbitrary CALENDAR dates, not trading
#     days, and persist_raw()'s ON CONFLICT DO UPDATE is already fully
#     idempotent. So this listener carries NO stored cursor at all --
#     every poll cycle just re-fetches the same rolling window,
#     re-upserts it (a no-op for anything already seen and unchanged),
#     and only NEWLY-MATCHED keys trigger a fresh RSI reprocess (see
#     corporate_actions_persistence.reconcile()'s was_already_matched
#     check) -- simpler than indicators_registry's cursor/bootstrap
#     machinery, and correct for this data's actual shape.
#   - No threading -- there is exactly one job per cycle (poll both
#     exchanges, run the pipeline), not one thread per registered
#     indicator.
#   - Polling interval is hours, not seconds -- NSE/BSE publish
#     corporate-actions data at most a few times a day, unlike RSI
#     which reacts to a fresh BhavCopy the moment it lands. Polling
#     every 60s here would just be hammering the endpoint for no
#     benefit.
#
# WINDOW: LOOKBACK_DAYS catches an exchange correcting/re-publishing an
# action shortly after its original announcement; LOOKAHEAD_DAYS catches
# actions NSE/BSE pre-announce with a future ex_date, so the isin is
# already MATCHED (and its RSI already correctly adjusted) well before
# that ex_date actually arrives.

import sys
import time
from datetime import date, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from core.env_validator import load_and_validate_env, EnvValidationError
from core.db_client import get_connection, DbConnectionError
from core.logging_setup import start_run_logging
from core.corporate_actions.corporate_actions_downloader import (
    download_nse_corporate_actions,
    download_bse_corporate_actions,
    CorporateActionsDownloadError,
    NSE_CORPORATE_ACTIONS_URL,
    BSE_CORPORATE_ACTIONS_URL,
)
from core.corporate_actions.pipeline import run_pipeline, CorporateActionsPipelineError

POLL_INTERVAL_SECONDS = 6 * 60 * 60  # every 6 hours
LOOKBACK_DAYS = 7
LOOKAHEAD_DAYS = 30


def poll_once(env_values):
    print("=" * 60)
    print("  Corporate actions listener -- poll cycle starting")
    print("=" * 60)

    today = date.today()
    from_date = today - timedelta(days=LOOKBACK_DAYS)
    to_date = today + timedelta(days=LOOKAHEAD_DAYS)
    print(f"  Window: {from_date} to {to_date}")

    try:
        nse_raw_rows = download_nse_corporate_actions(from_date, to_date)
        print(f"  [OK] NSE: {len(nse_raw_rows)} raw row(s).")
    except CorporateActionsDownloadError as e:
        print(f"  [FAILED] NSE download: {e}")
        nse_raw_rows = []

    try:
        bse_raw_rows = download_bse_corporate_actions(from_date, to_date)
        print(f"  [OK] BSE: {len(bse_raw_rows)} raw row(s).")
    except CorporateActionsDownloadError as e:
        print(f"  [FAILED] BSE download: {e}")
        bse_raw_rows = []

    if not nse_raw_rows and not bse_raw_rows:
        print("  [SKIP] Both exchanges failed or returned nothing -- nothing to do this cycle.")
        print("  Poll cycle complete.")
        print("=" * 60)
        return

    try:
        conn = get_connection(env_values)
    except DbConnectionError as e:
        print(f"  [FAILED] Could not open a connection: {e}")
        print("=" * 60)
        return

    try:
        summary = run_pipeline(
            conn, nse_raw_rows, bse_raw_rows,
            nse_source_url=NSE_CORPORATE_ACTIONS_URL,
            bse_source_url=BSE_CORPORATE_ACTIONS_URL,
        )
    except CorporateActionsPipelineError as e:
        print(f"  [FAILED] Pipeline error: {e}")
        conn.close()
        print("=" * 60)
        return
    finally:
        conn.close()

    if summary["unparsed_ratio_count"]:
        print(f"  [WARNING] {summary['unparsed_ratio_count']} row(s) matched a SPLIT/BONUS keyword but "
              f"their ratio text didn't match a known pattern -- see corporate_actions_raw for manual review.")

    if summary["bse_unresolved_isin_count"]:
        print(f"  [INFO] {summary['bse_unresolved_isin_count']} BSE row(s) dropped -- scrip_code had no "
              f"stock_universe match (debt/mutual-fund/delisted, expected, not an error).")

    if not summary["newly_matched_keys"]:
        print(f"  [OK] {len(summary['touched_keys'])} action key(s) touched, nothing newly MATCHED this cycle.")
    else:
        # THIS is "the notification" -- the moment a newly-detected,
        # cross-exchange-confirmed corporate action shows up in this
        # cycle's log output, its impacted isin(s) have already had
        # their rsi14d_workbook history rebuilt (see the per-isin lines
        # below) and the row is already visible to the tmt dashboard's
        # Corporate Actions card the next time it's loaded -- no
        # separate push/email step exists or is needed.
        print(f"  [NEW] {len(summary['newly_matched_keys'])} newly MATCHED action(s) this cycle:")
        for isin, action_type, ex_date in summary["newly_matched_keys"]:
            print(f"    {isin}  {action_type}  ex_date={ex_date}")
        for r in summary["reprocess_results"]:
            if r["error"]:
                print(f"    [FAILED] {r['isin']} ({r['exchange']}) RSI reprocess: {r['error']}")
            else:
                print(f"    [OK] {r['isin']} ({r['exchange']}): rebuilt {r['written']} rsi14d_workbook row(s).")

    print("  Poll cycle complete.")
    print("=" * 60)


def run():
    """Standard entry point -- also callable via loaders.py, matching every other loader under loaders/."""
    with start_run_logging("corporate_actions_listener"):
        try:
            env_values = load_and_validate_env()
        except EnvValidationError as e:
            print(f"[FAILED] {e}")
            sys.exit(1)

        print(f"Corporate actions listener starting -- polling every {POLL_INTERVAL_SECONDS}s "
              f"({POLL_INTERVAL_SECONDS // 3600}h). Ctrl+C to stop.")
        try:
            while True:
                poll_once(env_values)
                time.sleep(POLL_INTERVAL_SECONDS)
        except KeyboardInterrupt:
            print("\nShutting down -- safe to interrupt between cycles (each cycle's writes commit "
                  "as a single transaction; persist_raw()/reconcile() are idempotent to re-run).")


if __name__ == "__main__":
    run()
