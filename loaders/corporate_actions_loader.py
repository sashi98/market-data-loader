# loaders/corporate_actions_loader.py
#
# market-data-loader -- Corporate Actions (splits/bonus) ingestion +
# reconciliation loader. Fixes the RSI-divergence bug documented in
# docs/current-issues.txt: rsi14d_workbook was computed off bhav_copy's
# raw, unadjusted close/prev_close, so a split/bonus produces one
# artificially huge gain/loss that Wilder's smoothing then bleeds into
# RSI14 for weeks afterward (confirmed live example: ZFCVINDIA,
# isin INE342J01019).
#
# FULL SCOPE --
#   Step 0: Load + validate config/.env, direct DB connectivity check.
#           No health check / auth step needed (unlike bhav_copy_with_corporate_action_loader)
#           -- this loader only talks to Postgres directly plus NSE/BSE's
#           own public endpoints, never the TMT REST API.
#   Step 1: Prompt for from_date/to_date -- a plain CALENDAR-date window,
#           NOT a trading-date list (corporate actions are announced on
#           any calendar day, not just trading days).
#   Step 2: Download NSE + BSE corporate-actions data for the range
#           (core/corporate_actions/corporate_actions_downloader.py).
#   Step 3: Parse both (SPLIT/BONUS only, everything else out of scope),
#           persist_raw() + reconcile() as ONE transaction, then trigger
#           a targeted rsi14d_workbook rebuild for every isin that got a
#           newly-MATCHED action this run -- NOT a global recompute
#           (scoped backfill, per this epic's design decision).
#   Step 4: Summary report -- raw rows per exchange, reconciliation
#           breakdown, isins reprocessed, rsi14d_workbook rows
#           rewritten, elapsed time.
#
# Exposes run() -- the standard entry point every loader under loaders/
# provides, so loaders.py (the menu launcher) can invoke it generically.
#
# Can also be run standalone for development/testing:
#   python corporate_actions_loader.py

import sys
import time
from datetime import datetime
from pathlib import Path

# Allow `from core.xxx import yyy` when run directly as a script
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from core.env_validator import load_and_validate_env, EnvValidationError
from core.db_client import get_connection, test_connection, DbConnectionError
from core.logging_setup import start_run_logging
from core.corporate_actions.corporate_actions_downloader import CorporateActionsDownloadError
from core.corporate_actions.corporate_actions_persistence import CorporateActionsPersistenceError
from core.corporate_actions.pipeline import CorporateActionsPipelineError
from core.corporate_actions.csv_pipeline import run_csv_pipeline, CorporateActionsCsvPipelineError

DATE_INPUT_FORMAT = "%d%m%Y"  # DDMMYYYY


def step_0():
    print("=" * 60)
    print("  STEP 0 -- Environment validation, DB connectivity")
    print("=" * 60)

    print("\n[0.1] Validating config/.env ...")
    try:
        env_values = load_and_validate_env()
    except EnvValidationError as e:
        print(f"  [FAILED] {e}")
        sys.exit(1)
    print("  [OK] All required keys present.")

    print("\n[0.2] Checking direct DB connectivity ...")
    try:
        test_connection(env_values)
    except DbConnectionError as e:
        print(f"  [FAILED] {e}")
        sys.exit(1)
    print("  [OK] Connected to Postgres successfully.")

    print("\n" + "=" * 60)
    print("  STEP 0 COMPLETE")
    print("=" * 60)

    return {"env_values": env_values}


def step_1():
    print("\n" + "=" * 60)
    print("  STEP 1 -- Date range (calendar dates, not trading dates)")
    print("=" * 60)

    default_to_date_str = datetime.now().strftime(DATE_INPUT_FORMAT)
    to_date_str = input(f"Input to_date(DDMMYYYY) [default: {default_to_date_str}]: ").strip()
    if not to_date_str:
        to_date_str = default_to_date_str
    try:
        to_date = datetime.strptime(to_date_str, DATE_INPUT_FORMAT).date()
    except ValueError:
        print(f"  [FAILED] Invalid date '{to_date_str}' -- expected DDMMYYYY.")
        sys.exit(1)

    from_date_str = input("Input from_date(DDMMYYYY): ").strip()
    try:
        from_date = datetime.strptime(from_date_str, DATE_INPUT_FORMAT).date()
    except ValueError:
        print(f"  [FAILED] Invalid date '{from_date_str}' -- expected DDMMYYYY.")
        sys.exit(1)

    if from_date > to_date:
        print(f"  [FAILED] from_date ({from_date_str}) cannot be after to_date ({to_date_str}).")
        sys.exit(1)

    print(f"\nFetching corporate actions from {from_date} to {to_date}.")

    print("\n" + "=" * 60)
    print("  STEP 1 COMPLETE")
    print("=" * 60)

    return {"from_date": from_date, "to_date": to_date}


def step_2(env_values, from_date, to_date):
    """
    Downloads + parses + persists + reconciles + targeted-RSI-reprocesses
    corporate actions for BOTH exchanges, one at a time, via
    core/corporate_actions/csv_pipeline.py's run_csv_pipeline() -- the
    same CSV-export-based path stock-py-services' corporate-actions
    endpoint uses (triggered automatically after every bhav-copy load).
    Standardized on CSV for both exchanges as of 2026-08-13 -- see
    corporate_actions_downloader.py's module docstring for why NSE's
    older JSON endpoint and BSE's DefaultData/w JSON endpoint are no
    longer used here (the latter was proven to ignore date-range params
    entirely; NSE's JSON endpoint works fine but CSV is now the one
    consistent path for both exchanges).

    A failure on one exchange does NOT abort the other -- each is
    independent, same "isolate the failure, keep going" spirit as the
    old step_2/step_3 split had for NSE vs BSE downloads.
    """
    print("\n" + "=" * 60)
    print("  STEP 2 -- Download (CSV) + parse + persist + reconcile, per exchange")
    print("=" * 60)

    try:
        conn = get_connection(env_values)
    except DbConnectionError as e:
        print(f"  [FAILED] {e}")
        sys.exit(1)

    per_exchange_results = []
    try:
        for exchange in ("NSE", "BSE"):
            print(f"\n[2.{exchange}] {exchange} ...")
            try:
                summary = run_csv_pipeline(conn, exchange, from_date, to_date)
            except (CorporateActionsDownloadError, CorporateActionsCsvPipelineError,
                     CorporateActionsPipelineError, CorporateActionsPersistenceError) as e:
                print(f"  [FAILED] {exchange}: {e}")
                per_exchange_results.append({"exchange": exchange, "summary": None, "error": str(e)})
                continue

            if summary["unparsed_ratio_count"]:
                print(f"  [WARNING] {summary['unparsed_ratio_count']} row(s) matched a SPLIT/BONUS keyword but "
                      f"their ratio text didn't match a known pattern -- see corporate_actions_raw for manual review.")
            if summary["unresolved_isin_count"]:
                print(f"  [INFO] {summary['unresolved_isin_count']} row(s) dropped -- no stock_universe match "
                      f"(debt/mutual-fund/delisted, expected, not an error).")

            print(f"  [OK] {summary['nse_parsed_count'] + summary['bse_parsed_count']} split/bonus row(s) "
                  f"classified for {exchange}.")
            print(f"  [OK] {len(summary['touched_keys'])} distinct (isin, action_type, ex_date) key(s) touched, "
                  f"{len(summary['newly_matched_keys'])} newly MATCHED this run.")

            if not summary["newly_matched_keys"]:
                print(f"  [SKIP] No newly-matched actions for {exchange} this run -- nothing to reprocess.")
            else:
                for r in summary["reprocess_results"]:
                    if r["error"]:
                        print(f"  [FAILED] {r['isin']} ({r['exchange']}): {r['error']}")
                    else:
                        print(f"  [OK] {r['isin']} ({r['exchange']}): rebuilt {r['written']} rsi14d_workbook row(s).")

            per_exchange_results.append({"exchange": exchange, "summary": summary, "error": None})
    finally:
        conn.close()

    print("\n" + "=" * 60)
    print("  STEP 2 COMPLETE")
    print("=" * 60)

    return {"per_exchange_results": per_exchange_results}


def step_3(step_1_context, step_2_context, elapsed_seconds):
    print("\n" + "=" * 60)
    print("  STEP 3 -- Summary report")
    print("=" * 60)

    minutes, seconds = divmod(int(elapsed_seconds), 60)
    per_exchange_results = step_2_context["per_exchange_results"]

    total_parsed = 0
    total_touched = 0
    total_newly_matched = 0
    total_written = 0
    failed_exchanges = []
    failed_reprocess = []

    for r in per_exchange_results:
        if r["error"]:
            failed_exchanges.append(r)
            continue
        summary = r["summary"]
        total_parsed += summary["nse_parsed_count"] + summary["bse_parsed_count"]
        total_touched += len(summary["touched_keys"])
        total_newly_matched += len(summary["newly_matched_keys"])
        total_written += sum(x["written"] for x in summary["reprocess_results"])
        failed_reprocess.extend(x for x in summary["reprocess_results"] if x["error"])

    print(f"\nDate range:                     {step_1_context['from_date']} to {step_1_context['to_date']}")
    print(f"Exchanges processed:            {len(per_exchange_results) - len(failed_exchanges)} / {len(per_exchange_results)}")
    if failed_exchanges:
        print(f"Exchanges that FAILED entirely:")
        for r in failed_exchanges:
            print(f"  {r['exchange']}: {r['error']}")
    print(f"Total split/bonus rows parsed:   {total_parsed}")
    print(f"Distinct action keys touched:    {total_touched}")
    print(f"Newly MATCHED this run:          {total_newly_matched}")
    print(f"rsi14d_workbook rows rewritten:  {total_written}")
    if failed_reprocess:
        print(f"\nReprocess failures ({len(failed_reprocess)}):")
        for r in failed_reprocess:
            print(f"  {r['isin']} ({r['exchange']}): {r['error']}")
    print(f"\nElapsed time: {minutes}m {seconds}s")

    print("\n" + "=" * 60)
    print("  RUN COMPLETE")
    print("=" * 60)


def run():
    """Standard entry point called by loaders.py (or directly, standalone)."""
    with start_run_logging("corporate_actions_loader"):
        run_start_time = time.time()
        step_0_context = step_0()
        step_1_context = step_1()
        step_2_context = step_2(
            step_0_context["env_values"],
            step_1_context["from_date"],
            step_1_context["to_date"],
        )
        elapsed_seconds = time.time() - run_start_time
        step_3(step_1_context, step_2_context, elapsed_seconds)


if __name__ == "__main__":
    run()
