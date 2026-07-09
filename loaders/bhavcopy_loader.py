# loaders/bhavcopy_loader.py
#
# market-data-loader -- BhavCopy backfill loader.
#
# FULL SCOPE (all steps implemented) --
#   Step 0: Load + validate config/.env, pre-flight health check,
#           admin authentication (JWT for /api/holidays/sync/{year}),
#           direct DB connectivity check
#   Step 1: Prompt for to_date (defaults to today/yesterday per the 8pm
#           BhavCopy-availability cutoff) and from_date (always typed),
#           derive the actual trading_date_list between them (weekends +
#           holidays excluded)
#   Step 2: Download NSE (zip) + BSE (direct CSV) BhavCopy files for each
#           trading date into MARKET_DATA_LOADER_DOWNLOAD_DIR
#   Step 3: Parse + persist NSE then BSE per date -- NSE fresh insert,
#           BSE upsert-merge by ISIN, each an independent transaction
#   Step 4: Summary report -- date-range breakdown (fully/partially/fully
#           failed), total rows persisted per exchange, elapsed time,
#           and a list of every error encountered with its exact message
#
# Exposes run() -- the standard entry point every loader under loaders/
# provides, so main.py can invoke any loader generically without knowing
# its internals.
#
# Can also be run standalone for development/testing:
#   python bhavcopy_loader.py

import sys
import time
from datetime import datetime
from pathlib import Path

# Allow `from core.xxx import yyy` when run directly as a script
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from core.env_validator import load_and_validate_env, EnvValidationError
from core.health_client import check_health, HealthCheckError
from core.auth_client import authenticate, AuthError
from core.trading_calendar import (
    compute_trading_date_range_between,
    compute_default_to_date,
    TradingCalendarError,
)
from core.bhavcopy.bhavcopy_downloader import (
    download_bhavcopy,
    is_nse_legacy_format,
    BhavCopyNotFoundError,
    BhavCopyDownloadError,
)
from core.db_client import get_connection, test_connection, DbConnectionError
from core.bhavcopy.bhavcopy_parser import parse_bhavcopy_csv, BhavCopyParseError
from core.bhavcopy.bhavcopy_persistence import persist_nse, persist_bse, is_already_persisted, BhavCopyPersistenceError
from core.logging_setup import start_run_logging

DATE_INPUT_FORMAT = "%d%m%Y"  # DDMMYYYY
RATE_LIMIT_SECONDS = 1.5  # sleep between download requests


def step_0():
    print("=" * 60)
    print("  STEP 0 -- Environment validation, health check, auth")
    print("=" * 60)

    # -- 0.1: Validate .env --
    print("\n[0.1] Validating config/.env ...")
    try:
        env_values = load_and_validate_env()
    except EnvValidationError as e:
        print(f"  [FAILED] {e}")
        sys.exit(1)
    print("  [OK] All required keys present.")

    tmt_app_base_url = env_values["TMT_APP_BASE_URL"]

    # -- 0.2: Health check --
    print(f"\n[0.2] Checking TMT + stock-py-services health at {tmt_app_base_url}/actuator/health ...")
    try:
        health_body = check_health(tmt_app_base_url)
    except HealthCheckError as e:
        print(f"  [FAILED] {e}")
        sys.exit(1)
    print(f"  [OK] Overall status: {health_body.get('status')}")

    # -- 0.3: Admin authentication --
    print("\n[0.3] Authenticating ...")
    try:
        jwt_token = authenticate(tmt_app_base_url)
    except AuthError as e:
        print(f"  [FAILED] {e}")
        sys.exit(1)

    # -- 0.4: DB connectivity check --
    print("\n[0.4] Checking direct DB connectivity ...")
    try:
        test_connection(env_values)
    except DbConnectionError as e:
        print(f"  [FAILED] {e}")
        sys.exit(1)
    print("  [OK] Connected to Postgres successfully.")

    print("\n" + "=" * 60)
    print("  STEP 0 COMPLETE")
    print("=" * 60)

    return {
        "env_values": env_values,
        "jwt_token": jwt_token,
    }


def step_1(env_values, jwt_token):
    print("\n" + "=" * 60)
    print("  STEP 1 -- Build trading date list")
    print("=" * 60)

    tmt_app_base_url = env_values["TMT_APP_BASE_URL"]

    # -- 1.1: Prompt for to_date (pre-filled default -- Enter accepts it, or type any other date) --
    default_to_date = compute_default_to_date()
    default_to_date_str = default_to_date.strftime(DATE_INPUT_FORMAT)
    to_date_str = input(f"Input to_date(DDMMYYYY) [default: {default_to_date_str}]: ").strip()
    if not to_date_str:
        to_date_str = default_to_date_str
    try:
        to_date = datetime.strptime(to_date_str, DATE_INPUT_FORMAT).date()
    except ValueError:
        print(f"  [FAILED] Invalid date '{to_date_str}' -- expected DDMMYYYY (e.g. 05072026).")
        sys.exit(1)

    # -- 1.2: Prompt for from_date (no default -- always typed) --
    from_date_str_input = input("Input from_date(DDMMYYYY): ").strip()
    try:
        from_date = datetime.strptime(from_date_str_input, DATE_INPUT_FORMAT).date()
    except ValueError:
        print(f"  [FAILED] Invalid date '{from_date_str_input}' -- expected DDMMYYYY (e.g. 01012024).")
        sys.exit(1)

    if from_date > to_date:
        print(f"  [FAILED] from_date ({from_date_str_input}) cannot be after to_date ({to_date_str}).")
        sys.exit(1)

    # -- 1.3: Derive the actual trading_date_list between from_date and to_date --
    try:
        result = compute_trading_date_range_between(
            from_date, to_date, tmt_app_base_url, jwt_token
        )
    except TradingCalendarError as e:
        print(f"  [FAILED] {e}")
        sys.exit(1)

    trading_date_list = result["trading_date_list"]
    weekend_count = result["weekend_count"]
    holiday_count = result["holiday_count"]
    num_days = len(trading_date_list)

    from_date_confirm_str = from_date.strftime(DATE_INPUT_FORMAT)
    to_date_confirm_str = to_date.strftime(DATE_INPUT_FORMAT)

    total_calendar_days = num_days + weekend_count + holiday_count

    print(f"\nRange {from_date_confirm_str} to {to_date_confirm_str} contains {num_days} trading days, "
          f"after excluding weekends(Saturday,Sundays) + Holidays")
    print(f"  Weekends excluded: {weekend_count}")
    print(f"  Holidays excluded: {holiday_count}")
    print(f"  Total calendar days: {total_calendar_days} "
          f"({num_days} trading + {weekend_count} weekends + {holiday_count} holidays)")
    print(f"BhavCopy will be fetched from {from_date_confirm_str} to {to_date_confirm_str}")

    confirm = input("Continue Y/N: ").strip().upper()
    if confirm != "Y":
        print("Aborted by user.")
        sys.exit(0)

    print("\n" + "=" * 60)
    print("  STEP 1 COMPLETE")
    print("=" * 60)

    return {
        "to_date": to_date,
        "from_date": from_date,
        "num_days": num_days,
        "trading_date_list": trading_date_list,
    }


def step_2(env_values, trading_date_list):
    print("\n" + "=" * 60)
    print("  STEP 2 -- Download BhavCopy files (NSE + BSE)")
    print("=" * 60)

    download_dir = env_values["MARKET_DATA_LOADER_DOWNLOAD_DIR"]
    total = len(trading_date_list)
    date_bc_map = {}
    skipped_count = 0
    downloaded_count = 0
    legacy_format_count = 0

    for idx, trade_date in enumerate(trading_date_list, start=1):
        date_display = trade_date.strftime("%d-%b-%Y")
        print(f"\n[{idx}/{total}] {date_display}")

        nse_path = None
        bse_path = None
        nse_skipped = False
        bse_skipped = False

        # -- NSE --
        try:
            downloaded_path, nse_skipped, _nse_is_legacy = download_bhavcopy("NSE", trade_date, download_dir)
            nse_is_legacy = is_nse_legacy_format(trade_date)  # reliable regardless of skip status
            if nse_skipped:
                print(f"  [SKIP] NSE already exists -> {downloaded_path}")
                skipped_count += 1
            elif nse_is_legacy:
                print(f"  [OK] NSE downloaded (legacy format) -> {downloaded_path}")
                downloaded_count += 1
                legacy_format_count += 1
            else:
                print(f"  [OK] NSE downloaded -> {downloaded_path}")
                downloaded_count += 1
            nse_path = downloaded_path
        except BhavCopyNotFoundError as e:
            print(f"  [SKIP] NSE not available: {e}")
        except BhavCopyDownloadError as e:
            print(f"  [FAILED] NSE download error: {e}")

        # Only rate-limit when an actual network request was made -- no
        # need to pause after a same-file skip, since nothing was sent.
        if not nse_skipped:
            time.sleep(RATE_LIMIT_SECONDS)

        # -- BSE --
        try:
            downloaded_path, bse_skipped, _bse_is_legacy = download_bhavcopy("BSE", trade_date, download_dir)
            if bse_skipped:
                print(f"  [SKIP] BSE already exists -> {downloaded_path}")
                skipped_count += 1
            else:
                print(f"  [OK] BSE downloaded -> {downloaded_path}")
                downloaded_count += 1
            bse_path = downloaded_path
        except BhavCopyNotFoundError as e:
            print(f"  [SKIP] BSE not available: {e}")
        except BhavCopyDownloadError as e:
            print(f"  [FAILED] BSE download error: {e}")

        if not bse_skipped:
            time.sleep(RATE_LIMIT_SECONDS)

        date_bc_map[trade_date] = [nse_path, bse_path]

    both_count = sum(1 for v in date_bc_map.values() if v[0] and v[1])
    nse_only_count = sum(1 for v in date_bc_map.values() if v[0] and not v[1])
    bse_only_count = sum(1 for v in date_bc_map.values() if v[1] and not v[0])
    neither_count = sum(1 for v in date_bc_map.values() if not v[0] and not v[1])

    print("\n" + "=" * 60)
    print("  STEP 2 COMPLETE")
    print("=" * 60)
    print(f"  Both exchanges downloaded: {both_count}")
    print(f"  NSE only:                  {nse_only_count}")
    print(f"  BSE only:                  {bse_only_count}")
    print(f"  Neither downloaded:        {neither_count}")
    print(f"  Newly downloaded (files):  {downloaded_count}")
    print(f"  Skipped (already on disk): {skipped_count}")
    if legacy_format_count:
        print(f"  NSE legacy-format files:   {legacy_format_count}")

    return {
        "date_bc_map": date_bc_map,
    }


def step_3(env_values, trading_date_list, date_bc_map):
    print("\n" + "=" * 60)
    print("  STEP 3 -- Parse and persist (NSE then BSE, per date)")
    print("=" * 60)

    try:
        conn = get_connection(env_values)
    except DbConnectionError as e:
        print(f"  [FAILED] {e}")
        sys.exit(1)

    total = len(trading_date_list)
    # per-date status: "NSE" -> True/False/None (None = no file to try),
    #                  "BSE" -> True/False/None
    date_status = {}
    total_nse_rows = 0
    total_bse_rows = 0
    errors = []  # list of {"date": date, "exchange": "NSE"/"BSE", "error": str}

    try:
        for idx, trade_date in enumerate(trading_date_list, start=1):
            date_display = trade_date.strftime("%d-%b-%Y")
            print(f"\n[{idx}/{total}] {date_display}")

            nse_path, bse_path = date_bc_map.get(trade_date, [None, None])
            nse_ok = None
            bse_ok = None

            # -- NSE: parse then persist --
            if nse_path:
                if is_already_persisted(conn, trade_date, "NSE"):
                    print("  [SKIP] NSE already persisted for this date (re-run safe)")
                    nse_ok = True
                else:
                    start_time_ms = time.time() * 1000
                    try:
                        rows = parse_bhavcopy_csv(nse_path, trade_date)
                        persist_nse(conn, rows, trade_date, nse_path, start_time_ms)
                        print(f"  [OK] NSE persisted -- {len(rows)} rows")
                        nse_ok = True
                        total_nse_rows += len(rows)
                    except BhavCopyParseError as e:
                        print(f"  [FAILED] NSE parse error: {e}")
                        nse_ok = False
                        errors.append({"date": trade_date, "exchange": "NSE", "error": str(e)})
                    except BhavCopyPersistenceError as e:
                        print(f"  [FAILED] NSE persistence error: {e}")
                        nse_ok = False
                        errors.append({"date": trade_date, "exchange": "NSE", "error": str(e)})
            else:
                print("  [SKIP] NSE -- no file downloaded for this date")

            # -- BSE: parse then persist -- independent of NSE outcome --
            if bse_path:
                if is_already_persisted(conn, trade_date, "BSE"):
                    print("  [SKIP] BSE already persisted for this date (re-run safe)")
                    bse_ok = True
                else:
                    start_time_ms = time.time() * 1000
                    try:
                        rows = parse_bhavcopy_csv(bse_path, trade_date)
                        persist_bse(conn, rows, trade_date, bse_path, start_time_ms)
                        print(f"  [OK] BSE persisted -- {len(rows)} rows")
                        bse_ok = True
                        total_bse_rows += len(rows)
                    except BhavCopyParseError as e:
                        print(f"  [FAILED] BSE parse error: {e}")
                        bse_ok = False
                        errors.append({"date": trade_date, "exchange": "BSE", "error": str(e)})
                    except BhavCopyPersistenceError as e:
                        print(f"  [FAILED] BSE persistence error: {e}")
                        bse_ok = False
                        errors.append({"date": trade_date, "exchange": "BSE", "error": str(e)})
            else:
                print("  [SKIP] BSE -- no file downloaded for this date")

            date_status[trade_date] = {"NSE": nse_ok, "BSE": bse_ok}
    finally:
        conn.close()

    both_ok = sum(1 for s in date_status.values() if s["NSE"] is True and s["BSE"] is True)
    nse_only_ok = sum(1 for s in date_status.values() if s["NSE"] is True and s["BSE"] is not True)
    bse_only_ok = sum(1 for s in date_status.values() if s["BSE"] is True and s["NSE"] is not True)
    neither_ok = sum(1 for s in date_status.values() if s["NSE"] is not True and s["BSE"] is not True)

    print("\n" + "=" * 60)
    print("  STEP 3 COMPLETE")
    print("=" * 60)
    print(f"  Both exchanges persisted: {both_ok}")
    print(f"  NSE only:                 {nse_only_ok}")
    print(f"  BSE only:                 {bse_only_ok}")
    print(f"  Neither persisted:        {neither_ok}")

    return {
        "date_status": date_status,
        "total_nse_rows": total_nse_rows,
        "total_bse_rows": total_bse_rows,
        "errors": errors,
    }


def step_4(step_1_context, step_3_context, elapsed_seconds):
    print("\n" + "=" * 60)
    print("  STEP 4 -- Summary report")
    print("=" * 60)

    trading_date_list = step_1_context["trading_date_list"]
    date_status = step_3_context["date_status"]
    total_nse_rows = step_3_context["total_nse_rows"]
    total_bse_rows = step_3_context["total_bse_rows"]
    errors = step_3_context["errors"]

    both_ok = [d for d, s in date_status.items() if s["NSE"] is True and s["BSE"] is True]
    nse_only_ok = [d for d, s in date_status.items() if s["NSE"] is True and s["BSE"] is not True]
    bse_only_ok = [d for d, s in date_status.items() if s["BSE"] is True and s["NSE"] is not True]
    neither_ok = [d for d, s in date_status.items() if s["NSE"] is not True and s["BSE"] is not True]

    from_date_str = step_1_context["from_date"].strftime(DATE_INPUT_FORMAT)
    to_date_str = step_1_context["to_date"].strftime(DATE_INPUT_FORMAT)

    minutes, seconds = divmod(int(elapsed_seconds), 60)

    print(f"\nRange:                      {from_date_str} to {to_date_str}")
    print(f"Total trading dates targeted: {len(trading_date_list)}")
    print(f"  Fully successful (both):    {len(both_ok)}")
    print(f"  Partially successful:       {len(nse_only_ok) + len(bse_only_ok)}")
    if nse_only_ok:
        print(f"    NSE only ({len(nse_only_ok)}): " + ", ".join(d.strftime('%d-%b-%Y') for d in sorted(nse_only_ok)))
    if bse_only_ok:
        print(f"    BSE only ({len(bse_only_ok)}): " + ", ".join(d.strftime('%d-%b-%Y') for d in sorted(bse_only_ok)))
    print(f"  Fully failed (neither):     {len(neither_ok)}")
    if neither_ok:
        print("    Dates: " + ", ".join(d.strftime('%d-%b-%Y') for d in sorted(neither_ok)))

    print(f"\nRows persisted:")
    print(f"  NSE: {total_nse_rows}")
    print(f"  BSE: {total_bse_rows}")

    print(f"\nElapsed time: {minutes}m {seconds}s")

    if errors:
        print(f"\nErrors ({len(errors)}):")
        for err in errors:
            print(f"  [{err['date'].strftime('%d-%b-%Y')}] {err['exchange']}: {err['error']}")
    else:
        print("\nNo errors encountered.")

    print("\n" + "=" * 60)
    print("  RUN COMPLETE")
    print("=" * 60)


def run():
    """Standard entry point called by main.py (or directly, standalone)."""
    with start_run_logging("bhavcopy_loader"):
        run_start_time = time.time()
        step_0_context = step_0()
        step_1_context = step_1(step_0_context["env_values"], step_0_context["jwt_token"])
        step_2_context = step_2(step_0_context["env_values"], step_1_context["trading_date_list"])
        step_3_context = step_3(
            step_0_context["env_values"],
            step_1_context["trading_date_list"],
            step_2_context["date_bc_map"],
        )
        elapsed_seconds = time.time() - run_start_time
        step_4(step_1_context, step_3_context, elapsed_seconds)


if __name__ == "__main__":
    run()
