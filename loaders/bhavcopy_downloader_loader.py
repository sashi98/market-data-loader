# loaders/bhavcopy_downloader_loader.py
#
# market-data-loader -- BhavCopy download-only loader.
#
# Same Step 0 (minus the DB check, since this loader never touches the
# DB) + Step 1 (trading date list, via explicit from_date/to_date) +
# Step 2 (download) as bhav_copy_with_corporate_action_loader, but stops there -- no parsing,
# no persistence, no summary report.
#
# Useful for:
#   - Quickly checking whether NSE/BSE still publish files in the
#     expected format/URL for an old date, before committing to a large
#     backfill (e.g. testing the far end of a 3-year lookback with a
#     single-day from_date/to_date range before running the full range
#     through bhav_copy_with_corporate_action_loader)
#   - Just wanting the raw CSV/ZIP files on disk without a DB dependency
#
# Exposes run() -- the standard entry point every loader under loaders/
# provides, so main.py can invoke any loader generically.
#
# Can also be run standalone for development/testing:
#   python bhavcopy_downloader_loader.py

import sys
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
from core.logging_setup import start_run_logging

# Reuse the same bhav copy download logic from
# bhav_copy_with_corporate_action_loader -- imported directly rather
# than duplicated, so both loaders always stay in sync.
from loaders.bhav_copy_with_corporate_action_loader import download_bhavcopy_files

DATE_INPUT_FORMAT = "%d%m%Y"  # DDMMYYYY


def step_0_download_only():
    """
    Same as bhav_copy_with_corporate_action_loader.step_0(), minus the DB connectivity check --
    this loader never writes to the DB, so requiring a DB connection
    would be an unnecessary blocker.
    """
    print("=" * 60)
    print("  STEP 0 -- Environment validation, health check, auth")
    print("  (DB connectivity check skipped -- download-only loader)")
    print("=" * 60)

    print("\n[0.1] Validating config/.env ...")
    try:
        env_values = load_and_validate_env()
    except EnvValidationError as e:
        print(f"  [FAILED] {e}")
        sys.exit(1)
    print("  [OK] All required keys present.")

    tmt_app_base_url = env_values["TMT_APP_BASE_URL"]

    print(f"\n[0.2] Checking TMT + stock-py-services health at {tmt_app_base_url}/actuator/health ...")
    try:
        health_body = check_health(tmt_app_base_url)
    except HealthCheckError as e:
        print(f"  [FAILED] {e}")
        sys.exit(1)
    print(f"  [OK] Overall status: {health_body.get('status')}")

    print("\n[0.3] Authenticating ...")
    try:
        jwt_token = authenticate(tmt_app_base_url)
    except AuthError as e:
        print(f"  [FAILED] {e}")
        sys.exit(1)

    print("\n" + "=" * 60)
    print("  STEP 0 COMPLETE")
    print("=" * 60)

    return {"env_values": env_values, "jwt_token": jwt_token}


def step_1_dates_only(env_values, jwt_token):
    print("\n" + "=" * 60)
    print("  STEP 1 -- Build trading date list")
    print("=" * 60)

    tmt_app_base_url = env_values["TMT_APP_BASE_URL"]

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

    from_date_str_input = input("Input from_date(DDMMYYYY): ").strip()
    try:
        from_date = datetime.strptime(from_date_str_input, DATE_INPUT_FORMAT).date()
    except ValueError:
        print(f"  [FAILED] Invalid date '{from_date_str_input}' -- expected DDMMYYYY (e.g. 01012024).")
        sys.exit(1)

    if from_date > to_date:
        print(f"  [FAILED] from_date ({from_date_str_input}) cannot be after to_date ({to_date_str}).")
        sys.exit(1)

    try:
        result = compute_trading_date_range_between(from_date, to_date, tmt_app_base_url, jwt_token)
    except TradingCalendarError as e:
        print(f"  [FAILED] {e}")
        sys.exit(1)

    trading_date_list = result["trading_date_list"]
    weekend_count = result["weekend_count"]
    holiday_count = result["holiday_count"]
    num_days = len(trading_date_list)
    total_calendar_days = num_days + weekend_count + holiday_count

    from_date_confirm_str = from_date.strftime(DATE_INPUT_FORMAT)
    to_date_confirm_str = to_date.strftime(DATE_INPUT_FORMAT)

    print(f"\nRange {from_date_confirm_str} to {to_date_confirm_str} contains {num_days} trading days, "
          f"after excluding weekends(Saturday,Sundays) + Holidays")
    print(f"  Weekends excluded: {weekend_count}")
    print(f"  Holidays excluded: {holiday_count}")
    print(f"  Total calendar days: {total_calendar_days} "
          f"({num_days} trading + {weekend_count} weekends + {holiday_count} holidays)")
    print(f"BhavCopy will be downloaded from {from_date_confirm_str} to {to_date_confirm_str}")

    confirm = input("Continue Y/N: ").strip().upper()
    if confirm != "Y":
        print("Aborted by user.")
        sys.exit(0)

    print("\n" + "=" * 60)
    print("  STEP 1 COMPLETE")
    print("=" * 60)

    return {"trading_date_list": trading_date_list}


def run():
    """Standard entry point called by main.py (or directly, standalone)."""
    with start_run_logging("bhavcopy_downloader_loader"):
        step_0_context = step_0_download_only()
        step_1_context = step_1_dates_only(step_0_context["env_values"], step_0_context["jwt_token"])
        download_bhavcopy_files(step_0_context["env_values"], step_1_context["trading_date_list"])
        print("\n(Download-only loader -- no parse/persist/summary. Files are on disk under MARKET_DATA_LOADER_DOWNLOAD_DIR.)")


if __name__ == "__main__":
    run()
