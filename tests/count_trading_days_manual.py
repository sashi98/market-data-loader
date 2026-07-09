# tests/count_trading_days_manual.py
#
# One-off diagnostic -- NOT a registered loader, not wired into main.py,
# not an automated test. Answers: "how many trading days, walking
# backward from to_date, does it take to reach (or pass) target_date?" --
# using the REAL holiday-aware logic in core/trading_calendar.py (same
# function bhavcopy_loader.py itself uses), not an estimate.
#
# Usage, from repo root:
#   python tests/count_trading_days_manual.py

import sys
from datetime import date, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from core.env_validator import load_and_validate_env, EnvValidationError
from core.health_client import check_health, HealthCheckError
from core.auth_client import authenticate, AuthError
from core.holiday_client import get_holiday_dates_for_year, HolidayFetchError
from core.trading_calendar import compute_default_to_date, SATURDAY, SUNDAY

TARGET_DATE = date(2024, 1, 1)  # change this if you want a different boundary


def main():
    print("Validating config/.env ...")
    try:
        env_values = load_and_validate_env()
    except EnvValidationError as e:
        print(f"[FAILED] {e}")
        sys.exit(1)

    tmt_app_base_url = env_values["TMT_APP_BASE_URL"]

    print("Checking health ...")
    try:
        check_health(tmt_app_base_url)
    except HealthCheckError as e:
        print(f"[FAILED] {e}")
        sys.exit(1)

    print("Authenticating ...")
    try:
        jwt_token = authenticate(tmt_app_base_url)
    except AuthError as e:
        print(f"[FAILED] {e}")
        sys.exit(1)

    to_date = compute_default_to_date()
    print(f"\nWalking backward from to_date={to_date} until reaching target_date={TARGET_DATE} ...\n")

    current = to_date
    trading_days = 0
    weekend_count = 0
    holiday_count = 0
    holiday_cache_by_year = {}

    while current >= TARGET_DATE:
        year = current.year
        if year not in holiday_cache_by_year:
            try:
                holiday_cache_by_year[year] = get_holiday_dates_for_year(
                    tmt_app_base_url, jwt_token, year
                )
            except HolidayFetchError as e:
                print(f"[FAILED] Could not fetch holidays for {year}: {e}")
                sys.exit(1)

        is_weekend = current.weekday() in (SATURDAY, SUNDAY)
        is_holiday = current in holiday_cache_by_year[year]

        if is_weekend:
            weekend_count += 1
        elif is_holiday:
            holiday_count += 1
        else:
            trading_days += 1

        current -= timedelta(days=1)

    calendar_days = (to_date - TARGET_DATE).days + 1

    print("=" * 50)
    print(f"  to_date:              {to_date}")
    print(f"  target_date:          {TARGET_DATE}")
    print(f"  Total calendar days:  {calendar_days}")
    print(f"  Weekends excluded:    {weekend_count}")
    print(f"  Holidays excluded:    {holiday_count}")
    print(f"  TRADING DAYS:         {trading_days}")
    print("=" * 50)
    print(f"\n-> bhavcopy_loader.py now takes from_date/to_date directly (not a day")
    print(f"   count) -- just type from_date={TARGET_DATE.strftime('%d%m%Y')} at its prompt.")
    print(f"   This script's TRADING DAYS figure above is informational only, so you")
    print(f"   know roughly how much work that range represents before running it.")


if __name__ == "__main__":
    main()
