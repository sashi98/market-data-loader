# bhav_copy_missing_dates_backfill.py -- TMT-MDL-BUG-0012 (14-Nov-2026). Manual utility, NOT part of the scheduler.
#
# PURPOSE
#   Finds every calendar date in [--from (2024-01-01), --to (yesterday)] that has no row at all in bhav_copy for
#   an exchange, attempts the NSE / BSE bhav copy download for those dates only, and loads what is published
#   with STEP 2's own code (bhav_copy + its bhav_copy_metadata SUCCESS row). A 404 = the exchange was closed;
#   nothing is written for it.
#
# WHEN TO USE IT
#   - A special session on a weekend / holiday was missed (Diwali Muhurat, Budget-day Saturday, ...).
#     The scheduler's STEP 2 tries every calendar day after the last loaded day, so it only misses one if the
#     exchange published the file after the next trading day was already loaded.
#   - After a long scheduler outage, or to confirm there are no gaps (--dry-run lists the missing dates).
#   - First use: 14-Nov-2026, to load every special session since 2024-01-01.
#
# AFTER LOADING ANY OLD DATE
#   History changed: run the scheduler with every MDL_MODE_OF_RECOMPUTE_* = FULL. Its next cycle with a new
#   trade date rebuilds bhav_copy_adjusted, the W/M rollups and every indicator from 2024-01-01.
#
# HOW
#   Run between scheduler cycles (09:00-17:00 IST), from the market-data-loader folder:
#     python bhav_copy_missing_dates_backfill.py --dry-run      (list only)
#     python bhav_copy_missing_dates_backfill.py                (download + load; safe to re-run)
#   Options: --from YYYY-MM-DD, --to YYYY-MM-DD, --exchange NSE|BSE.
#   Report per exchange: dates loaded, closed on a true weekend/holiday, closed on a weekday NOT in the holiday
#   calendar (worth a look), download errors (re-run to retry).

import argparse
import sys
import time
from datetime import date, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from core import auth_client
from core.db_client import get_connection
from core.env_validator import load_and_validate_env, EnvValidationError
from core.holiday_client import get_holiday_dates_for_year, HolidayFetchError
from core.date_format import fmt_date
from runners.price_actions.bhav_copy_d_unadjusted_price_runner import _process_one_exchange

EXCHANGES = ["NSE", "BSE"]
PAUSE_SECONDS = 1.0   # between requests, on top of STEP 2's own pause after a real download


def _label(d):
    return f"{fmt_date(d)} ({d.strftime('%a')})"


def missing_dates(conn, exchange, start, end):
    with conn.cursor() as cur:
        cur.execute(
            "SELECT DISTINCT trade_date FROM bhav_copy WHERE exchange = %s AND trade_date BETWEEN %s AND %s",
            (exchange, start, end),
        )
        loaded = {row[0] for row in cur.fetchall()}
    days = [start + timedelta(days=i) for i in range((end - start).days + 1)]
    return [d for d in days if d not in loaded]


def main():
    parser = argparse.ArgumentParser(description="Load bhav copies missing from bhav_copy since 2024-01-01")
    parser.add_argument("--from", dest="start", default="2024-01-01")
    parser.add_argument("--to", dest="end", default=str(date.today() - timedelta(days=1)))
    parser.add_argument("--exchange", choices=EXCHANGES)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    start, end = date.fromisoformat(args.start), date.fromisoformat(args.end)
    exchanges = [args.exchange] if args.exchange else EXCHANGES

    try:
        env_values = load_and_validate_env()
    except EnvValidationError as e:
        print(f"[FAILED] {e}")
        sys.exit(1)
    token = auth_client.login(env_values["TMT_APP_BASE_URL"], env_values["TMT_ADMIN_USER_ID"],
                              env_values["TMT_ADMIN_PASSWORD"])
    holidays = {}
    for year in range(start.year, end.year + 1):
        try:
            holidays.update({d: True for d in get_holiday_dates_for_year(env_values["TMT_APP_BASE_URL"], token, year)})
        except HolidayFetchError as e:
            print(f"[WARN] holiday calendar for {year} not available ({e}) -- closed weekdays reported as unexplained.")

    def is_trading_day(d):
        return d.weekday() < 5 and d not in holidays

    download_dir = env_values["DATA_MARKET_DATA_LOADER_BHAV_COPY_DOWNLOAD_DIR"]
    print(f"BHAV COPY MISSING-DATES BACKFILL -- {fmt_date(start)} to {fmt_date(end)}"
          + (" (DRY RUN)" if args.dry_run else ""))

    for exchange in exchanges:
        conn = get_connection(env_values)
        try:
            missing = missing_dates(conn, exchange, start, end)
        finally:
            conn.close()
        weekdays_open = [d for d in missing if is_trading_day(d)]
        print(f"\n[{exchange}] {len(missing)} missing date(s): {len(missing) - len(weekdays_open)} weekend/holiday, "
              f"{len(weekdays_open)} trading day(s) per the calendar"
              + (f": {', '.join(_label(d) for d in weekdays_open)}" if weekdays_open else ""))
        if args.dry_run or not missing:
            continue

        loaded, closed_true, closed_unexplained, errors = [], 0, [], []
        started = time.time()
        for i, d in enumerate(missing, start=1):
            result = _process_one_exchange(env_values, exchange, d, download_dir)
            if result["outcome"] == "OK":
                kind = "special session on a weekend/holiday" if not is_trading_day(d) else "trading day"
                loaded.append((d, kind))
            elif result["outcome"] == "SKIPPED" and result["message"].startswith("not available"):
                if is_trading_day(d):
                    closed_unexplained.append(d)
                else:
                    closed_true += 1
            elif result["outcome"] == "FAILED":
                errors.append((d, result["message"]))
            time.sleep(PAUSE_SECONDS)
            if i % 50 == 0:
                print(f"  ... {i}/{len(missing)} done, {len(loaded)} loaded, {int(time.time() - started)} s")

        print(f"\n[{exchange}] SUMMARY")
        print(f"  Loaded ({len(loaded)}): " + (", ".join(f"{_label(d)} [{k}]" for d, k in loaded) or "none"))
        print(f"  Closed, true weekend/holiday: {closed_true}")
        print(f"  Closed on a weekday NOT in the holiday calendar ({len(closed_unexplained)}): "
              + (", ".join(_label(d) for d in closed_unexplained) or "none"))
        print(f"  Download/parse errors ({len(errors)}) -- re-run the script for these: "
              + ("; ".join(f"{_label(d)}: {m}" for d, m in errors) or "none"))


if __name__ == "__main__":
    main()
