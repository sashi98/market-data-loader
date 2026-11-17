# tests/test_step2_calendar_days.py -- TMT-MDL-BUG-0012: STEP 2 tries every CALENDAR day of its window, not only the
# trading days (no DB, no network: the per-date worker, the calendar and the audit write are stubbed).
# Run: python3 tests/test_step2_calendar_days.py   (or pytest)
#
# Rules checked:
#   - weekend / holiday with a published file  -> loaded (special session), counts as new data
#   - weekend / holiday 404                    -> closed, skipped, the walk goes on
#   - weekend / holiday download error (403)   -> treated as closed; never blocks the trading days after it
#   - trading day 404                          -> skipped (not published yet; the scheduler's wait logic handles it)
#   - trading day download / parse error       -> FAILED, the walk stops there (unchanged behaviour)
import sys
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from runners.price_actions import bhav_copy_d_unadjusted_price_runner as step2

FRI, SAT, SUN, MON, TUE, WED = (date(2026, 11, d) for d in (6, 7, 8, 9, 10, 11))
HOLIDAYS = {TUE}   # Balipratipada


def _run(outcomes, start=FRI, end=WED):
    """outcomes: date -> "OK" | "404" | "ERR". Returns (result, dates the worker was asked for)."""
    asked = []

    def calendar(start_date, end_date, base_url, token):
        days = step2.calendar_days(start_date, end_date)
        return {"trading_date_list": [d for d in days if d.weekday() < 5 and d not in HOLIDAYS]}

    def worker(env_values, exchange, trade_date, download_dir):
        asked.append(trade_date)
        o = outcomes.get(trade_date, "OK")
        if o == "OK":
            return {"outcome": "OK", "message": "100 rows persisted", "processing_time_ms": 1}
        if o == "404":
            return {"outcome": "SKIPPED", "message": "not available: 404", "processing_time_ms": 1}
        return {"outcome": "FAILED", "message": "download error: HTTP 403", "processing_time_ms": 1}

    saved = (step2.compute_trading_date_range_between, step2._process_one_exchange, step2._write_daily_audit)
    step2.compute_trading_date_range_between = calendar
    step2._process_one_exchange = worker
    step2._write_daily_audit = lambda *a, **k: None
    try:
        result = step2.run({}, "http://tmt", "token", "/tmp", "NSE", start, end, end)
    finally:
        step2.compute_trading_date_range_between, step2._process_one_exchange, step2._write_daily_audit = saved
    return result, asked


def test_calendar_days():
    assert step2.calendar_days(FRI, MON) == [FRI, SAT, SUN, MON]
    assert step2.calendar_days(MON, MON) == [MON]


def test_every_calendar_day_is_tried_in_order():
    _, asked = _run({SAT: "404", SUN: "404", TUE: "404"})
    assert asked == [FRI, SAT, SUN, MON, TUE, WED], asked


def test_muhurat_sunday_is_loaded():
    result, _ = _run({SAT: "404", SUN: "OK", TUE: "404"})
    assert result["outcome"] == "DONE"
    assert SUN in result["daily"]["dates_ok"] and result["any_new_data"]


def test_closed_weekend_and_holiday_do_not_stop_the_walk():
    result, asked = _run({SAT: "404", SUN: "404", TUE: "404"})
    assert result["outcome"] == "DONE" and asked[-1] == WED
    assert [d for d in result["daily"]["dates_ok"] if d in (MON, WED)] == [MON, WED]


def test_weekend_download_error_is_treated_as_closed():
    # e.g. BSE answering 403 for a Sunday: must not hold back Monday onwards
    result, asked = _run({SAT: "ERR", SUN: "ERR", TUE: "404"})
    assert result["outcome"] == "DONE", result
    assert asked == [FRI, SAT, SUN, MON, TUE, WED]


def test_trading_day_error_still_stops_the_walk():
    result, asked = _run({SAT: "404", SUN: "404", MON: "ERR"})
    assert result["outcome"] == "FAILED"
    assert asked == [FRI, SAT, SUN, MON], asked           # stopped at Monday, Tuesday / Wednesday not tried
    assert MON not in result["daily"]["dates_ok"]


def test_monday_window_after_a_friday_load():
    # the scheduler's window is [day after the last loaded day, latest trade date] -> Sat..Mon on a Monday
    result, asked = _run({SAT: "404", SUN: "OK"}, start=SAT, end=MON)
    assert asked == [SAT, SUN, MON] and result["daily"]["dates_ok"] == [SAT, SUN, MON]


if __name__ == "__main__":
    for name, fn in list(globals().items()):
        if name.startswith("test_"):
            fn()
            print("PASS", name)
