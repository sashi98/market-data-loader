# tests/test_rollup_calculator.py
#
# Automated (no DB, no external files) verification of
# core/rollup/period.py and core/rollup/rollup_calculator.py against
# hand-built fixture data -- Story A task 5.1
# (claude/user-story-weekly-monthly-ohlc-rsi-candlestick.md, this
# project's Claude Project docs). Unlike test_parser_manual.py, this
# genuinely runs standalone with no downloaded files or DB connection
# required -- the rollup math is pure pandas logic.
#
# Run from repo root:
#   python tests/test_rollup_calculator.py

import sys
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pandas as pd

from core.rollup.period import period_start, next_period_start, pending_period_starts, WEEKLY, MONTHLY
from core.rollup.rollup_calculator import compute_rollup

FAILURES = []


def check(label, condition):
    if condition:
        print(f"  [OK] {label}")
    else:
        print(f"  [FAILED] {label}")
        FAILURES.append(label)


def row(isin, exchange, series, symbol, trade_date, open_, high, low, close, last,
        tot_trd_qty, tot_trd_val, total_trades):
    return {
        "isin": isin, "exchange": exchange, "series": series, "symbol": symbol,
        "trade_date": trade_date, "open": open_, "high": high, "low": low,
        "close": close, "last": last, "tot_trd_qty": tot_trd_qty,
        "tot_trd_val": tot_trd_val, "total_trades": total_trades,
    }


def test_period_start_and_next():
    print("\n" + "=" * 60)
    print("  period_start() / next_period_start()")
    print("=" * 60)
    check("Monday itself -> same Monday", period_start(date(2026, 8, 17), WEEKLY) == date(2026, 8, 17))
    check("Friday -> that week's Monday", period_start(date(2026, 8, 21), WEEKLY) == date(2026, 8, 17))
    check("Sunday -> that week's Monday", period_start(date(2026, 8, 23), WEEKLY) == date(2026, 8, 17))
    check("Mid-month -> 1st of month", period_start(date(2026, 8, 15), MONTHLY) == date(2026, 8, 1))
    check("next_period_start WEEKLY", next_period_start(date(2026, 8, 17), WEEKLY) == date(2026, 8, 24))
    check("next_period_start MONTHLY", next_period_start(date(2026, 8, 1), MONTHLY) == date(2026, 9, 1))
    check("next_period_start MONTHLY year rollover", next_period_start(date(2026, 12, 1), MONTHLY) == date(2027, 1, 1))


def test_pending_period_starts():
    print("\n" + "=" * 60)
    print("  pending_period_starts() -- calendar-clock trigger (Story A decision 2)")
    print("=" * 60)
    # cursor = Mon 10-Aug-2026 (a prior period-start already rolled up).
    # today = Tue 25-Aug-2026.
    #   next period after cursor: 17-Aug, ends 24-Aug (<= today) -> due.
    #   the one after that: 24-Aug, ends 31-Aug (> today) -> NOT due yet.
    pending = pending_period_starts(date(2026, 8, 10), WEEKLY, date(2026, 8, 25))
    check("exactly one period due", pending == [date(2026, 8, 17)])

    check("None cursor -> no periods", pending_period_starts(None, WEEKLY, date(2026, 8, 25)) == [])

    # Catch-up case: listener was down for 3 whole weeks.
    catchup = pending_period_starts(date(2026, 7, 27), WEEKLY, date(2026, 8, 25))
    check("catch-up returns all 3 completed weeks in order",
          catchup == [date(2026, 8, 3), date(2026, 8, 10), date(2026, 8, 17)])


def test_weekly_rollup_monday_holiday():
    print("\n" + "=" * 60)
    print("  compute_rollup() -- WEEKLY, Monday is a holiday (Story A decision 1)")
    print("=" * 60)
    # Mon 17-Aug is a holiday -- first real trading day is Tue 18-Aug.
    # Last trading day (Fri 21-Aug) is a normal trading day.
    df = pd.DataFrame([
        row("INE1", "NSE", "EQ", "FOO", date(2026, 8, 18), 100, 110, 95, 105, 105, 1000, 100000, 50),
        row("INE1", "NSE", "EQ", "FOO", date(2026, 8, 21), 106, 120, 104, 118, 118, 2000, 200000, 60),
    ])
    result = compute_rollup(df, WEEKLY)
    check("exactly one rolled-up row", len(result) == 1)
    r = result.iloc[0]
    check("tradeDate is the calendar Monday, even though it had no row", r["trade_date"] == date(2026, 8, 17))
    check("open sourced from Tuesday (first real trading day)", r["open"] == 100)
    check("close sourced from Friday (last real trading day)", r["close"] == 118)
    check("high is MAX across the period", r["high"] == 120)
    check("low is MIN across the period", r["low"] == 95)
    check("tot_trd_qty is SUM across the period", r["tot_trd_qty"] == 3000)
    check("total_trades is SUM across the period", r["total_trades"] == 110)
    check("prevClose is NULL (no prior period in this input)", pd.isna(r["prev_close"]))


def test_weekly_rollup_friday_holiday():
    print("\n" + "=" * 60)
    print("  compute_rollup() -- WEEKLY, Friday is a holiday (Story A decision 1)")
    print("=" * 60)
    # Mon 24-Aug is a normal trading day. Fri 28-Aug is a holiday --
    # last real trading day is Thu 27-Aug. tradeDate is unaffected by
    # a Friday holiday either way -- it was never keyed off Friday.
    df = pd.DataFrame([
        row("INE1", "NSE", "EQ", "FOO", date(2026, 8, 24), 119, 125, 115, 122, 122, 1500, 150000, 55),
        row("INE1", "NSE", "EQ", "FOO", date(2026, 8, 27), 122, 130, 120, 128, 128, 1800, 180000, 58),
    ])
    result = compute_rollup(df, WEEKLY)
    r = result.iloc[0]
    check("tradeDate is the calendar Monday (unaffected by the Friday holiday)",
          r["trade_date"] == date(2026, 8, 24))
    check("open sourced from Monday (first real trading day)", r["open"] == 119)
    check("close sourced from Thursday (last real trading day, Friday was a holiday)", r["close"] == 128)


def test_weekly_rollup_prevclose_chaining():
    print("\n" + "=" * 60)
    print("  compute_rollup() -- prevClose chains across consecutive periods in one call")
    print("=" * 60)
    df = pd.DataFrame([
        row("INE1", "NSE", "EQ", "FOO", date(2026, 8, 17), 100, 110, 95, 105, 105, 1000, 100000, 50),
        row("INE1", "NSE", "EQ", "FOO", date(2026, 8, 21), 106, 120, 104, 118, 118, 2000, 200000, 60),
        row("INE1", "NSE", "EQ", "FOO", date(2026, 8, 24), 119, 125, 115, 122, 122, 1500, 150000, 55),
    ])
    result = compute_rollup(df, WEEKLY)
    check("two distinct weekly rows", len(result) == 2)
    week1, week2 = result.iloc[0], result.iloc[1]
    check("week1 prevClose is NULL (isin's first period, no prior at all)", pd.isna(week1["prev_close"]))
    check("week2 prevClose == week1's close (prior PERIOD's close, not week2's own first day)",
          week2["prev_close"] == 118)
    check("week2 ltp_percent_change matches close vs prevClose",
          week2["ltp_percent_change"] == round((122 - 118) / 118 * 100, 2))


def test_prior_close_lookup_for_incremental_single_period():
    print("\n" + "=" * 60)
    print("  compute_rollup() -- prior_close_lookup (Part 2 incremental listener's use case)")
    print("=" * 60)
    # Only ONE period's worth of daily rows in this call's input -- the
    # prior period's close must come from prior_close_lookup, exactly
    # like bhav_copy_wm_rollup_listener.py supplies it (sourced from
    # rollup_persistence.fetch_prior_close_lookup() against the table
    # itself).
    df = pd.DataFrame([
        row("INE1", "NSE", "EQ", "FOO", date(2026, 8, 24), 119, 125, 115, 122, 122, 1500, 150000, 55),
    ])
    lookup = {("INE1", "NSE", date(2026, 8, 24)): 118}
    result = compute_rollup(df, WEEKLY, prior_close_lookup=lookup)
    check("prevClose resolved from the lookup", result.iloc[0]["prev_close"] == 118)

    result_no_lookup = compute_rollup(df, WEEKLY)
    check("no lookup supplied -> prevClose stays NULL", pd.isna(result_no_lookup.iloc[0]["prev_close"]))


def test_monthly_rollup():
    print("\n" + "=" * 60)
    print("  compute_rollup() -- MONTHLY")
    print("=" * 60)
    df = pd.DataFrame([
        row("INE1", "NSE", "EQ", "FOO", date(2026, 8, 3), 100, 110, 95, 105, 105, 1000, 100000, 50),
        row("INE1", "NSE", "EQ", "FOO", date(2026, 8, 31), 106, 120, 104, 118, 118, 2000, 200000, 60),
    ])
    result = compute_rollup(df, MONTHLY)
    r = result.iloc[0]
    check("tradeDate is the 1st of the month", r["trade_date"] == date(2026, 8, 1))
    check("open from earliest trading day in the month", r["open"] == 100)
    check("close from latest trading day in the month", r["close"] == 118)


def test_multiple_isins_independent():
    print("\n" + "=" * 60)
    print("  compute_rollup() -- multiple isins/exchanges roll up independently")
    print("=" * 60)
    df = pd.DataFrame([
        row("INE1", "NSE", "EQ", "FOO", date(2026, 8, 17), 100, 110, 95, 105, 105, 1000, 100000, 50),
        row("INE2", "NSE", "EQ", "BAR", date(2026, 8, 18), 50, 55, 48, 52, 52, 500, 25000, 20),
        row("INE1", "BSE", "EQ", "FOO", date(2026, 8, 17), 101, 111, 96, 106, 106, 900, 90000, 45),
    ])
    result = compute_rollup(df, WEEKLY)
    check("3 independent (isin, exchange) rollup rows", len(result) == 3)
    check("INE1/NSE and INE1/BSE are separate rows (not merged)",
          len(result[(result["isin"] == "INE1")]) == 2)


def test_empty_input():
    print("\n" + "=" * 60)
    print("  compute_rollup() -- empty input")
    print("=" * 60)
    empty = pd.DataFrame(columns=[
        "isin", "exchange", "series", "symbol", "trade_date",
        "open", "high", "low", "close", "last", "tot_trd_qty", "tot_trd_val", "total_trades",
    ])
    result = compute_rollup(empty, WEEKLY)
    check("empty input -> empty, correctly-shaped output", result.empty)


if __name__ == "__main__":
    test_period_start_and_next()
    test_pending_period_starts()
    test_weekly_rollup_monday_holiday()
    test_weekly_rollup_friday_holiday()
    test_weekly_rollup_prevclose_chaining()
    test_prior_close_lookup_for_incremental_single_period()
    test_monthly_rollup()
    test_multiple_isins_independent()
    test_empty_input()

    print("\n" + "=" * 60)
    if FAILURES:
        print(f"  {len(FAILURES)} CHECK(S) FAILED:")
        for f in FAILURES:
            print(f"    - {f}")
        print("=" * 60)
        sys.exit(1)
    else:
        print("  ALL CHECKS PASSED")
        print("=" * 60)
