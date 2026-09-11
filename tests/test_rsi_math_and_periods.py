# tests/test_rsi_math_and_periods.py
#
# Automated (no DB, no external files) coverage for core/rsi/rsi_math.py's
# Wilder seed/step/compute_rsi14 formula, and for core/rsi/rsi_calculator.py's
# DataFrame-level walk (compute_rsi14_for_isin/compute_rsi14_all) across
# daily/weekly/monthly-cadence input -- same "no DB, pure fixture" category
# as tests/test_rollup_calculator.py, added 2026-09-08 per Sashikant's
# request ("once its fixed we need to have junit for this") following the
# 2026-09-07 ALOKINDS monthly-RSI investigation (see this project's Claude
# Project doc claude/session-handover-2026-09-07.md).
#
# What that investigation found, and what this file locks in:
#   - core/rsi/rsi_math.py's step()/seed()/compute_rsi14() are shared,
#     verbatim, by rsi14d_runner.py, rsi14w_runner.py, and rsi14m_runner.py
#     -- there is no separate "monthly" formula to diverge from the daily
#     one. test_alokinds_july_to_august_2026_regression() below reproduces
#     the exact real-world case (ALOKINDS/NSE, July->August 2026, a genuine
#     ~29% crash Aug 27-31) that was hand-verified against the live DB
#     during that investigation, so a future change to the formula that
#     silently breaks this specific walk gets caught here instead of in
#     production again.
#   - The apparent "monthly RSI is wrong" report turned out to be a
#     period-timing mismatch, not a bug: a period's stored RSI always uses
#     that period's FINAL close (because rsi14w_workbook/rsi14m_workbook
#     only ever get a row once bhav_copy_w/bhav_copy_m's own rollup has
#     written a completed period -- see core/rollup/period.py's
#     pending_period_starts(), already covered by
#     test_rollup_calculator.py's test_pending_period_starts(), which
#     asserts a not-yet-ended period is never returned as due). This file's
#     test_alokinds_partial_month_close_differs_from_final() demonstrates,
#     concretely, WHY comparing against a live/mid-period reading (e.g. a
#     broker terminal's still-forming candle) will not match our
#     final-close-only value -- so a future "why doesn't this match
#     TradingView" report can be checked against this test's numbers
#     before assuming a regression.
#   - compute_rsi14_for_isin() (the DataFrame-grouped walk actually called
#     by rsi14w_runner.py/rsi14m_runner.py via compute_rsi14_all()) is
#     generic over row spacing -- it has no daily-frequency assumption
#     baked in. test_compute_rsi14_for_isin_monthly_cadence() and
#     test_compute_rsi14_for_isin_weekly_cadence() below assert its output
#     exactly matches an independently-built seed()/step() reference walk
#     over the same sparse (30-day / 7-day gapped) dates, so a future
#     change that accidentally introduces a daily-cadence assumption (e.g.
#     from calendar_index gap-detection, which weekly/monthly runners
#     deliberately pass as None -- see rsi14w_runner.py's own header) would
#     fail here immediately.
#
# Run from repo root:
#   python tests/test_rsi_math_and_periods.py

import sys
from datetime import date, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pandas as pd

from core.rsi.rsi_math import RSI_PERIOD, seed, step, compute_rsi14, compute_gain_loss
from core.rsi.rsi_calculator import compute_rsi14_for_isin, compute_rsi14_all

FAILURES = []


def check(label, condition):
    if condition:
        print(f"  [OK] {label}")
    else:
        print(f"  [FAILED] {label}")
        FAILURES.append(label)


def close_enough(a, b, tolerance=0.01):
    if a is None or b is None:
        return a is None and b is None
    return abs(a - b) <= tolerance


def row(security_id, isin, exchange, series, symbol, trade_date, close, prev_close):
    return {
        "security_id": security_id, "isin": isin, "exchange": exchange,
        "series": series, "symbol": symbol, "trade_date": trade_date,
        "close": close, "prev_close": prev_close,
    }


# ---------------------------------------------------------------------------
# core/rsi/rsi_math.py -- pure Wilder formula
# ---------------------------------------------------------------------------

def test_compute_gain_loss():
    print("\n" + "=" * 60)
    print("  compute_gain_loss()")
    print("=" * 60)
    check("price up -> (gain, 0)", compute_gain_loss(105, 100) == (5.0, 0.0))
    check("price down -> (0, loss)", compute_gain_loss(95, 100) == (0.0, 5.0))
    check("unchanged -> (0, 0)", compute_gain_loss(100, 100) == (0.0, 0.0))
    check("no prev_close -> (None, None)", compute_gain_loss(100, None) == (None, None))
    check("no close -> (None, None)", compute_gain_loss(None, 100) == (None, None))


def test_seed_basic():
    print("\n" + "=" * 60)
    print("  seed()")
    print("=" * 60)
    gains = [1, 0, 3, 0, 5, 0, 7, 0, 9, 0, 11, 0, 13, 0]
    losses = [0, 2, 0, 4, 0, 6, 0, 8, 0, 10, 0, 12, 0, 14]
    avg_gain, avg_loss = seed(gains, losses)
    check("avg_gain is the simple mean of the 14 gains", close_enough(avg_gain, sum(gains) / 14))
    check("avg_loss is the simple mean of the 14 losses", close_enough(avg_loss, sum(losses) / 14))

    try:
        seed(gains[:13], losses)
        check("seed() rejects a gains list shorter than RSI_PERIOD", False)
    except ValueError:
        check("seed() rejects a gains list shorter than RSI_PERIOD", True)


def test_step_basic():
    print("\n" + "=" * 60)
    print("  step()")
    print("=" * 60)
    avg_gain, avg_loss = step(prior_avg_gain=3.5, prior_avg_loss=4.0, gain=15, loss=0)
    check("avg_gain == (prior*13 + gain)/14", close_enough(avg_gain, (3.5 * 13 + 15) / 14))
    check("avg_loss == (prior*13 + loss)/14", close_enough(avg_loss, (4.0 * 13 + 0) / 14))


def test_compute_rsi14_edge_cases():
    print("\n" + "=" * 60)
    print("  compute_rsi14() -- edge cases")
    print("=" * 60)
    check("pre-seed (either input None) -> None", compute_rsi14(None, 1.0) is None and compute_rsi14(1.0, None) is None)
    check("zero net movement (avg_gain==avg_loss==0) -> None (matches pandas NaN)", compute_rsi14(0, 0) is None)
    check("avg_loss==0, avg_gain>0 -> saturates to exactly 100", compute_rsi14(5.0, 0) == 100.0)
    check("ordinary case: avg_gain==avg_loss -> rsi14==50.0", compute_rsi14(2.0, 2.0) == 50.0)


# ---------------------------------------------------------------------------
# 2026-09-07 ALOKINDS/NSE investigation -- concrete regression anchor
# ---------------------------------------------------------------------------
#
# Real, hand-verified-against-the-live-DB values from that investigation:
#   July 2026 row (bhav_copy_m):  close=11.92, avg_gain=0.3801, avg_loss=0.9968, rsi14=27.61
#   August 2026 row (bhav_copy_m): close=8.50,  avg_gain=0.3530, avg_loss=1.1699, rsi14=23.18
#   (July's close 11.92 matched July 31's real daily close; August's close
#   8.50 matched August 31's real daily close -- both confirmed directly
#   against bhav_copy_adjusted during the investigation, so this is a
#   genuine real-world case, not a synthetic one.)

JULY_2026_AVG_GAIN = 0.3801
JULY_2026_AVG_LOSS = 0.9968
JULY_2026_CLOSE = 11.92
AUGUST_2026_CLOSE = 8.50          # August's TRUE month-end close (Aug 31)
AUGUST_24_2026_CLOSE = 11.48      # ALOKINDS' close on Aug 24 -- BEFORE the Aug 27-31 crash


def test_alokinds_july_to_august_2026_regression():
    print("\n" + "=" * 60)
    print("  ALOKINDS/NSE regression -- July->August 2026 monthly RSI step")
    print("=" * 60)
    gain, loss = compute_gain_loss(AUGUST_2026_CLOSE, JULY_2026_CLOSE)
    check("August is a pure-loss month (close < prev_close)", gain == 0.0 and close_enough(loss, 3.42))

    avg_gain, avg_loss = step(JULY_2026_AVG_GAIN, JULY_2026_AVG_LOSS, gain, loss)
    check("August avg_gain matches the DB-verified value (0.3530)", close_enough(avg_gain, 0.3530, tolerance=0.001))
    check("August avg_loss matches the DB-verified value (1.1699)", close_enough(avg_loss, 1.1699, tolerance=0.001))

    rsi14 = compute_rsi14(avg_gain, avg_loss)
    check("August rsi14 matches the DB-stored value (23.18)", close_enough(rsi14, 23.18, tolerance=0.01))


def test_alokinds_partial_month_close_differs_from_final():
    print("\n" + "=" * 60)
    print("  ALOKINDS/NSE -- WHY a mid-month reading won't match our final value")
    print("=" * 60)
    # Same July starting point, but using Aug 24's close (before the
    # crash) as a stand-in for "the month's close" -- i.e. what a live
    # chart reading the still-forming August candle would have shown on
    # Aug 24 itself, three trading days before the crash even happened.
    gain, loss = compute_gain_loss(AUGUST_24_2026_CLOSE, JULY_2026_CLOSE)
    avg_gain, avg_loss = step(JULY_2026_AVG_GAIN, JULY_2026_AVG_LOSS, gain, loss)
    partial_rsi14 = compute_rsi14(avg_gain, avg_loss)

    final_avg_gain, final_avg_loss = step(
        JULY_2026_AVG_GAIN, JULY_2026_AVG_LOSS,
        *compute_gain_loss(AUGUST_2026_CLOSE, JULY_2026_CLOSE),
    )
    final_rsi14 = compute_rsi14(final_avg_gain, final_avg_loss)

    check("a mid-month (pre-crash) reading is meaningfully HIGHER than the final, post-crash value",
          partial_rsi14 > final_rsi14 + 2.0)
    check("the mid-month reading is in the same ballpark as an external Aug-24 reading (~27, not ~23 or ~50+)",
          25.0 <= partial_rsi14 <= 29.0)
    # This is the documented, expected behavior, not a bug: our system only
    # ever stores a period's RSI once that period is fully closed, using
    # its true final close (see pending_period_starts() in
    # core/rollup/period.py and its own test coverage in
    # tests/test_rollup_calculator.py).


# ---------------------------------------------------------------------------
# core/rsi/rsi_calculator.py -- DataFrame-level walk, sparse (weekly/
# monthly) cadence. Builds an independent seed()/step() reference walk
# over the SAME dates/closes and asserts compute_rsi14_for_isin() matches
# it exactly, regardless of how far apart the rows are.
# ---------------------------------------------------------------------------

def _reference_walk(closes):
    """
    closes: list of floats, in date order. Returns a list of (avg_gain,
    avg_loss, rsi14) tuples, one per row, computed directly via
    seed()/step()/compute_rsi14() -- an independent re-implementation of
    the walk compute_rsi14_for_isin() is expected to reproduce.
    """
    n = len(closes)
    gains, losses = [None], [None]  # row 0 has no prev_close
    for i in range(1, n):
        g, l = compute_gain_loss(closes[i], closes[i - 1])
        gains.append(g)
        losses.append(l)

    results = [(None, None, None)]  # row 0: pre-seed
    for i in range(1, n):
        if i < RSI_PERIOD:
            results.append((None, None, None))
            continue
        if i == RSI_PERIOD:
            avg_gain, avg_loss = seed(gains[1:RSI_PERIOD + 1], losses[1:RSI_PERIOD + 1])
        else:
            prior_avg_gain, prior_avg_loss, _ = results[i - 1]
            avg_gain, avg_loss = step(prior_avg_gain, prior_avg_loss, gains[i], losses[i])
        results.append((avg_gain, avg_loss, compute_rsi14(avg_gain, avg_loss)))
    return results


def _build_sparse_fixture(start, step_days, n_rows, closes):
    dates = [start + timedelta(days=step_days * i) for i in range(n_rows)]
    prev = [None] + closes[:-1]
    rows = [
        row("SEC1", "INE1", "NSE", "EQ", "FOO", d, c, p)
        for d, c, p in zip(dates, closes, prev)
    ]
    return pd.DataFrame(rows)


# 16 rows -- enough for a 14-row seed (rows 1..14) plus one step (row 15).
# Alternating up/down moves so gain and loss are both exercised every row.
_SPARSE_CLOSES = [100, 101, 99, 102, 98, 103, 97, 104, 96, 105, 95, 106, 94, 107, 93, 108]


def test_compute_rsi14_for_isin_monthly_cadence():
    print("\n" + "=" * 60)
    print("  compute_rsi14_for_isin() -- MONTHLY cadence (~30-day gaps)")
    print("=" * 60)
    df = _build_sparse_fixture(date(2025, 1, 1), 30, len(_SPARSE_CLOSES), _SPARSE_CLOSES)
    result = compute_rsi14_for_isin(df, calendar_index=None)
    reference = _reference_walk(_SPARSE_CLOSES)

    check("row count unchanged", len(result) == len(_SPARSE_CLOSES))
    all_match = True
    for i, (exp_ag, exp_al, exp_rsi) in enumerate(reference):
        got_ag = None if pd.isna(result.iloc[i]["avg_gain"]) else result.iloc[i]["avg_gain"]
        got_al = None if pd.isna(result.iloc[i]["avg_loss"]) else result.iloc[i]["avg_loss"]
        got_rsi = None if pd.isna(result.iloc[i]["rsi14"]) else result.iloc[i]["rsi14"]
        if not (close_enough(got_ag, exp_ag) and close_enough(got_al, exp_al) and close_enough(got_rsi, exp_rsi)):
            all_match = False
    check("every row's avg_gain/avg_loss/rsi14 matches the independent seed()/step() reference walk", all_match)
    check("seed happens at the 14th valid row regardless of 30-day spacing (row index 14 is seeded, not None)",
          not pd.isna(result.iloc[RSI_PERIOD]["rsi14"]))
    check("rows before the seed stay pre-seed (NaN rsi14)", pd.isna(result.iloc[RSI_PERIOD - 1]["rsi14"]))


def test_compute_rsi14_for_isin_weekly_cadence():
    print("\n" + "=" * 60)
    print("  compute_rsi14_for_isin() -- WEEKLY cadence (7-day gaps)")
    print("=" * 60)
    df = _build_sparse_fixture(date(2025, 1, 6), 7, len(_SPARSE_CLOSES), _SPARSE_CLOSES)
    result = compute_rsi14_for_isin(df, calendar_index=None)
    reference = _reference_walk(_SPARSE_CLOSES)

    all_match = True
    for i, (exp_ag, exp_al, exp_rsi) in enumerate(reference):
        got_rsi = None if pd.isna(result.iloc[i]["rsi14"]) else result.iloc[i]["rsi14"]
        if not close_enough(got_rsi, exp_rsi):
            all_match = False
    check("every row's rsi14 matches the independent reference walk at 7-day spacing too", all_match)


def test_compute_rsi14_all_groups_independently():
    print("\n" + "=" * 60)
    print("  compute_rsi14_all() -- multiple securities roll up independently (monthly)")
    print("=" * 60)
    df1 = _build_sparse_fixture(date(2025, 1, 1), 30, len(_SPARSE_CLOSES), _SPARSE_CLOSES)
    closes2 = [50 + c - 100 for c in _SPARSE_CLOSES]  # a second, differently-valued security
    df2 = _build_sparse_fixture(date(2025, 1, 1), 30, len(closes2), closes2)
    df2["security_id"] = "SEC2"
    df2["isin"] = "INE2"

    combined = pd.concat([df1, df2], ignore_index=True)
    result = compute_rsi14_all(combined, calendar_index=None)

    check("both securities present in the combined output", len(result) == len(_SPARSE_CLOSES) * 2)
    sec1_rsi = result[result["security_id"] == "SEC1"].reset_index(drop=True)
    sec2_rsi = result[result["security_id"] == "SEC2"].reset_index(drop=True)
    # Same shape of price moves (just a different base price) -> identical
    # gain/loss sequence -> identical rsi14 sequence. Confirms grouping
    # doesn't cross-contaminate the two securities' walks.
    both_seeded = not pd.isna(sec1_rsi.iloc[RSI_PERIOD]["rsi14"]) and not pd.isna(sec2_rsi.iloc[RSI_PERIOD]["rsi14"])
    check("both securities seed independently", both_seeded)
    if both_seeded:
        check("two securities with the same-shaped moves produce the same rsi14 sequence (no cross-contamination)",
              close_enough(sec1_rsi.iloc[RSI_PERIOD]["rsi14"], sec2_rsi.iloc[RSI_PERIOD]["rsi14"]))


def test_first_row_no_prev_close_stays_null():
    print("\n" + "=" * 60)
    print("  compute_rsi14_for_isin() -- a security's very first row (no prev_close) stays NULL")
    print("=" * 60)
    df = pd.DataFrame([
        row("SEC1", "INE1", "NSE", "EQ", "FOO", date(2026, 1, 1), 100, None),
        row("SEC1", "INE1", "NSE", "EQ", "FOO", date(2026, 2, 1), 105, 100),
    ])
    result = compute_rsi14_for_isin(df, calendar_index=None)
    check("first row's gain/loss/rsi14 are all NaN (no prev_close to diff against)",
          pd.isna(result.iloc[0]["gain"]) and pd.isna(result.iloc[0]["loss"]) and pd.isna(result.iloc[0]["rsi14"]))
    check("second row has a real gain (still pre-seed, only 1 valid row so far)",
          result.iloc[1]["gain"] == 5.0 and pd.isna(result.iloc[1]["rsi14"]))


if __name__ == "__main__":
    test_compute_gain_loss()
    test_seed_basic()
    test_step_basic()
    test_compute_rsi14_edge_cases()
    test_alokinds_july_to_august_2026_regression()
    test_alokinds_partial_month_close_differs_from_final()
    test_compute_rsi14_for_isin_monthly_cadence()
    test_compute_rsi14_for_isin_weekly_cadence()
    test_compute_rsi14_all_groups_independently()
    test_first_row_no_prev_close_stays_null()

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
