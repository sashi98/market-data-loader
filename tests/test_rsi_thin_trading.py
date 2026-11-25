# tests/test_rsi_thin_trading.py -- TMT-MDL-BUG-0016
#
# A thinly traded stock (INDIFRA, NSE SME: a trade every few sessions) must get RSI 14 on every traded day after its
# 14th trade, and RSI-SMA 14 after 14 RSI values -- like TradingView and like TMT's weekly / monthly RSI. Before the fix
# every gap of 5+ missed sessions restarted the Wilder average, so most of its days had no RSI and RSI-SMA never filled.
# FULL and INCREMENTAL must still agree. No DB. Run: python tests/test_rsi_thin_trading.py (or pytest).

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pandas as pd  # noqa: E402

from core.recompute import shadow  # noqa: E402
from core.rsi.rsi_calculator import compute_rsi14_all  # noqa: E402
from core.rsi.rsi_continuity import build_calendar_index, is_gap, GAP_THRESHOLD_SESSIONS  # noqa: E402
from core.rsi.rsi_recompute import add_rsi_sma, walk_from_anchor, OUTPUT_COLUMNS  # noqa: E402

CALENDAR = list(pd.bdate_range("2025-01-01", "2026-11-23").date)      # every weekday = a real session
CAL = build_calendar_index(CALENDAR)


def _thin_stock():
    """A trade on every 6th session with a few 9-12 session gaps (INDIFRA-like), closes wobbling up and down."""
    days, i, n = [], 0, 0
    while i < len(CALENDAR):
        days.append(CALENDAR[i])
        i += 12 if n % 7 == 6 else 6
        n += 1
    closes = [100 + ((k * 7) % 11) - 5 + k * 0.1 for k in range(len(days))]
    return pd.DataFrame({"security_id": "INE0PS301014", "isin": "INE0PS301014", "exchange": "NSE", "series": "SM",
                         "symbol": "INDIFRA", "trade_date": days, "close": closes,
                         "prev_close": [None] + closes[:-1]})


def test_gaps_between_trades_do_not_reseed():
    assert GAP_THRESHOLD_SESSIONS is None
    df = _thin_stock()
    assert is_gap(CAL, df["trade_date"].iloc[0], df["trade_date"].iloc[1]) is False        # 5 missed sessions
    assert is_gap(CAL, df["trade_date"].iloc[0], df["trade_date"].iloc[1], threshold=5) is True   # opt-in still works


def test_every_traded_day_after_the_14th_trade_has_rsi_and_sma():
    df = _thin_stock()
    full = add_rsi_sma(compute_rsi14_all(df, calendar_index=CAL), 14)
    rsi = pd.to_numeric(full["rsi14"])
    # row 0 has no prev_close; Wilder seeds on the 14th change (row 14); every row from there on has RSI
    assert rsi.iloc[:14].isna().all()
    assert rsi.iloc[14:].notna().all(), f"{rsi.iloc[14:].isna().sum()} traded days without RSI"
    sma = pd.to_numeric(full["rsi_sma14"])
    assert sma.iloc[:27].isna().all() and sma.iloc[27:].notna().all()
    # the calendar no longer changes anything: same walk as with no calendar at all (TradingView's own-bars rule)
    plain = compute_rsi14_all(df, calendar_index=None)
    r = shadow.compare(compute_rsi14_all(df, calendar_index=CAL), plain, ["security_id", "trade_date"],
                       ["avg_gain", "avg_loss", "rsi14"], {"avg_gain": 1e-12, "avg_loss": 1e-12, "rsi14": 1e-9})
    assert r["mismatches"] == 0, r


def test_incremental_walks_across_the_gaps_like_full():
    df = _thin_stock()
    full = compute_rsi14_all(df, calendar_index=CAL).reset_index(drop=True)
    k = 40                                          # anchor = 40th trade; the new rows all sit after 5-11 session gaps
    new = df.iloc[k + 1:].copy()
    new["anchor_date"] = full.at[k, "trade_date"]
    new["anchor_avg_gain"] = round(full.at[k, "avg_gain"], 10)
    new["anchor_avg_loss"] = round(full.at[k, "avg_loss"], 10)
    rows = walk_from_anchor(new, CAL)
    assert rows is not None
    walked = pd.DataFrame(rows, columns=OUTPUT_COLUMNS)
    r = shadow.compare(walked, full, ["security_id", "trade_date"], ["gain", "loss", "avg_gain", "avg_loss", "rsi14"],
                       {"gain": 1e-9, "loss": 1e-9, "avg_gain": 1e-8, "avg_loss": 1e-8, "rsi14": 0.0101})
    assert r["rows_compared"] == len(df) - k - 1 and r["mismatches"] == 0, r


if __name__ == "__main__":
    for name, fn in list(globals().items()):
        if name.startswith("test_"):
            fn()
            print("PASS", name)
