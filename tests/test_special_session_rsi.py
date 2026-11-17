# tests/test_special_session_rsi.py -- TMT-MDL-BUG-0012 regression: the missing Diwali Muhurat bar (Sun 08-Nov-2026).
# Run: python3 tests/test_special_session_rsi.py   (or pytest)
#
# ADVANCE (NSE, INE1B0W01010), the last 100 bhav_copy_adjusted closes up to 13-Nov-2026 (14-Nov-2026 query), plus
# the Muhurat session TradingView shows on Sun 08-Nov-2026 (O 120.00 H 121.70 L 119.81 C 121.32).
# prev_close is built the way STEP 4 builds it (core/price_series/adjusted_series.py: the previous row's close), so
# once 08-Nov is loaded, 09-Nov's prev_close becomes 121.32 automatically.
#
# Without the bar MDL gave RSI 45.36 / RSI-SMA 46.86 (full history); TradingView shows 45.56 / 46.39.
# With only these 100 rows the seed differs slightly from the full history, hence the 0.02 tolerance on the
# "without" values; the "with" values must match TradingView within 0.02.
import sys
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pandas as pd

from core.rsi.rsi_calculator import compute_rsi14_all
from core.rsi.rsi_recompute import add_rsi_sma

ADVANCE_CLOSES = [
    ("2026-06-22", 109.82),
    ("2026-06-23", 106.86),
    ("2026-06-24", 110.74),
    ("2026-06-25", 110.13),
    ("2026-06-29", 108.96),
    ("2026-06-30", 107.60),
    ("2026-07-01", 106.83),
    ("2026-07-02", 114.81),
    ("2026-07-03", 113.80),
    ("2026-07-06", 115.10),
    ("2026-07-07", 112.24),
    ("2026-07-08", 108.56),
    ("2026-07-09", 107.04),
    ("2026-07-10", 110.49),
    ("2026-07-13", 113.67),
    ("2026-07-14", 110.15),
    ("2026-07-15", 112.60),
    ("2026-07-16", 115.67),
    ("2026-07-17", 113.66),
    ("2026-07-20", 113.10),
    ("2026-07-21", 111.21),
    ("2026-07-22", 111.77),
    ("2026-07-23", 110.20),
    ("2026-07-24", 109.89),
    ("2026-07-27", 113.60),
    ("2026-07-28", 118.17),
    ("2026-07-29", 118.20),
    ("2026-07-30", 119.40),
    ("2026-07-31", 125.01),
    ("2026-08-03", 124.56),
    ("2026-08-04", 125.11),
    ("2026-08-05", 134.10),
    ("2026-08-06", 129.78),
    ("2026-08-07", 127.18),
    ("2026-08-10", 121.21),
    ("2026-08-11", 122.21),
    ("2026-08-12", 118.01),
    ("2026-08-13", 119.20),
    ("2026-08-14", 118.79),
    ("2026-08-17", 117.06),
    ("2026-08-18", 117.96),
    ("2026-08-19", 116.62),
    ("2026-08-20", 115.93),
    ("2026-08-21", 119.20),
    ("2026-08-24", 118.00),
    ("2026-08-25", 119.19),
    ("2026-08-26", 122.58),
    ("2026-08-27", 120.32),
    ("2026-08-28", 116.95),
    ("2026-08-31", 116.42),
    ("2026-09-01", 116.22),
    ("2026-09-02", 117.89),
    ("2026-09-03", 118.53),
    ("2026-09-04", 117.43),
    ("2026-09-07", 116.43),
    ("2026-09-08", 115.53),
    ("2026-09-09", 113.85),
    ("2026-09-10", 119.03),
    ("2026-09-11", 117.85),
    ("2026-09-15", 113.95),
    ("2026-09-16", 110.15),
    ("2026-09-17", 111.67),
    ("2026-09-18", 110.48),
    ("2026-09-21", 107.83),
    ("2026-09-22", 108.14),
    ("2026-09-23", 119.37),
    ("2026-09-24", 118.67),
    ("2026-09-25", 123.12),
    ("2026-09-28", 121.50),
    ("2026-09-29", 120.19),
    ("2026-09-30", 121.19),
    ("2026-10-01", 116.75),
    ("2026-10-05", 115.85),
    ("2026-10-06", 117.76),
    ("2026-10-07", 117.49),
    ("2026-10-08", 113.85),
    ("2026-10-09", 113.27),
    ("2026-10-12", 126.52),
    ("2026-10-13", 143.97),
    ("2026-10-14", 155.67),
    ("2026-10-15", 158.43),
    ("2026-10-16", 158.08),
    ("2026-10-19", 156.82),
    ("2026-10-21", 155.28),
    ("2026-10-22", 144.06),
    ("2026-10-23", 136.43),
    ("2026-10-26", 131.51),
    ("2026-10-27", 135.05),
    ("2026-10-28", 134.03),
    ("2026-10-29", 132.12),
    ("2026-10-30", 123.97),
    ("2026-11-02", 129.08),
    ("2026-11-03", 128.64),
    ("2026-11-04", 131.28),
    ("2026-11-05", 125.25),
    ("2026-11-06", 119.81),
    ("2026-11-09", 121.75),
    ("2026-11-11", 125.75),
    ("2026-11-12", 122.91),
    ("2026-11-13", 123.89),
]
MUHURAT = ("2026-11-08", 121.32)
TV_RSI, TV_RSI_SMA = 45.56, 46.39


def _frame(rows):
    df = pd.DataFrame(sorted(rows), columns=["trade_date", "close"])
    df["trade_date"] = df["trade_date"].map(date.fromisoformat)
    df["prev_close"] = df["close"].shift(1)          # STEP 4: previous row's adjusted close
    df = df.iloc[1:].reset_index(drop=True)          # the first row has no prev_close
    df["security_id"] = "INE1B0W01010"
    df["isin"] = "INE1B0W01010"
    df["exchange"] = "NSE"
    df["series"] = "EQ"
    df["symbol"] = "ADVANCE"
    return df


def _last_rsi_and_sma(rows):
    rsi = add_rsi_sma(compute_rsi14_all(_frame(rows)), 14).sort_values("trade_date")
    last = rsi.iloc[-1]
    assert last["trade_date"] == date(2026, 11, 13), last["trade_date"]
    return float(last["rsi14"]), float(last["rsi_sma14"])


def test_without_muhurat_bar_reproduces_the_gap():
    rsi, sma = _last_rsi_and_sma(ADVANCE_CLOSES)
    assert abs(rsi - 45.35) < 0.02 and abs(sma - 46.85) < 0.02, (rsi, sma)
    assert abs(sma - TV_RSI_SMA) > 0.4, (sma, "the missing bar should explain a ~0.47 RSI-SMA gap")


def test_with_muhurat_bar_matches_tradingview():
    rsi, sma = _last_rsi_and_sma(ADVANCE_CLOSES + [MUHURAT])
    assert abs(rsi - TV_RSI) < 0.02 and abs(sma - TV_RSI_SMA) < 0.02, (rsi, sma)


def test_monday_prev_close_becomes_the_muhurat_close():
    df = _frame(ADVANCE_CLOSES + [MUHURAT]).set_index("trade_date")
    assert df.loc[date(2026, 11, 8), "prev_close"] == 119.81      # Fri 06-Nov close
    assert df.loc[date(2026, 11, 9), "prev_close"] == 121.32      # Sun 08-Nov Muhurat close, not 119.81


if __name__ == "__main__":
    for name, fn in list(globals().items()):
        if name.startswith("test_"):
            fn()
            print("PASS", name)
