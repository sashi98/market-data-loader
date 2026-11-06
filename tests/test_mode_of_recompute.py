# tests/test_mode_of_recompute.py -- TMT-MDL-US-0005 pure-logic checks (no DB).
# Run: python3 tests/test_mode_of_recompute.py   (or pytest)
import sys
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pandas as pd

from core.recompute import mode as m
from core.recompute import shadow
from core.price_series import classification as c
from core.rollup.period import WEEKLY, period_start
from core.rollup.rollup_calculator import compute_rollup, _percent_change


def test_parse_mode():
    assert m.parse_mode(" shadow ") == "SHADOW"
    for bad in (None, "", "FAST"):
        try:
            m.parse_mode(bad)
            assert False, bad
        except m.ModeOfRecomputeError:
            pass
    try:
        m.validate_all({})
        assert False
    except m.ModeOfRecomputeError as e:
        assert "MDL_MODE_OF_RECOMPUTE_WEEKLY_ROLLUP" in str(e)


def _state(first, last, n, f_first=1.0, f_last=1.0):
    return {"first_date": first, "last_date": last, "row_count": n, "first_factor": f_first, "last_factor": f_last}


def test_classify():
    d1, d2 = date(2024, 1, 1), date(2026, 10, 30)
    isins = {"A": ["IA"], "B": ["IB"], "N": ["IN"], "F": ["IF"], "X": ["IX"], "Z": ["IZ"]}
    stored = {
        "A": _state(d1, d2, 700),
        "B": _state(d1, d2, 700),                     # back-dated: raw has 701 days
        "F": _state(d1, d2, 700),
        "X": _state(d1, d2, 700, f_first=0.5, f_last=0.5),   # stored 1:2 split, ex_date after bookmark
        "Z": _state(d1, d2, 700),                     # new split arrives with ex_date after bookmark
    }
    ca = {"X": [(date(2026, 11, 5), 0.5)], "Z": [(date(2026, 11, 5), 0.5)]}
    raw = {"A": 700, "B": 701, "F": 700, "X": 700, "Z": 700}
    list_a, list_b = c.classify(isins, stored, ca, raw, forced_security_ids=["F"])
    assert list_a == {"A": d2, "X": d2}, list_a
    assert list_b == {"B": c.BACKDATED_DATA, "N": c.NEW_STOCK, "F": c.FORCED, "Z": c.CA_EFFECTIVE}, list_b
    _, list_b = c.classify(isins, stored, ca, raw, previous_run_ok=False)
    assert set(list_b.values()) == {c.STATE_INCONSISTENT} and len(list_b) == 6


def test_shadow_compare():
    cand = pd.DataFrame({"security_id": ["A", "A"], "trade_date": [date(2026, 11, 2), date(2026, 11, 3)],
                         "close": [10.0, 11.0]})
    act = pd.DataFrame({"security_id": ["A"], "trade_date": [date(2026, 11, 2)], "close": [10.00004]})
    r = shadow.compare(cand, act, ["security_id", "trade_date"], ["close"])
    assert r["rows_compared"] == 2 and r["mismatches"] == 1 and shadow.verdict_of(r) == "FAIL"
    r = shadow.compare(cand.iloc[:1], act, ["security_id", "trade_date"], ["close"])
    assert r["mismatches"] == 0 and shadow.verdict_of(r) == "PASS"


def _daily(sid, days, closes):
    return pd.DataFrame({
        "security_id": sid, "isin": "I" + sid, "exchange": "NSE", "series": "EQ", "symbol": sid,
        "trade_date": days, "open": closes, "high": closes, "low": closes, "close": closes, "last": closes,
        "tot_trd_qty": 100, "tot_trd_val": 1000.0, "total_trades": 5,
    })


def test_rollup_list_b_matches_full():
    """list-B whole-history rebuild == FULL's period-by-period walk (prior close read back rounded to 2dp)."""
    days = list(pd.bdate_range("2026-09-01", "2026-10-30").date)
    closes = [100 + i * 0.333 for i in range(len(days))]
    df = _daily("S1", days, closes)

    # FULL: one period at a time, prior close = stored (rounded) close of the previous period
    stored, full_rows = {}, []
    for p in sorted({period_start(d, WEEKLY) for d in days}):
        part = df[df["trade_date"].map(lambda d: period_start(d, WEEKLY)) == p]
        lookup = {("S1", "NSE", p): stored["S1"]} if "S1" in stored else {}
        out = compute_rollup(part, WEEKLY, prior_close_lookup=lookup)
        stored["S1"] = round(float(out["close"].iloc[0]), 2)
        full_rows.append(out)
    full = pd.concat(full_rows, ignore_index=True)

    # list-B: whole history at once, same fix-up as rollup_runner._rebuild_securities()
    reb = compute_rollup(df, WEEKLY)
    reb["prev_close"] = reb["close"].astype(float).round(2).groupby([reb["security_id"], reb["exchange"]]).shift(1)
    reb["ltp_percent_change"] = _percent_change(reb["close"].astype(float), reb["prev_close"])

    r = shadow.compare(reb, full, ["security_id", "trade_date"], ["close", "prev_close", "ltp_percent_change"])
    assert r["rows_compared"] == len(full) and r["mismatches"] == 0, r


def test_rsi_walk_from_anchor_matches_full():
    """INCREMENTAL RSI (Wilder step from the stored anchor at 10 decimals) == FULL's whole-history walk."""
    from core.rsi.rsi_calculator import compute_rsi14_all
    from core.rsi.rsi_continuity import build_calendar_index
    from core.rsi.rsi_recompute import walk_from_anchor, OUTPUT_COLUMNS

    days = list(pd.bdate_range("2026-06-01", "2026-09-30").date)
    closes = [0.05 + 0.01 * ((i * 7) % 5) for i in range(len(days))]     # a few-paise stock: worst case
    df = pd.DataFrame({"security_id": "P1", "isin": "IP1", "exchange": "NSE", "series": "EQ", "symbol": "P1",
                       "trade_date": days, "close": closes, "prev_close": [None] + closes[:-1]})
    cal = build_calendar_index(days)
    full = compute_rsi14_all(df, calendar_index=cal).reset_index(drop=True)

    k = 40
    new = df.iloc[k + 1:].copy()
    new["anchor_date"] = full.at[k, "trade_date"]
    new["anchor_avg_gain"] = round(full.at[k, "avg_gain"], 10)
    new["anchor_avg_loss"] = round(full.at[k, "avg_loss"], 10)
    walked = pd.DataFrame(walk_from_anchor(new, cal), columns=OUTPUT_COLUMNS)
    r = shadow.compare(walked, full, ["security_id", "trade_date"], ["gain", "loss", "avg_gain", "avg_loss", "rsi14"],
                       {"gain": 1e-9, "loss": 1e-9, "avg_gain": 1e-8, "avg_loss": 1e-8, "rsi14": 0.0101})
    assert r["rows_compared"] == len(days) - k - 1 and r["mismatches"] == 0, r

    # anchor still pre-seed, or a 5+ session gap FULL would reseed at -> whole-history fallback (None)
    pre = new.copy(); pre["anchor_avg_gain"] = None
    assert walk_from_anchor(pre, cal) is None
    gap = new.copy(); gap["anchor_date"] = full.at[k - 6, "trade_date"]
    assert walk_from_anchor(gap, cal) is None
    # averages decayed below the stored decimals (stock flat for months) -> fallback, not RSI NULL
    flat = new.copy(); flat["anchor_avg_gain"] = 0.0; flat["anchor_avg_loss"] = 0.0
    assert walk_from_anchor(flat, cal) is None


if __name__ == "__main__":
    for name, fn in list(globals().items()):
        if name.startswith("test_"):
            fn()
            print("PASS", name)
