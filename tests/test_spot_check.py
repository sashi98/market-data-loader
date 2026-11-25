# tests/test_spot_check.py -- TMT-MDL-US-0005-005
#
# Spot check for INCREMENTAL steps: the rotation covers every stock once per 50 weekdays, list-B is skipped, a stored
# value that drifted from the FULL result is caught and the stock is queued (SPOT_CHECK) for a full rebuild next
# cycle, a clean stock is left alone, and an error inside the check never escapes.
# No DB: a fake connection that records the SQL. Run: python tests/test_spot_check.py (or pytest).

import sys
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pandas as pd  # noqa: E402

from core.recompute import spot_check as sc  # noqa: E402
from core.recompute import shadow  # noqa: E402


class FakeCursor:
    def __init__(self, conn):
        self.conn = conn

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def execute(self, sql, params=None):
        self.conn.executed.append((" ".join(sql.split()), params))


class FakeConn:
    def __init__(self):
        self.executed, self.commits, self.rollbacks = [], 0, 0

    def cursor(self):
        return FakeCursor(self)

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1

    def queued(self):
        return [p for sql, p in self.executed if sql.startswith("INSERT INTO recompute_queue")]

    def checks(self):
        return [p for sql, p in self.executed if sql.startswith("INSERT INTO recompute_shadow_check")]


SIDS = [f"INE{n:06d}" for n in range(3000)]


def test_rotation_covers_every_stock_once_per_50_weekdays():
    days = list(pd.bdate_range("2026-11-02", periods=50).date)
    seen = []
    for d in days:
        seen.extend(sc.select_sample(SIDS, d))
    assert sorted(seen) == sorted(SIDS)                      # every stock, exactly once
    sizes = [len(sc.select_sample(SIDS, d)) for d in days]
    assert 30 <= min(sizes) and max(sizes) <= 95, sizes      # ~2% (60) a day, no huge day
    # the 51st weekday starts the rotation again
    assert sc.select_sample(SIDS, pd.bdate_range("2026-11-02", periods=51).date[-1]) == sc.select_sample(SIDS, days[0])


def test_slot_is_stable_and_list_b_is_skipped():
    assert sc.slot_of("INE002A01018") == sc.slot_of("INE002A01018") and 0 <= sc.slot_of("X") < 50
    d = date(2026, 11, 24)
    sample = sc.select_sample(SIDS, d)
    assert sample and sc.select_sample(SIDS, d, exclude=sample[:2]) == sample[2:]
    from datetime import datetime
    assert sc.slot_for_date(datetime(2026, 11, 24, 17, 0)) == sc.slot_for_date(d)


def _truth(sids):
    rows = []
    for sid in sids:
        for i, d in enumerate(pd.bdate_range("2026-11-02", periods=5).date):
            rows.append({"security_id": sid, "trade_date": d, "close": 100.0 + i, "rsi14": 50.0 + i})
    return pd.DataFrame(rows)


def test_drifted_stock_is_queued_and_clean_stock_is_not():
    conn = FakeConn()
    sample = ["S1", "S2", "S3"]
    stored = _truth(sample)
    stored.loc[(stored["security_id"] == "S2") & (stored.index % 5 == 4), "rsi14"] += 0.37   # drifted on its last day
    r = sc.check(conn, date(2026, 11, 24), "RSI14D", "NSE", "RSI14D", sample, 4000, _truth, lambda t: stored,
                 ["security_id", "trade_date"], ["close", "rsi14"], {"rsi14": 0.0101})
    assert r["mismatches"] == 1 and r["mismatched_security_ids"] == {"S2"}
    assert conn.queued() == [("S2", "NSE", "RSI14D", "SPOT_CHECK", "spot check RSI14D 2026-11-24")]
    (row,) = conn.checks()
    assert row[1] == "SPOT:RSI14D" and row[3] == 3 and row[4] == 1 and row[8] == "FAIL"

    conn = FakeConn()
    r = sc.check(conn, date(2026, 11, 24), "RSI14D", "NSE", "RSI14D", sample, 4000, _truth, lambda t: _truth(sample),
                 ["security_id", "trade_date"], ["close", "rsi14"])
    assert r["mismatches"] == 0 and conn.queued() == [] and conn.checks()[0][8] == "PASS"


def test_missing_stored_row_counts_as_drift():
    conn = FakeConn()
    stored = _truth(["S1"]).iloc[:-1]                         # last day never written
    r = sc.check(conn, date(2026, 11, 24), "WEEKLY_ROLLUP", "BSE", "WEEKLY_ROLLUP", ["S1"], 10, _truth,
                 lambda t: stored, ["security_id", "trade_date"], ["close"])
    assert r["mismatched_security_ids"] == {"S1"} and conn.queued()[0][2:4] == ("WEEKLY_ROLLUP", "SPOT_CHECK")


def test_error_inside_the_check_never_escapes():
    conn = FakeConn()

    def boom(_):
        raise RuntimeError("db went away")
    assert sc.check(conn, date(2026, 11, 24), "RSI14W", "NSE", "RSI14W", ["S1"], 10, boom, None,
                    ["security_id", "trade_date"], ["close"]) is None
    assert conn.rollbacks == 1 and conn.queued() == []
    assert sc.check(conn, date(2026, 11, 24), "RSI14W", "NSE", "RSI14W", [], 10, boom, None,
                    ["security_id", "trade_date"], ["close"]) is None    # empty slot: skipped, nothing called


def test_real_rsi_drift_is_caught():
    """Real RSI maths: a stored avg_gain that drifted from the whole-history walk is flagged; the clean copy passes."""
    from core.rsi.rsi_calculator import compute_rsi14_all
    days = list(pd.bdate_range("2026-06-01", "2026-09-30").date)
    closes = [100 + ((i * 7) % 11) - 5 for i in range(len(days))]
    df = pd.DataFrame({"security_id": "P1", "isin": "IP1", "exchange": "NSE", "series": "EQ", "symbol": "P1",
                       "trade_date": days, "close": closes, "prev_close": [None] + closes[:-1]})
    full = compute_rsi14_all(df, calendar_index=None)
    cols = ["gain", "loss", "avg_gain", "avg_loss", "rsi14"]
    tol = {"gain": 0.0001, "loss": 0.0001, "avg_gain": 0.0001, "avg_loss": 0.0001, "rsi14": 0.0101}
    assert shadow.compare(full, full.copy(), ["security_id", "trade_date"], cols, tol)["mismatches"] == 0
    drifted = full.copy()
    drifted.loc[drifted.index[-1], "avg_gain"] = drifted["avg_gain"].iloc[-1] + 0.01
    conn = FakeConn()
    r = sc.check(conn, date(2026, 9, 30), "RSI14D", "NSE", "RSI14D", ["P1"], 1, lambda s: full, lambda t: drifted,
                 ["security_id", "trade_date"], cols, tol)
    assert r["mismatched_security_ids"] == {"P1"} and len(conn.queued()) == 1


if __name__ == "__main__":
    for name, fn in list(globals().items()):
        if name.startswith("test_"):
            fn()
            print("PASS", name)
