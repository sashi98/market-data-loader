# tests/test_corporate_action_apply.py -- TMT-MDL-BUG-0011 pure-logic checks (no DB).
# Every corporate action that changes the price (split / bonus with a usable factor) must be applied,
# whatever its reconciliation_status -- NSE_ONLY / BSE_ONLY / CONFLICT as well as MATCHED.
# Run: python3 tests/test_corporate_action_apply.py   (or pytest)
import sys
from datetime import date
from decimal import Decimal
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from core.corporate_actions import adjustment
from core.corporate_actions import corporate_actions_persistence as p
from core.price_series import adjusted_series

EX = date(2026, 11, 4)
KEY = ("INE338H01029", "BONUS", EX)


class FakeCursor:
    def __init__(self, raw_rows, existing):
        self.raw_rows, self.existing, self.writes, self._last = raw_rows, existing, [], None

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def execute(self, sql, params):
        s = " ".join(sql.split())
        if s.startswith("SELECT exchange"):
            self._last = self.raw_rows
        elif s.startswith("SELECT id"):
            self._last = [self.existing] if self.existing else []
        else:
            self.writes.append((s.split()[0], params))
            self._last = []

    def fetchall(self):
        return self._last

    def fetchone(self):
        return self._last[0] if self._last else None


class FakeConn:
    def __init__(self, cur):
        self.cur = cur

    def cursor(self):
        return self.cur


def raw(exchange, old, new, text="Bonus 1:1"):
    return (exchange, "CONCORDBIO", text, None if old is None else Decimal(old), None if new is None else Decimal(new))


def run(raw_rows, existing=None):
    cur = FakeCursor(raw_rows, existing)
    newly = p.reconcile(FakeConn(cur), {KEY})
    verb, params = cur.writes[-1]
    factor = params[4] if verb == "INSERT" else params[1]
    status = params[7] if verb == "INSERT" else params[4]
    return status, factor, newly


def test_nse_only_bonus_gets_factor_and_counts_as_applied():
    status, factor, newly = run([raw("NSE", 2, 1)])
    assert status == "NSE_ONLY" and factor == Decimal("0.5"), (status, factor)
    assert newly == [KEY]


def test_bse_only_split_gets_factor():
    status, factor, newly = run([raw("BSE", 10, 1, "Stock Split From Rs.10/- to Re.1/-")])
    assert status == "BSE_ONLY" and factor == Decimal("0.1"), (status, factor)
    assert newly == [KEY]


def test_conflict_applies_nse_factor():
    status, factor, newly = run([raw("NSE", 2, 1), raw("BSE", 3, 1, "Bonus 2:1")])
    assert status == "CONFLICT" and factor == Decimal("0.5"), (status, factor)
    assert newly == [KEY]


def test_matched_unchanged():
    status, factor, newly = run([raw("NSE", 2, 1), raw("BSE", 2, 1)])
    assert status == "MATCHED" and factor == Decimal("0.5")
    assert newly == [KEY]


def test_unparsed_ratio_not_applied():
    status, factor, newly = run([raw("NSE", None, None, "Bonus (ratio tbd)")])
    assert status == "NSE_ONLY" and factor is None
    assert newly == []


def test_already_applied_not_counted_again():
    # NSE_ONLY row stored earlier with its factor, BSE now confirms -> MATCHED, but not "newly" applied
    status, factor, newly = run([raw("NSE", 2, 1), raw("BSE", 2, 1)], existing=(7, "NSE_ONLY", Decimal("0.5")))
    assert status == "MATCHED" and newly == []


def test_price_sql_does_not_gate_on_status():
    for sql in (adjusted_series.MATCHED_CORPORATE_ACTIONS_SQL, adjustment.ADJUSTMENT_FACTOR_JOIN_SQL):
        assert "reconciliation_status" not in sql
        assert "adjustment_factor IS NOT NULL" in sql


if __name__ == "__main__":
    for name, fn in list(globals().items()):
        if name.startswith("test_") and callable(fn):
            fn()
            print("OK", name)
