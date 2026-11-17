# tests/test_startup_lock_and_wm_freshness.py -- TMT-MDL-BUG-0013 / TMT-MDL-BUG-0005
#
# 1. A freshly started scheduler / enrichment listener clears a lock left by a killed predecessor, whatever its age
#    (the old 6-hour rule kept prod locked after a mid-cycle deploy restart, 15-Nov-2026), and never the other flag.
# 2. Weekly / monthly indicators record the date the run really covers, so a weekend with no new data is not
#    "behind" and does not enter maintenance.
# No DB: an in-memory maintenance_status row. Run: python tests/test_startup_lock_and_wm_freshness.py (or pytest).

import sys
from datetime import date, datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from core.stock_universe.persistence import clear_scheduler_lock_at_startup, clear_enrichment_lock_at_startup  # noqa: E402
from core.indicators.persistence import covered_through  # noqa: E402
import bhavcopy_scheduler_main as sched  # noqa: E402


class Row:
    def __init__(self, scheduler=False, scheduler_at=None, enrichment=False, enrichment_at=None):
        self.scheduler, self.scheduler_at = scheduler, scheduler_at
        self.enrichment, self.enrichment_at = enrichment, enrichment_at
        self.updates = 0

    @property
    def is_running(self):
        return self.scheduler or self.enrichment


class Cursor:
    def __init__(self, row):
        self.row, self.result = row, None

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, query, params=None):
        q = " ".join(query.split())
        if q.startswith("SELECT scheduler_running, scheduler_started_at"):
            self.result = (self.row.scheduler, self.row.scheduler_at)
        elif q.startswith("SELECT enrichment_running, enrichment_started_at"):
            self.result = (self.row.enrichment, self.row.enrichment_at)
        elif "UPDATE maintenance_status" in q and "SET scheduler_running = %(v)s" in q:
            self.row.scheduler, self.row.updates = params["v"], self.row.updates + 1
        elif "UPDATE maintenance_status" in q and "SET enrichment_running = %(v)s" in q:
            self.row.enrichment, self.row.updates = params["v"], self.row.updates + 1
        else:
            raise AssertionError(f"unexpected query: {q}")

    def fetchone(self):
        return self.result


class Conn:
    def __init__(self, row):
        self.row = row

    def cursor(self):
        return Cursor(self.row)

    def commit(self):
        pass

    def rollback(self):
        pass


def test_l1_fresh_scheduler_lock_is_cleared_at_startup():
    # 15-Nov-2026: the lock was 24 minutes old when the restarted scheduler came up -- the 6-hour rule kept it
    started = datetime.now() - timedelta(minutes=24)
    row = Row(scheduler=True, scheduler_at=started)
    assert clear_scheduler_lock_at_startup(Conn(row)) == started
    assert row.scheduler is False and row.is_running is False


def test_l2_scheduler_startup_never_touches_enrichment():
    row = Row(scheduler=True, scheduler_at=datetime.now(), enrichment=True, enrichment_at=datetime.now())
    clear_scheduler_lock_at_startup(Conn(row))
    assert row.scheduler is False and row.enrichment is True and row.is_running is True


def test_l3_no_lock_no_write():
    row = Row()
    assert clear_scheduler_lock_at_startup(Conn(row)) is None
    assert clear_enrichment_lock_at_startup(Conn(row)) is None
    assert row.updates == 0


def test_l4_enrichment_lock_is_cleared_at_listener_startup_and_scheduler_kept():
    started = datetime.now() - timedelta(minutes=3)
    row = Row(scheduler=True, scheduler_at=datetime.now(), enrichment=True, enrichment_at=started)
    assert clear_enrichment_lock_at_startup(Conn(row)) == started
    assert row.enrichment is False and row.scheduler is True


def test_f1_weekly_bar_of_the_current_week_covers_the_trade_date():
    # rsi14w's newest bar is "09-NOV" (week of Mon 09-Nov) after the 13-Nov (Fri) run
    assert covered_through("rsi14w", date(2026, 11, 9), date(2026, 11, 13)) == date(2026, 11, 13)
    assert covered_through("ma50w", date(2026, 11, 9), date(2026, 11, 13)) == date(2026, 11, 13)


def test_f2_monthly_bar_of_the_current_month_covers_the_trade_date():
    assert covered_through("rsi14m", date(2026, 11, 1), date(2026, 11, 13)) == date(2026, 11, 13)
    assert covered_through("ma200m", date(2026, 11, 2), date(2026, 11, 30)) == date(2026, 11, 30)


def test_f3_an_older_period_is_not_stretched():
    # newest weekly bar is last week's: the run did not reach end_date, keep the bar date
    assert covered_through("rsi14w", date(2026, 11, 2), date(2026, 11, 13)) == date(2026, 11, 2)
    assert covered_through("rsi14m", date(2026, 10, 1), date(2026, 11, 13)) == date(2026, 10, 1)


def test_f4_daily_indicators_unchanged_and_none_safe():
    assert covered_through("rsi14d", date(2026, 11, 12), date(2026, 11, 13)) == date(2026, 11, 12)
    assert covered_through("ma9d", date(2026, 11, 13), date(2026, 11, 13)) == date(2026, 11, 13)
    assert covered_through("rsi14w", None, date(2026, 11, 13)) is None


def test_f5_weekend_is_up_to_date_next_trading_day_is_pending():
    recorded = covered_through("rsi14w", date(2026, 11, 9), date(2026, 11, 13))
    # Sat / Sun cycle: latest trade date is still Fri 13-Nov -> nothing pending, no maintenance window
    assert sched._own_target_stale(recorded, date(2026, 11, 13)) is False
    # Mon 16-Nov brings new data (same rule for Tue 17-Nov inside the same week) -> recompute
    assert sched._own_target_stale(recorded, date(2026, 11, 16)) is True
    tuesday = covered_through("rsi14w", date(2026, 11, 16), date(2026, 11, 16))
    assert sched._own_target_stale(tuesday, date(2026, 11, 17)) is True


if __name__ == "__main__":
    failures = 0
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            try:
                fn()
                print(f"[PASS] {name}")
            except Exception as e:  # noqa: BLE001
                failures += 1
                print(f"[FAIL] {name}: {e!r}")
    print("ALL PASSED" if not failures else f"{failures} FAILED")
    sys.exit(1 if failures else 0)
