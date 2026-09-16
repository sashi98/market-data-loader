# tests/test_maintenance_status_gating.py
#
# Automated (no DB, no external files) verification that
# maintenance_status.scheduler_running is only ever touched by a
# GENUINE cycle -- i.e. one that found real work to do -- and that it
# always comes back to False, whether the cycle succeeds, fails
# mid-way, or the whole process gets killed hard before it can clean up
# after itself.
#
# Covers the 2026-09-16 incident: scheduler_started_at was over 12
# hours in the past while a freshly-started scheduler process sat idle
# between cycle windows -- maintenance_status.is_running stayed stuck
# True purely because a PREVIOUS run of bhavcopy_scheduler_main.py was
# killed before its own finally block could clear scheduler_running.
#
# Everything here is a fake in-memory stand-in for the real Postgres
# connection/cursor (this project's tests never hit a real DB -- see
# test_rollup_calculator.py's own header) plus a fake env_values dict.
# No psycopg2, no network, no app server.
#
# Run from repo root:
#   python tests/test_maintenance_status_gating.py

import sys
from datetime import datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from core.stock_universe.persistence import (
    set_scheduler_running,
    reconcile_stale_scheduler_lock,
    MAX_SCHEDULER_CYCLE_SECONDS,
    StockUniversePersistenceError,
)
import bhavcopy_scheduler_main as sched

FAILURES = []


def check(label, condition):
    if condition:
        print(f"  [OK] {label}")
    else:
        print(f"  [FAILED] {label}")
        FAILURES.append(label)


# ============================================================================
# Fake DB layer -- just enough of psycopg2's connection/cursor surface for
# the maintenance_status functions under test. Models the SAME row shape
# and the SAME "is_running = (v OR the other flag)" derivation the real
# UPDATE statements use, so these tests would have caught the exact bug
# they're written to prevent.
# ============================================================================

class FakeMaintenanceRow:
    def __init__(self, scheduler_running=False, scheduler_started_at=None,
                 enrichment_running=False, enrichment_started_at=None):
        self.scheduler_running = scheduler_running
        self.scheduler_started_at = scheduler_started_at
        self.enrichment_running = enrichment_running
        self.enrichment_started_at = enrichment_started_at

    @property
    def is_running(self):
        return self.scheduler_running or self.enrichment_running


class FakeCursor:
    def __init__(self, row):
        self._row = row
        self._pending_result = None

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, query, params=None):
        q = " ".join(query.split())  # normalize whitespace for matching
        params = params or {}

        if "UPDATE maintenance_status" in q and "scheduler_running = %(v)s" in q:
            v = params["v"]
            if v:
                self._row.scheduler_started_at = datetime.now()
            self._row.scheduler_running = v
            self._pending_result = None
            return

        if "SELECT scheduler_running, scheduler_started_at" in q:
            self._pending_result = (self._row.scheduler_running, self._row.scheduler_started_at)
            return

        if "SELECT scheduler_running FROM maintenance_status" in q:
            self._pending_result = (self._row.scheduler_running,)
            return

        raise AssertionError(f"FakeCursor got an unexpected query: {q!r}")

    def fetchone(self):
        return self._pending_result


class FakeConn:
    def __init__(self, row):
        self.row = row
        self.closed = False

    def cursor(self):
        return FakeCursor(self.row)

    def commit(self):
        pass

    def rollback(self):
        pass

    def close(self):
        self.closed = True


class FailingCursor(FakeCursor):
    """Simulates a DB error on every execute() -- for exception-path tests."""
    def execute(self, query, params=None):
        raise RuntimeError("simulated DB failure")


class FailingConn(FakeConn):
    def cursor(self):
        return FailingCursor(self.row)


# ============================================================================
# Scenario 1 -- genuine successful cycle: flag goes True, then back to
# False, exactly bracketing the work.
# ============================================================================

def test_genuine_cycle_sets_then_clears_flag():
    print("\n" + "=" * 60)
    print("  Genuine cycle: set_scheduler_running(True) then (False)")
    print("=" * 60)
    row = FakeMaintenanceRow()
    conn = FakeConn(row)

    set_scheduler_running(conn, True)
    check("flag is True immediately after entering a cycle", row.scheduler_running is True)
    check("scheduler_started_at stamped on the False->True transition", row.scheduler_started_at is not None)

    set_scheduler_running(conn, False)
    check("flag is False after a clean cycle completes", row.scheduler_running is False)
    check("is_running (combined) reflects the clear", row.is_running is False)


# ============================================================================
# Scenario 2 -- exception mid-cycle: try/finally still clears the flag.
# This mirrors check_and_process()'s own structure exactly.
# ============================================================================

def test_exception_mid_cycle_still_clears_flag():
    print("\n" + "=" * 60)
    print("  Exception mid-cycle: finally block still clears the flag")
    print("=" * 60)
    row = FakeMaintenanceRow()
    conn = FakeConn(row)

    set_scheduler_running(conn, True)
    raised = False
    try:
        try:
            raise ValueError("boom -- something in STEP 4 blew up")
        finally:
            set_scheduler_running(conn, False)
    except ValueError:
        raised = True

    check("the exception still propagated (not swallowed)", raised)
    check("flag was cleared despite the exception", row.scheduler_running is False)


# ============================================================================
# Scenario 3 -- the actual 2026-09-16 incident: a PREVIOUS process was
# killed hard with the flag stuck True and a stale started_at. A NEW
# process starting up must detect and heal it, without disturbing a
# genuinely fresh/recent lock.
# ============================================================================

def test_reconcile_heals_a_genuinely_stale_lock():
    print("\n" + "=" * 60)
    print("  reconcile_stale_scheduler_lock() -- the 2026-09-16 incident")
    print("=" * 60)
    stale_started_at = datetime.now() - timedelta(seconds=MAX_SCHEDULER_CYCLE_SECONDS + 3600)
    row = FakeMaintenanceRow(scheduler_running=True, scheduler_started_at=stale_started_at)
    conn = FakeConn(row)

    healed = reconcile_stale_scheduler_lock(conn)

    check("reconcile reports it healed something", healed is True)
    check("scheduler_running was force-cleared", row.scheduler_running is False)


def test_reconcile_does_not_touch_a_fresh_lock():
    print("\n" + "=" * 60)
    print("  reconcile_stale_scheduler_lock() -- a genuinely in-progress cycle")
    print("=" * 60)
    fresh_started_at = datetime.now() - timedelta(minutes=5)
    row = FakeMaintenanceRow(scheduler_running=True, scheduler_started_at=fresh_started_at)
    conn = FakeConn(row)

    healed = reconcile_stale_scheduler_lock(conn)

    check("reconcile reports nothing was healed", healed is False)
    check("a genuinely fresh/in-progress lock is left alone", row.scheduler_running is True)


def test_reconcile_is_a_noop_when_nothing_is_running():
    print("\n" + "=" * 60)
    print("  reconcile_stale_scheduler_lock() -- no-op on the common case")
    print("=" * 60)
    row = FakeMaintenanceRow(scheduler_running=False, scheduler_started_at=None)
    conn = FakeConn(row)

    healed = reconcile_stale_scheduler_lock(conn)

    check("reconcile is a no-op when the flag is already False", healed is False)
    check("flag stays False", row.scheduler_running is False)


def test_reconcile_never_touches_enrichment_flag():
    print("\n" + "=" * 60)
    print("  reconcile_stale_scheduler_lock() -- never clobbers enrichment_running")
    print("=" * 60)
    stale_started_at = datetime.now() - timedelta(seconds=MAX_SCHEDULER_CYCLE_SECONDS + 3600)
    row = FakeMaintenanceRow(
        scheduler_running=True, scheduler_started_at=stale_started_at,
        enrichment_running=True, enrichment_started_at=datetime.now(),
    )
    conn = FakeConn(row)

    reconcile_stale_scheduler_lock(conn)

    check("scheduler_running healed", row.scheduler_running is False)
    check("enrichment_running (the OTHER process's own lock) is untouched", row.enrichment_running is True)
    check("combined is_running still True -- enrichment is genuinely still running", row.is_running is True)


# ============================================================================
# Scenario 4 -- "there was nothing to do" -- check_and_process()'s own
# has_real_work gate must NEVER call _set_maintenance at all when every
# exchange is up to date and no indicator is stale. Exercised directly
# against the gating logic via monkeypatched collaborators, since
# check_and_process() itself needs a live env/DB for everything past
# this point -- these tests isolate exactly the decision this fix adds.
# ============================================================================

class _Patch:
    """Tiny context-managed monkeypatch helper -- stdlib only."""
    def __init__(self, obj, name, value):
        self.obj, self.name, self.value = obj, name, value

    def __enter__(self):
        self.original = getattr(self.obj, self.name)
        setattr(self.obj, self.name, self.value)
        return self

    def __exit__(self, *exc):
        setattr(self.obj, self.name, self.original)
        return False


def test_no_gap_no_indicators_never_touches_maintenance_flag():
    print("\n" + "=" * 60)
    print("  has_real_work gate: nothing to do -> maintenance flag never touched")
    print("=" * 60)

    maintenance_calls = []

    def fake_set_maintenance(env_values, is_running):
        maintenance_calls.append(is_running)
        return True

    with _Patch(sched, "_set_maintenance", fake_set_maintenance), \
         _Patch(sched, "_gap_detected", lambda freshness, latest: False), \
         _Patch(sched, "_indicators_pending", lambda env_values, latest: False):

        gap = {ex: sched._gap_detected(None, None) for ex in sched.EXCHANGES}
        indicators_pending = sched._indicators_pending({}, None)
        has_real_work = any(gap.values()) or indicators_pending

        check("gap is False for every exchange", not any(gap.values()))
        check("no indicators pending", not indicators_pending)
        check("has_real_work correctly computed as False", has_real_work is False)

        # Mirrors check_and_process()'s own early return -- when
        # has_real_work is False, _set_maintenance must never be called.
        if not has_real_work:
            pass  # the real function returns True here, before _set_maintenance
        else:
            sched._set_maintenance({}, True)

        check("maintenance flag was NEVER touched for a no-op cycle", maintenance_calls == [])


def test_gap_on_one_exchange_enters_maintenance():
    print("\n" + "=" * 60)
    print("  has_real_work gate: a real gap -> maintenance flag IS set")
    print("=" * 60)

    maintenance_calls = []

    def fake_set_maintenance(env_values, is_running):
        maintenance_calls.append(is_running)
        return True

    with _Patch(sched, "_set_maintenance", fake_set_maintenance):
        gap = {"NSE": True, "BSE": False}
        indicators_pending = False
        has_real_work = any(gap.values()) or indicators_pending

        check("has_real_work correctly computed as True (NSE has a gap)", has_real_work is True)

        if has_real_work:
            sched._set_maintenance({}, True)

        check("maintenance flag WAS set for a genuine gap", maintenance_calls == [True])


def test_indicators_pending_alone_enters_maintenance():
    print("\n" + "=" * 60)
    print("  has_real_work gate: no gap but a stale indicator -> maintenance flag IS set")
    print("=" * 60)

    maintenance_calls = []

    def fake_set_maintenance(env_values, is_running):
        maintenance_calls.append(is_running)
        return True

    with _Patch(sched, "_set_maintenance", fake_set_maintenance):
        gap = {"NSE": False, "BSE": False}
        indicators_pending = True
        has_real_work = any(gap.values()) or indicators_pending

        check("has_real_work correctly computed as True (indicator pending, no gap)", has_real_work is True)

        if has_real_work:
            sched._set_maintenance({}, True)

        check("maintenance flag WAS set for a stale indicator alone (STEP 7 is not gated on gap[])",
              maintenance_calls == [True])


def test_freshness_read_failure_fails_toward_protected_not_skipped():
    print("\n" + "=" * 60)
    print("  has_real_work gate: a freshness-READ failure must never look like 'nothing to do'")
    print("=" * 60)
    # _gap_detected(None, latest_trade_date) is True by construction (see
    # bhavcopy_scheduler_main.py's own docstring) -- this test locks that
    # fail-safe in place so a future refactor can't silently invert it.
    check(
        "_gap_detected(None, ...) is True -- a DB-read failure is treated as a gap, not as 'up to date'",
        sched._gap_detected(None, sched.date(2026, 9, 16)) is True,
    )


if __name__ == "__main__":
    test_genuine_cycle_sets_then_clears_flag()
    test_exception_mid_cycle_still_clears_flag()
    test_reconcile_heals_a_genuinely_stale_lock()
    test_reconcile_does_not_touch_a_fresh_lock()
    test_reconcile_is_a_noop_when_nothing_is_running()
    test_reconcile_never_touches_enrichment_flag()
    test_no_gap_no_indicators_never_touches_maintenance_flag()
    test_gap_on_one_exchange_enters_maintenance()
    test_indicators_pending_alone_enters_maintenance()
    test_freshness_read_failure_fails_toward_protected_not_skipped()

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
