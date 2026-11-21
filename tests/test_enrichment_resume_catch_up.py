# tests/test_enrichment_resume_catch_up.py -- TMT-MDL-US-0006 parts 2 and 3
# Part 3: ISINs with a [NO DATA] / [FAILED] side get one catch-up pass, and their first-pass counts / audit rows
#         are replaced (never counted twice). Part 2: a resumed batch skips ISINs the cut-off attempt finished.
# No network / DB: the worker pass and the DB reads are faked. Third-party modules missing locally are stubbed.
# Run: python tests/test_enrichment_resume_catch_up.py (or pytest)

import importlib
import sys
import types
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))


def _import_listener():
    for _ in range(40):
        try:
            return importlib.import_module("stock_universe_update_listener")
        except ModuleNotFoundError as e:
            name = e.name
            parts = name.split(".")
            for i in range(1, len(parts) + 1):
                sub = ".".join(parts[:i])
                if sub not in sys.modules:
                    sys.modules[sub] = mock.MagicMock(name=sub)
    raise RuntimeError("could not import the listener")


sul = _import_listener()

ISINS = [(f"INE{i:03d}", {"nse_symbol": f"S{i}", "nse_exchange": "NSE", "bse_symbol": None,
                          "bse_exchange": "BSE", "bse_security_code": f"{500000 + i}"}) for i in range(5)]


def _audit(isin, exch, outcome):
    return {"isin_number": isin, "exchange": exch, "outcome": outcome, "reason": "x"}


def test_c1_catch_up_replaces_first_pass_outcomes():
    # first pass: INE000 NSE ok + BSE no data; INE001 both ok; INE002 NSE failed, BSE excluded
    results = {"enriched": 3, "failed": 2, "excluded": 1, "ok": ["INE000", "INE001", "INE001"],
               "audit": [_audit("INE000", "BSE", "NO_DATA"), _audit("INE002", "NSE", "FAILED"),
                         _audit("INE002", "BSE", "EXCLUDED"), _audit("INE001", "NSE", "FIELD_DROPPED")]}
    seen = []

    def fake_pass(env, items, res, lock, tax):
        seen.extend(i for i, _ in items)
        # catch-up: INE000 both ok now; INE002 NSE ok, BSE excluded again
        res["enriched"] += 3
        res["excluded"] += 1
        res["ok"] += ["INE000", "INE000", "INE002"]
        res["audit"].append(_audit("INE002", "BSE", "EXCLUDED"))
    with mock.patch.object(sul, "_run_pass", fake_pass):
        sul._catch_up_pass({}, ISINS, results, mock.MagicMock(), None)
    assert sorted(seen) == ["INE000", "INE002"], seen
    assert results["enriched"] == 3 - 1 + 3, results     # INE000's first NSE ok replaced
    assert results["failed"] == 0, results
    assert results["excluded"] == 1, results
    assert sorted(results["ok"]) == ["INE000", "INE000", "INE001", "INE001", "INE002"]
    outcomes = sorted((a["isin_number"], a["outcome"]) for a in results["audit"])
    assert outcomes == [("INE001", "FIELD_DROPPED"), ("INE002", "EXCLUDED")], outcomes


def test_c2_no_failures_no_catch_up():
    results = {"enriched": 2, "failed": 0, "excluded": 0, "ok": ["INE000", "INE001"], "audit": []}
    with mock.patch.object(sul, "_run_pass", side_effect=AssertionError("must not run")):
        sul._catch_up_pass({}, ISINS, results, mock.MagicMock(), None)
    assert results["enriched"] == 2


def test_c3_still_failing_stays_in_audit():
    results = {"enriched": 0, "failed": 1, "excluded": 0, "ok": [], "audit": [_audit("INE003", "NSE", "NO_DATA")]}

    def fake_pass(env, items, res, lock, tax):
        res["failed"] += 1
        res["audit"].append(_audit("INE003", "NSE", "NO_DATA"))
    with mock.patch.object(sul, "_run_pass", fake_pass):
        sul._catch_up_pass({}, ISINS, results, mock.MagicMock(), None)
    assert results["failed"] == 1 and len(results["audit"]) == 1


def test_r1_resume_skips_finished_isins():
    ran = []

    def fake_pass(env, items, res, lock, tax):
        ran.extend(i for i, _ in items)
    conn = mock.MagicMock()
    with mock.patch.object(sul, "get_connection", return_value=conn), \
         mock.patch.object(sul, "fetch_isin_exchange_map", return_value=dict(ISINS)), \
         mock.patch.object(sul, "fetch_isins_enriched_since", return_value={"INE000", "INE001", "INE002"}), \
         mock.patch.object(sul, "TaxonomyMaps"), \
         mock.patch.object(sul, "_run_pass", fake_pass), \
         mock.patch.object(sul, "record_enrichment_run", return_value=1), \
         mock.patch.object(sul, "record_enrichment_audit_rows"):
        try:
            sul._run_enrichment_batch_body({}, 15, None, sul.datetime.now(sul.timezone.utc), resume_from="2026-11-21 09:23:45")
        except Exception:
            pass   # the summary / cursor bookkeeping after the pass is not under test here
    assert sorted(ran) == ["INE003", "INE004"], ran


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
