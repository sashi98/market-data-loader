# tests/test_rsi_full_batches_and_failed_indicator.py -- TMT-MDL-BUG-0017
# 23-Nov-2026 prod: the FULL rsi14d recompute loaded a whole exchange (~3M rows) at once on an 8 GB PC, PostgreSQL
# dropped its connections, rsi14d failed -- and the cycle was still marked complete, so nothing retried that night.
#   1. FULL now runs in batches of stocks: the result must equal the one-shot computation exactly.
#   2. A failed indicator makes _run_indicator() return False (the cycle stays open and is retried).
# No DB. Run: python tests/test_rsi_full_batches_and_failed_indicator.py (or pytest)

import sys
from datetime import date, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402
from core.rsi import rsi_recompute as rr  # noqa: E402
from core.rsi.rsi_calculator import compute_rsi14_all  # noqa: E402
import bhavcopy_scheduler_main as sched  # noqa: E402

CFG = {"source_table": "bhav_copy_adjusted", "target_table": "rsi14d_workbook", "sma_window": 14}


def _prices(n_stocks=7, days=60, seed=3):
    rng = np.random.default_rng(seed)
    rows, start = [], date(2026, 8, 3)
    for s in range(n_stocks):
        sid, close = f"SID{s:03d}", 100.0 + 10 * s
        prev = None
        for d in range(days):
            close = round(close * (1 + rng.normal(0, 0.02)), 2)
            rows.append({"security_id": sid, "isin": f"INE{s:03d}", "exchange": "NSE", "series": "EQ",
                         "symbol": f"S{s}", "trade_date": start + timedelta(days=d), "close": close,
                         "prev_close": prev if prev is not None else close})
            prev = close
    return pd.DataFrame(rows)


def test_r1_batched_full_equals_one_shot():
    df = _prices()
    one_shot = rr.add_rsi_sma(compute_rsi14_all(df.copy()), 14)

    written = []
    originals = (rr._exchange_security_ids, rr._fetch_full_history, rr.upsert_rsi_workbook)
    rr._exchange_security_ids = lambda conn, cfg, ex: sorted(df["security_id"].unique())
    rr._fetch_full_history = lambda conn, cfg, ex, sids: df[df["security_id"].isin(sids)].reset_index(drop=True)

    def capture(conn, table, frame):
        written.append(frame.copy())
        return len(frame)
    rr.upsert_rsi_workbook = capture
    try:
        n = rr._full_in_batches(None, CFG, "rsi14d", "NSE", None, batch_size=3)
    finally:
        rr._exchange_security_ids, rr._fetch_full_history, rr.upsert_rsi_workbook = originals

    assert len(written) == 3, "7 stocks in batches of 3 -> 3 writes"
    batched = pd.concat(written, ignore_index=True)
    assert n == len(batched) == len(one_shot)
    key = ["security_id", "trade_date"]
    a = one_shot.sort_values(key).reset_index(drop=True)
    b = batched.sort_values(key).reset_index(drop=True)
    for col in ["rsi14", "avg_gain", "avg_loss", rr.SMA_COLUMN]:
        pd.testing.assert_series_equal(pd.to_numeric(a[col], errors="coerce"), pd.to_numeric(b[col], errors="coerce"),
                                       check_names=False)


class _Conn:
    def close(self):
        pass


def _run_indicator_with(runner):
    saved = (sched.RUNNERS.get("rsi14d"), sched.get_connection, sched.deactivate_indicator)
    sched.RUNNERS["rsi14d"] = runner
    sched.get_connection = lambda env: _Conn()
    sched.deactivate_indicator = lambda conn, iid: None
    try:
        env = {k: "FULL" for k in ("MDL_MODE_OF_RECOMPUTE_RSI14D", "MDL_MODE_OF_RECOMPUTE_RSI14W",
                                   "MDL_MODE_OF_RECOMPUTE_RSI14M", "MDL_MODE_OF_RECOMPUTE_PRICE_ADJUSTMENT",
                                   "MDL_MODE_OF_RECOMPUTE_WEEKLY_ROLLUP", "MDL_MODE_OF_RECOMPUTE_MONTHLY_ROLLUP")}
        return sched._run_indicator(env, "rsi14d", date(2024, 1, 1), date(2026, 11, 23))
    finally:
        sched.RUNNERS["rsi14d"], sched.get_connection, sched.deactivate_indicator = saved


def test_r2_completed_indicator_returns_true():
    assert _run_indicator_with(lambda *a, **k: True) is True


def test_r3_runner_reporting_failure_returns_false():
    assert _run_indicator_with(lambda *a, **k: False) is False


def test_r4_connection_dropped_mid_run_returns_false():
    def boom(*a, **k):
        raise Exception("connection already closed")   # the 23-Nov-2026 failure
    assert _run_indicator_with(boom) is False


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
