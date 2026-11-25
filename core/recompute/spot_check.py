# core/recompute/spot_check.py -- TMT-MDL-US-0005-005
#
# Spot check: the safety net for steps running in INCREMENTAL mode. INCREMENTAL only computes the new day(s)
# and builds on what is stored, so a wrong stored value would be carried forward unnoticed. Every cycle, after
# an INCREMENTAL step has written its rows, a small slice of its stocks is recomputed over the WHOLE history
# (the FULL way, in memory, nothing written) and compared with what is stored:
#   - which stocks: a fixed rotation, no state kept -- each stock belongs to one of SPOT_CHECK_ROTATION_DAYS slots
#     (crc32 of its security_id), and each cycle checks the slot of its trade date's weekday number. That is
#     about 2% of the stocks per cycle, and every stock once every 50 weekdays (~10 weeks). Stocks rebuilt in
#     full this cycle (list-B) are skipped: they were just recomputed the FULL way anyway.
#   - result: one "SPOT CHECK <target> [<exchange>]" verdict line + a recompute_shadow_check row with target
#     "SPOT:<target>" (same tolerances as SHADOW).
#   - a mismatch never fails the step (alert, don't block): the stock is queued in recompute_queue with reason
#     SPOT_CHECK, so the NEXT cycle rebuilds its whole history as list-B (and the downstream steps follow it
#     through the queue as usual).
#   - FULL and SHADOW runs are not spot-checked: FULL recomputes everything, SHADOW compares everything.
#   - any error inside the spot check is logged as a warning and swallowed -- it can never break a cycle.

import zlib
from datetime import date

import numpy as np

from core.recompute import queue as recompute_queue
from core.recompute import shadow

SPOT_CHECK_ROTATION_DAYS = 50          # ~2% of the stocks per cycle; every stock once every 50 weekdays
REASON = "SPOT_CHECK"
_ROTATION_EPOCH = date(2024, 1, 1)     # a Monday; only fixes which slot a given weekday gets


def slot_of(security_id, rotation_days=SPOT_CHECK_ROTATION_DAYS):
    """The stock's fixed slot, 0..rotation_days-1 (stable across runs, machines and Python versions)."""
    return zlib.crc32(str(security_id).encode("utf-8")) % rotation_days


def slot_for_date(cycle_trade_date, rotation_days=SPOT_CHECK_ROTATION_DAYS):
    """The slot checked on this trade date: weekday number since the epoch, so consecutive weekdays walk the slots."""
    d = cycle_trade_date.date() if hasattr(cycle_trade_date, "date") and callable(cycle_trade_date.date) \
        else cycle_trade_date
    return int(np.busday_count(_ROTATION_EPOCH, d)) % rotation_days


def select_sample(security_ids, cycle_trade_date, exclude=(), rotation_days=SPOT_CHECK_ROTATION_DAYS):
    """The stocks to spot-check this cycle: those in today's slot, minus `exclude` (list-B / queued)."""
    today = slot_for_date(cycle_trade_date, rotation_days)
    skip = {str(s) for s in exclude}
    return sorted(str(s) for s in set(security_ids)
                  if str(s) not in skip and slot_of(s, rotation_days) == today)


def check(conn, cycle_trade_date, target, exchange, queue_target, sample, population, recompute_fn, stored_fn,
          key_columns, value_columns, tolerances=None, rotation_days=SPOT_CHECK_ROTATION_DAYS):
    """
    Runs one spot check and prints its verdict line. Never raises.
      sample       -- security_ids to check (from select_sample)
      population   -- how many stocks the step covers (for the log line)
      recompute_fn -- sample -> DataFrame: whole history recomputed the FULL way, in memory
      stored_fn    -- truth DataFrame -> DataFrame: what the table holds for those keys
    Mismatching stocks are queued under queue_target with reason SPOT_CHECK. Returns the compare result,
    or None when it could not run.
    """
    slot = slot_for_date(cycle_trade_date, rotation_days)
    if not sample:
        print(f"  SPOT CHECK {target} [{exchange}]: no stocks in slot {slot + 1}/{rotation_days} this cycle -- skipped.")
        return None
    try:
        truth = recompute_fn(sample)
        stored = stored_fn(truth) if truth is not None and not truth.empty else None
        result = shadow.compare(truth, stored, key_columns, value_columns, tolerances, labels=("recomputed", "stored"))
        detail = (f"{len(sample):,} of {population:,} stocks (slot {slot + 1}/{rotation_days}), ")
        failed = sorted(result["mismatched_security_ids"])
        print(f"  {shadow.record(conn, cycle_trade_date, target, exchange, result, list_a_count=len(sample), list_b_count=len(failed), kind='SPOT CHECK', detail=detail)}")
        if failed:
            try:
                recompute_queue.enqueue(conn, exchange, [queue_target], {sid: REASON for sid in failed},
                                        detail=f"spot check {target} {cycle_trade_date}")
                print(f"    -> {len(failed)} stock(s) queued for a full rebuild next cycle ({queue_target}): "
                      f"{', '.join(failed[:10])}{' ...' if len(failed) > 10 else ''}")
            except recompute_queue.RecomputeQueueError as e:
                print(f"  [WARN] SPOT CHECK {target}/{exchange}: could not queue the failed stocks: {e}")
        return result
    except Exception as e:  # noqa: BLE001 -- a spot check must never break the cycle
        try:
            conn.rollback()
        except Exception:  # noqa: BLE001
            pass
        print(f"  [WARN] SPOT CHECK {target}/{exchange}: could not run ({e}) -- skipped this cycle.")
        return None
