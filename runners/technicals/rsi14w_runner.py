# listeners/technicals/rsi14w_runner.py
#
# rsi14w's own runner -- STEP 8 of the 2026-08-30 scheduler redesign.
# Same "different runners for different indicators" split as
# rsi14d_runner.py's own header describes; see that file for the full
# framing. Unlike rsi14d, rsi14w has no incremental mode -- it always
# full-recomputes its entire target table from its entire source table
# in one call, same "always full recompute, every run" contract every
# other loader in this repo already uses (this is exactly what the old
# core/indicators/dispatch.py's _make_rsi_full_recompute_handlers()
# built for rsi14w/rsi14m; that generic factory is gone, this file is
# its rsi14w-specific replacement).
#
# start_date/end_date (propagated from STEP 2, see rsi14d_runner.py's
# own header) are NOT used to bound this computation -- confirmed with
# Sashikant 2026-08-30: bounding a moving-average-style calculation to
# a short window would need reading back extra lookback context to seed
# it correctly, real behavior change from every existing loader's math.
# They're accepted as parameters purely for the log line below, keeping
# every runner's call signature identical regardless of mode.
#
# MODE OF RECOMPUTE 2026-11-03 (TMT-MDL-US-0005-003) -- run() now takes mode_of_recompute (FULL | SHADOW |
# INCREMENTAL) and delegates to core/rsi/rsi_recompute.py, shared by all three RSI runners. FULL is the
# behaviour described above, unchanged.

from core.indicators.persistence import record_success, record_failure
from core.date_format import fmt_date
from core.recompute import mode as recompute_mode
from core.recompute import queue as recompute_queue
from core.rsi import rsi_recompute

SOURCE_TABLE = "bhav_copy_w"
TARGET_TABLE = "rsi14w_workbook"

CONFIG = {
    "source_table": SOURCE_TABLE,
    "target_table": TARGET_TABLE,
    "target": recompute_queue.TARGET_RSI14W,
    "use_calendar": False,
}


def run(conn, indicator_id, start_date, end_date, mode_of_recompute=recompute_mode.FULL):
    """
    Recomputes rsi14w_workbook for both exchanges in the given mode_of_recompute (see
    core/rsi/rsi_recompute.py). Returns True on success, False on failure (record_failure() already
    called before returning).
    """
    mode = recompute_mode.parse_mode(mode_of_recompute)
    print(f"  [{indicator_id}] {mode} recompute over {SOURCE_TABLE} "
          f"(range this cycle: {fmt_date(start_date)} to {fmt_date(end_date)}).")
    try:
        total_written = rsi_recompute.run(conn, CONFIG, indicator_id, start_date, end_date, mode)
    except Exception as e:
        try:
            conn.rollback()
        except Exception:
            pass
        print(f"  [{indicator_id}] FAILED: {e}")
        record_failure(conn, indicator_id, end_date, str(e))
        print(f"  [{indicator_id}] Flipped to DEACTIVE -- stopping.")
        return False

    with conn.cursor() as cur:
        cur.execute(f"SELECT MAX(trade_date) FROM {TARGET_TABLE}")
        new_max_date = cur.fetchone()[0]
    if new_max_date is not None:
        record_success(conn, indicator_id, new_max_date)

    print(f"  [{indicator_id}] {mode} recompute complete -- {total_written} rows written. "
          f"Latest date now {fmt_date(new_max_date)}.")
    return True
