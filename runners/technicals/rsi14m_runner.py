# listeners/technicals/rsi14m_runner.py
#
# rsi14m's own runner -- STEP 8 of the 2026-08-30 scheduler redesign.
# Identical shape to rsi14w_runner.py, just pointed at the monthly
# source/target tables -- see that file's own header for the full
# reasoning (why no incremental mode, why start_date/end_date aren't
# used to bound the calculation).
#
# MODE OF RECOMPUTE 2026-11-03 (TMT-MDL-US-0005-003) -- run() now takes mode_of_recompute (FULL | SHADOW |
# INCREMENTAL) and delegates to core/rsi/rsi_recompute.py, shared by all three RSI runners. FULL is the
# behaviour described above, unchanged.

from core.indicators.persistence import record_success, record_failure
from core.date_format import fmt_date
from core.recompute import mode as recompute_mode
from core.recompute import queue as recompute_queue
from core.rsi import rsi_recompute

SOURCE_TABLE = "bhav_copy_m"
TARGET_TABLE = "rsi14m_workbook"

CONFIG = {
    "source_table": SOURCE_TABLE,
    "target_table": TARGET_TABLE,
    "target": recompute_queue.TARGET_RSI14M,
    "use_calendar": False,
}


def run(conn, indicator_id, start_date, end_date, mode_of_recompute=recompute_mode.FULL):
    """
    Recomputes rsi14m_workbook for both exchanges in the given mode_of_recompute (see
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
