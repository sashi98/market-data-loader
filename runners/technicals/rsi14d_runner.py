# runners/technicals/rsi14d_runner.py
#
# rsi14d's own runner -- STEP 8 of the 2026-08-30 scheduler redesign.
#
# FULL RECOMPUTE 2026-08-30 (third pass) -- Sashikant's explicit
# choice: accuracy over incremental. This used to be the ONE genuinely
# different runner in this framework -- a real Wilder-average
# recursive walk, one trading day at a time, resuming from whatever
# avg_gain/avg_loss was last persisted (core/rsi/rsi_incremental.py).
# That's now bypassed entirely: this runner does the SAME "fetch the
# entire source table's history for one exchange, recompute the whole
# Wilder walk from scratch, upsert everything" full-recompute
# rsi14w_runner.py/rsi14m_runner.py already do -- just against
# bhav_copy_adjusted instead of bhav_copy_w/bhav_copy_m.
#
# Why this is safe from a memory standpoint despite bhav_copy_adjusted
# being a much bigger table than bhav_copy_w/bhav_copy_m: this is the
# EXACT SAME "fetch one exchange's entire bhav_copy_adjusted history
# via pd.read_sql()" pattern STEP 4's own price-adjustment runner
# already does successfully, every single run (confirmed live: BSE's
# full ~2.7M-row history fetched fine) -- RSI's own fetch needs FEWER
# columns (security_id/isin/exchange/series/symbol/trade_date/close/
# prev_close only, no open/high/low/last/qty/val/trades or adjustment-
# factor computation), so it's a lighter version of an already-proven
# workload, not a new one.
#
# UNLIKE rsi14w_runner.py's own call (calendar_index=None -- see that
# file's header), this DOES build and pass a real calendar_index
# (core/rsi/rsi_continuity.py's fetch_trading_calendar() +
# build_calendar_index()) -- rsi14d is specifically the indicator this
# whole gap-detection system was built to protect (the MEIL RSI-
# corruption bug, see rsi_calculator.py's own header): silently passing
# None here would DISABLE gap-reseed for daily RSI and risk
# reintroducing that exact bug class. Weekly/monthly's own
# calendar_index=None gap is a separate, pre-existing issue (period-
# level gap detection needs a period-level calendar, a genuinely
# different problem) -- not fixed here, out of scope for this change.
#
# core/rsi/rsi_incremental.py (the day-by-day Wilder walk this replaces)
# is left in place, unused by this runner now -- not deleted, since
# retiring it fully wasn't part of this change.
#
# start_date/end_date (propagated from STEP 2) are NOT used to bound
# this computation, same as rsi14w_runner.py/rsi14m_runner.py -- see
# those files' own header for why bounding a moving-average-style
# calculation needs lookback context to seed correctly, a real
# behavior change from full-recompute's own math. Accepted as
# parameters purely for the log line below, keeping every runner's
# call signature identical regardless of mode.
#
# MODE OF RECOMPUTE 2026-11-03 (TMT-MDL-US-0005-003) -- run() now takes mode_of_recompute (FULL | SHADOW |
# INCREMENTAL) and delegates to core/rsi/rsi_recompute.py, shared by all three RSI runners. FULL is the
# behaviour described above, unchanged.

from core.indicators.persistence import record_success, record_failure
from core.date_format import fmt_date
from core.recompute import mode as recompute_mode
from core.recompute import queue as recompute_queue
from core.rsi import rsi_recompute

SOURCE_TABLE = "bhav_copy_adjusted"
TARGET_TABLE = "rsi14d_workbook"

CONFIG = {
    "source_table": SOURCE_TABLE,
    "target_table": TARGET_TABLE,
    "target": recompute_queue.TARGET_RSI14D,
    "use_calendar": True,
}


def run(conn, indicator_id, start_date, end_date, mode_of_recompute=recompute_mode.FULL):
    """
    Recomputes rsi14d_workbook for both exchanges in the given mode_of_recompute (see
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
