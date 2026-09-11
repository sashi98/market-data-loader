# listeners/technicals/rsi14m_runner.py
#
# rsi14m's own runner -- STEP 8 of the 2026-08-30 scheduler redesign.
# Identical shape to rsi14w_runner.py, just pointed at the monthly
# source/target tables -- see that file's own header for the full
# reasoning (why no incremental mode, why start_date/end_date aren't
# used to bound the calculation).

from core.rsi.rsi_calculator import compute_rsi14_all
from core.rsi.rsi_persistence import fetch_period_closes, upsert_rsi_workbook
from core.indicators.persistence import record_success, record_failure
from core.date_format import fmt_date

SOURCE_TABLE = "bhav_copy_m"
TARGET_TABLE = "rsi14m_workbook"
EXCHANGES = ["NSE", "BSE"]


def run(conn, indicator_id, start_date, end_date):
    """Full recompute of rsi14m_workbook -- see rsi14w_runner.run()'s own docstring."""
    print(f"  [{indicator_id}] Full recompute over {SOURCE_TABLE} "
          f"(propagated range this cycle: {fmt_date(start_date)} to {fmt_date(end_date)}).")

    try:
        total_written = 0
        for exchange in EXCHANGES:
            df = fetch_period_closes(conn, SOURCE_TABLE, exchange)
            if df.empty:
                continue
            rsi_df = compute_rsi14_all(df, calendar_index=None)
            total_written += upsert_rsi_workbook(conn, TARGET_TABLE, rsi_df)
    except Exception as e:
        print(f"  [{indicator_id}] FAILED: {e}")
        record_failure(conn, indicator_id, end_date, str(e))
        print(f"  [{indicator_id}] Flipped to DEACTIVE -- stopping.")
        return False

    with conn.cursor() as cur:
        cur.execute(f"SELECT MAX(trade_date) FROM {TARGET_TABLE}")
        new_max_date = cur.fetchone()[0]
    if new_max_date is not None:
        record_success(conn, indicator_id, new_max_date)

    print(f"  [{indicator_id}] Full recompute complete -- {total_written} rows written. "
          f"Latest date now {fmt_date(new_max_date)}.")
    return True
