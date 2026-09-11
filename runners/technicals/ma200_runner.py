# runners/technicals/ma200_runner.py
#
# ma200d/ma200w/ma200m's own runner -- STEP 8 of the scheduler.
# Identical shape to ma9_runner.py, just window=200 -- see that file's
# own header for the full reasoning, including the FULL RECOMPUTE
# design (accuracy over incremental, done in CHUNK_SIZE_DAYS-sized
# batches to stay memory-safe) and the 2026-09-06 CLEANUP (uses the
# passed-in start_date directly instead of a local hardcoded
# BACKFILL_START_DATE). Worth calling out here specifically: MA200's
# own lookback (199 rows/security) is the largest of the three windows,
# so it was also the single biggest contributor to the original
# unbounded-fetch crash this chunking was built to fix.

from datetime import timedelta

from core.ma.ma_calculator import compute_ma
from core.ma.ma_persistence import fetch_closes_for_ma, upsert_ma_workbook
from core.indicators.persistence import record_success, record_failure
from core.date_format import fmt_date

WINDOW = 200
COLUMN_NAME = "MA200"
EXCHANGES = ["NSE", "BSE"]

CHUNK_SIZE_DAYS = 180

_CONFIG = {
    "ma200d": ("bhav_copy_adjusted", "ma200d_workbook"),
    "ma200w": ("bhav_copy_w", "ma200w_workbook"),
    "ma200m": ("bhav_copy_m", "ma200m_workbook"),
}


def run(conn, indicator_id, start_date, end_date):
    """FULL recompute of indicator_id's own MA200 workbook table, from start_date through end_date -- see ma9_runner.run()'s own docstring."""
    source_table, target_table = _CONFIG[indicator_id]
    print(f"  [{indicator_id}] FULL recompute over {source_table}, "
          f"{fmt_date(start_date)} through {fmt_date(end_date)} (in {CHUNK_SIZE_DAYS}-day chunks).")

    try:
        total_written = 0
        for exchange in EXCHANGES:
            chunk_start = start_date
            while chunk_start <= end_date:
                chunk_end = min(chunk_start + timedelta(days=CHUNK_SIZE_DAYS - 1), end_date)
                df = fetch_closes_for_ma(conn, source_table, exchange, chunk_start, chunk_end, WINDOW)
                if not df.empty:
                    ma_df = compute_ma(df, WINDOW, COLUMN_NAME)
                    ma_df = ma_df[(ma_df["trade_date"] >= chunk_start) & (ma_df["trade_date"] <= chunk_end)]
                    if not ma_df.empty:
                        total_written += upsert_ma_workbook(conn, target_table, ma_df, COLUMN_NAME)
                chunk_start = chunk_end + timedelta(days=1)
    except Exception as e:
        print(f"  [{indicator_id}] FAILED: {e}")
        record_failure(conn, indicator_id, end_date, str(e))
        print(f"  [{indicator_id}] Flipped to DEACTIVE -- stopping.")
        return False

    with conn.cursor() as cur:
        cur.execute(f"SELECT MAX(trade_date) FROM {target_table}")
        new_max_date = cur.fetchone()[0]
    if new_max_date is not None:
        record_success(conn, indicator_id, new_max_date)

    print(f"  [{indicator_id}] FULL recompute complete -- {total_written} rows written. "
          f"Latest date now {fmt_date(new_max_date)}.")
    return True
