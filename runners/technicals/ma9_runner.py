# runners/technicals/ma9_runner.py
#
# ma9d/ma9w/ma9m's own runner -- STEP 8 of the scheduler. One runner
# module per MA WINDOW (9/50/200), each handling its own 3 timeframes
# internally via _CONFIG below.
#
# FULL RECOMPUTE -- Sashikant's explicit choice: accuracy over
# incremental. Recomputes the indicator's ENTIRE history (from the
# passed-in start_date through end_date) every single run -- a
# corporate action discovered today can retroactively change adjustment
# factors for old dates, so an incremental recompute could leave stale
# MA values behind for dates outside a narrower window.
#
# BLIND-UPSERT REDESIGN 2026-09-06 -- CLEANUP: this used to hardcode
# its own local BACKFILL_START_DATE = date(2024, 1, 1) and silently
# IGNORE the propagated start_date parameter entirely. Now that
# bhavcopy_scheduler_main.py owns the single DEFAULT_START_DATE anchor
# for every full-recompute step (STEP 4/5/6/7/8 alike) and always
# passes it straight through as this function's own start_date, there
# is no reason for this module to keep a second, independent copy of
# the same date -- using the passed-in start_date directly keeps this
# runner consistent with every other one, and means a future change to
# DEFAULT_START_DATE only has to happen in one place.
#
# Done in CHUNK_SIZE_DAYS-sized date-range batches, NOT one single
# fetch spanning the whole history -- a literal unbounded full-history
# fetch is exactly what crashed Postgres on an earlier design (see this
# module's git history, "second pass"). Each chunk still goes through
# fetch_closes_for_ma()'s own bounded-plus-lookback SQL, just called
# repeatedly across the whole history instead of once for a single
# day's delta -- memory stays bounded to one chunk's worth of data at a
# time, regardless of how many years of history accumulate.

from datetime import timedelta

from core.ma.ma_calculator import compute_ma
from core.ma.ma_persistence import fetch_closes_for_ma, upsert_ma_workbook
from core.indicators.persistence import record_success, record_failure
from core.date_format import fmt_date

WINDOW = 9
COLUMN_NAME = "MA9"
EXCHANGES = ["NSE", "BSE"]

CHUNK_SIZE_DAYS = 180  # bounds each fetch_closes_for_ma() call's own row
                        # count, even though the OVERALL recompute spans
                        # the whole [start_date, end_date] every run.

# indicator_id -> (source_table, target_table) -- see the requirement's
# own Indicator Data Source Mapping / Indicator's Workbook Mapping.
_CONFIG = {
    "ma9d": ("bhav_copy_adjusted", "ma9d_workbook"),
    "ma9w": ("bhav_copy_w", "ma9w_workbook"),
    "ma9m": ("bhav_copy_m", "ma9m_workbook"),
}


def run(conn, indicator_id, start_date, end_date):
    """
    FULL recompute of indicator_id's own MA9 workbook table, from
    start_date through end_date -- walked in CHUNK_SIZE_DAYS-sized
    batches rather than one fetch (see this module's own header).
    """
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
                    # Lookback rows (before chunk_start) were only fetched to
                    # seed the rolling window -- trim back out before
                    # upserting this chunk's own [chunk_start, chunk_end].
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
