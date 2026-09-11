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

from core.rsi.rsi_calculator import compute_rsi14_all
from core.rsi.rsi_persistence import fetch_period_closes, upsert_rsi_workbook
from core.indicators.persistence import record_success, record_failure
from core.date_format import fmt_date

SOURCE_TABLE = "bhav_copy_w"
TARGET_TABLE = "rsi14w_workbook"
EXCHANGES = ["NSE", "BSE"]


def run(conn, indicator_id, start_date, end_date):
    """
    Full recompute of rsi14w_workbook from bhav_copy_w's entire history,
    both exchanges. Returns True on success (always -- there's no
    partial/incremental notion here), False on failure (record_failure()
    already called before returning).
    """
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
