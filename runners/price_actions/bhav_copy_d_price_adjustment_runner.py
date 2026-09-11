# runners/price_actions/bhav_copy_d_price_adjustment_runner.py
#
# STEP 4 -- Price Adjustment Runner, for ONE exchange. Rebuilds
# bhav_copy_adjusted -- the single stabilized, continuous,
# split/bonus-adjusted, isin-lineage-bridged price series every
# price-based indicator reads from -- as a FULL recompute over the
# entire bhav_copy history for this exchange, every time this runs.
# Not bounded to [start_date, end_date] in its own fetch (a newly
# discovered corporate action can retroactively change every
# historical row, so anything less than a full recompute risks leaving
# stale unadjusted prices behind) -- start_date/end_date are accepted
# here purely so this runner's call signature matches every other
# blind-upsert runner's (env_values, exchange, start_date, end_date)
# shape; by the time the scheduler calls this, start_date is always
# DEFAULT_START_DATE and end_date is always LATEST_TRADE_DATE, i.e.
# "recompute everything there is."
#
# BLIND-UPSERT REDESIGN 2026-09-06 -- the internal
# is_fresh_through(conn, ceiling_date) freshness check (both exchanges
# together) is REMOVED. Per Sashikant's own confirmed call (review
# point 5): the scheduler now checks bhav_copy_adjusted_metadata's own
# per-exchange freshness itself (alongside the per-exchange bhav_copy
# gap) and decides skip-vs-run BEFORE calling this module at all -- see
# bhavcopy_scheduler_main.py's STEP 4 block. This module is now a pure
# "recompute this ONE exchange's whole adjusted series, upsert" blind
# executor with no freshness decision of its own.
#
# Single exchange per call now (was a `for exchange in EXCHANGES:` loop
# internally) -- the scheduler calls this once per exchange, same as
# STEP 2/3.
#
# ALSO FIXED (carried over from the 2026-08-30 extraction): raises
# PriceAdjustmentError on failure rather than calling sys.exit(1) --
# the caller (bhavcopy_scheduler_main.py) decides what to do about it.

import gc
import time

from core.db_client import get_connection, DbConnectionError
from core.date_format import fmt_date
from core.price_series.adjusted_series import (
    fetch_isin_lineage_map, fetch_matched_corporate_actions, fetch_raw_bhav_copy,
    build_adjusted_series, upsert_bhav_copy_adjusted, upsert_bhav_copy_adjusted_metadata,
    AdjustedSeriesError,
)


class PriceAdjustmentError(Exception):
    """Raised on any hard failure rebuilding bhav_copy_adjusted -- DB connection, fetch, build, or upsert."""
    pass


def _fetch_raw_bhav_copy_for_exchange(conn, exchange):
    try:
        df = fetch_raw_bhav_copy(conn, exchange)
    except AdjustedSeriesError as e:
        raise PriceAdjustmentError(f"Failed to fetch raw bhav_copy for {exchange}: {e}")

    row_count = len(df)
    isin_count = df["isin"].nunique() if row_count else 0

    if row_count == 0:
        print(f"  [WARNING] No rows found for exchange={exchange} -- skipping.")
        date_min = date_max = None
    else:
        date_min = df["trade_date"].min()
        date_max = df["trade_date"].max()
        print(f"  [OK] Fetched {row_count} rows across {isin_count} isins")
        print(f"  Date range: {date_min} to {date_max}")

    return {"df": df, "row_count": row_count, "isin_count": isin_count,
            "date_min": date_min, "date_max": date_max}


def _build_adjusted_series_for_exchange(df, exchange, actions_df, lineage_map):
    adjusted_df = build_adjusted_series(df, actions_df, lineage_map)
    if not adjusted_df.empty:
        adjusted_df["exchange"] = exchange

    security_count = adjusted_df["security_id"].nunique() if not adjusted_df.empty else 0
    bridged_count = (
        adjusted_df.loc[adjusted_df["security_id"] != adjusted_df["source_isin"], "security_id"].nunique()
        if not adjusted_df.empty else 0
    )
    adjusted_row_count = (
        int((adjusted_df["adjustment_factor_applied"] != 1.0).sum()) if not adjusted_df.empty else 0
    )

    print(f"  [OK] Built {len(adjusted_df)} rows across {security_count} securities")
    print(f"  Securities with an isin-lineage bridge applied: {bridged_count}")
    print(f"  Rows with a non-1.0 adjustment factor applied:  {adjusted_row_count}")

    return {"adjusted_df": adjusted_df, "security_count": security_count,
            "bridged_count": bridged_count, "adjusted_row_count": adjusted_row_count}


def _upsert_adjusted_series_for_exchange(conn, adjusted_df, exchange):
    try:
        written_count = upsert_bhav_copy_adjusted(conn, adjusted_df)
    except AdjustedSeriesError as e:
        raise PriceAdjustmentError(f"Failed to upsert bhav_copy_adjusted for {exchange}: {e}")

    print(f"  [OK] Upserted {written_count} rows into bhav_copy_adjusted for {exchange}")
    return {"written_count": written_count}


def run(env_values, exchange, start_date, end_date):
    """
    STEP 4 -- Price Adjustment Runner, for ONE exchange. Blind full
    recompute of bhav_copy_adjusted -- no freshness check (see this
    module's own header). Raises PriceAdjustmentError on any hard
    failure -- caller decides how to handle it.
    """
    print(f"STEP 4 -- Price Adjustment Runner [{exchange}] (full recompute, {fmt_date(start_date)} to {fmt_date(end_date)})")

    try:
        conn = get_connection(env_values)
    except DbConnectionError as e:
        raise PriceAdjustmentError(f"Could not connect to DB: {e}")

    run_started_at = time.time()
    try:
        print("\n[4.1] Fetching security_identity_lineage and MATCHED corporate actions ...")
        try:
            lineage_map = fetch_isin_lineage_map(conn)
            actions_df = fetch_matched_corporate_actions(conn)
        except AdjustedSeriesError as e:
            raise PriceAdjustmentError(f"Failed to fetch lineage/actions: {e}")
        print(f"  [OK] {len(lineage_map)} lineage bridge(s), {len(actions_df)} MATCHED corporate action(s) loaded.")

        try:
            print(f"\n[4.2] Fetching raw bhav_copy ({exchange} only, continuity-eligible rows) ...")
            fetch_context = _fetch_raw_bhav_copy_for_exchange(conn, exchange)

            if fetch_context["row_count"] == 0:
                result = {**fetch_context, "security_count": 0, "bridged_count": 0,
                          "adjusted_row_count": 0, "written_count": 0}
                print(f"\nSTEP 4 [{exchange}] complete -- nothing to write.")
                return {"outcome": "OK", "exchange": exchange, "result": result}

            print(f"\n[4.3] Building adjusted series for {exchange} (lineage bridge + split/bonus adjustment) ...")
            build_context = _build_adjusted_series_for_exchange(
                fetch_context["df"], exchange, actions_df, lineage_map
            )

            print(f"\n[4.4] Upserting {exchange} rows into bhav_copy_adjusted ...")
            upsert_context = _upsert_adjusted_series_for_exchange(conn, build_context["adjusted_df"], exchange)

            result = {
                "row_count": fetch_context["row_count"],
                "date_min": fetch_context["date_min"],
                "date_max": fetch_context["date_max"],
                "security_count": build_context["security_count"],
                "bridged_count": build_context["bridged_count"],
                "adjusted_row_count": build_context["adjusted_row_count"],
                "written_count": upsert_context["written_count"],
            }

            processing_time_ms = int((time.time() - run_started_at) * 1000)
            try:
                upsert_bhav_copy_adjusted_metadata(
                    conn, exchange, "SUCCESS", fetch_context["date_max"],
                    upsert_context["written_count"], processing_time_ms,
                )
            except AdjustedSeriesError as e:
                print(f"  [WARN] {exchange} recompute succeeded, but bhav_copy_adjusted_metadata write failed: {e}")

            del fetch_context, build_context, upsert_context
            gc.collect()

            print(f"\nSTEP 4 [{exchange}] complete -- {result['written_count']} row(s) written.")
            return {"outcome": "OK", "exchange": exchange, "result": result}
        except PriceAdjustmentError as e:
            processing_time_ms = int((time.time() - run_started_at) * 1000)
            try:
                upsert_bhav_copy_adjusted_metadata(conn, exchange, "FAILED", None, 0, processing_time_ms, str(e))
            except AdjustedSeriesError:
                pass  # don't let a metadata-write failure mask the real error below
            raise
    finally:
        conn.close()
