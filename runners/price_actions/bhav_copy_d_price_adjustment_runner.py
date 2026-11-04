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
# MODE OF RECOMPUTE 2026-11-03 (TMT-MDL-US-0005) -- run() now takes mode_of_recompute:
#   FULL         as before: every stock's whole history is rebuilt and written.
#   INCREMENTAL  STEP 3A (core/price_series/classification.py) splits the stocks: list-B stocks (history
#                changed -- corporate action, new stock, back-dated data, forced, unfinished previous run) are
#                rebuilt in full exactly as before; list-A stocks only get the rows after their bookmark (their
#                last stored trade_date).
#   SHADOW       FULL is written; the INCREMENTAL rows for list-A are also computed (in memory, before FULL
#                runs) and compared with what FULL wrote -> recompute_shadow_check + a verdict line.
# In every mode the list-B stocks are queued in recompute_queue for the downstream steps (weekly / monthly
# rollups), so they rebuild exactly those stocks. bhav_copy_adjusted_metadata.run_status is RUNNING while
# the step runs: a run that dies midway leaves RUNNING, and the next run then treats the whole exchange as
# list-B (STATE_INCONSISTENT).
#
# ALSO FIXED (carried over from the 2026-08-30 extraction): raises
# PriceAdjustmentError on failure rather than calling sys.exit(1) --
# the caller (bhavcopy_scheduler_main.py) decides what to do about it.
#
# CHUNKED 2026-09-13 -- fetch_raw_bhav_copy()/build_adjusted_series()
# used to run ONCE per exchange over the entire eligible history
# (potentially 1.8-2.7M rows in one pandas DataFrame) -- that unchunked
# fetch+build, not the (already-chunked) final upsert, is what actually
# caused the 2026-09-12/13 silent OOM crash. Now planned as security-id
# batches (see core/price_series/adjusted_series.py's
# chunk_isins_by_security() for why the boundary must be security_id,
# never a date range) and run one batch at a time: fetch -> build ->
# upsert -> discard -> gc.collect(), so at most one batch's worth of
# rows is ever held in memory. Per-batch stats are summed across
# batches for the final result/metadata write -- safe because the
# batches partition every security_id exactly once (see that
# function's own docstring).

import gc
import time

from core.db_client import get_connection, DbConnectionError
from core.date_format import fmt_date
from core.price_series.adjusted_series import (
    fetch_isin_lineage_map, fetch_matched_corporate_actions, fetch_raw_bhav_copy, fetch_raw_bhav_copy_from,
    fetch_distinct_isins, chunk_isins_by_security, resolve_security_id,
    build_adjusted_series, upsert_bhav_copy_adjusted, upsert_bhav_copy_adjusted_metadata,
    AdjustedSeriesError,
)
from core.price_series import classification
from core.recompute import mode as recompute_mode
from core.recompute import queue as recompute_queue
from core.recompute import shadow
import pandas as pd


class PriceAdjustmentError(Exception):
    """Raised on any hard failure rebuilding bhav_copy_adjusted -- DB connection, fetch, build, or upsert."""
    pass


def _fetch_raw_bhav_copy_for_exchange(conn, exchange, isins):
    try:
        df = fetch_raw_bhav_copy(conn, exchange, isins)
    except AdjustedSeriesError as e:
        raise PriceAdjustmentError(f"Failed to fetch raw bhav_copy for {exchange}: {e}")

    row_count = len(df)
    isin_count = df["isin"].nunique() if row_count else 0

    if row_count == 0:
        print(f"  [WARNING] No rows found for this batch (exchange={exchange}) -- skipping.")
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


PREVIOUS_RUN_SQL = "SELECT run_status FROM bhav_copy_adjusted_metadata WHERE exchange = %s"

ACTUAL_ROWS_SQL = """
    SELECT a.security_id, a.trade_date, a.open, a.high, a.low, a.close, a.last, a.prev_close,
           a.tot_trd_qty, a.adjustment_factor_applied
      FROM bhav_copy_adjusted a
      JOIN unnest(%(sids)s::text[], %(dates)s::date[]) AS k(security_id, trade_date)
        ON k.security_id = a.security_id AND k.trade_date = a.trade_date
     WHERE a.exchange = %(exchange)s
"""

SHADOW_VALUE_COLUMNS = ["open", "high", "low", "close", "last", "prev_close", "tot_trd_qty", "adjustment_factor_applied"]
SHADOW_TOLERANCES = {"open": 0.0001, "high": 0.0001, "low": 0.0001, "close": 0.0001, "last": 0.0001,
                     "prev_close": 0.0001, "tot_trd_qty": 1.0, "adjustment_factor_applied": 1e-6}
LIST_B_DETAIL_LINES = 50


def _previous_run_ok(conn, exchange):
    """False when the last STEP 4 run for this exchange did not finish (RUNNING / FAILED)."""
    try:
        with conn.cursor() as cur:
            cur.execute(PREVIOUS_RUN_SQL, (exchange,))
            row = cur.fetchone()
    except Exception as e:
        raise PriceAdjustmentError(f"Failed to read bhav_copy_adjusted_metadata for {exchange}: {e}")
    return row is None or row[0] == "SUCCESS"


def _classify(conn, exchange, isins, lineage_map, actions_df):
    """STEP 3A for this exchange -> (list_a {sid: bookmark}, list_b {sid: reason}, isins_by_security)."""
    isins_by_security = {}
    for isin in isins:
        isins_by_security.setdefault(resolve_security_id(isin, lineage_map), []).append(isin)

    previous_ok = _previous_run_ok(conn, exchange)
    try:
        queued = recompute_queue.fetch_queued(conn, exchange, recompute_queue.TARGET_PRICE_ADJUSTMENT)
    except recompute_queue.RecomputeQueueError as e:
        raise PriceAdjustmentError(str(e))
    if recompute_queue.ALL in queued:
        previous_ok = False
    try:
        stored = classification.fetch_stored_state(conn, exchange)
        bookmarks = {sid: st["last_date"] for sid, st in stored.items() if sid in isins_by_security}
        raw_days = classification.fetch_raw_days_up_to_bookmark(conn, exchange, isins_by_security, bookmarks) \
            if previous_ok else {}
    except AdjustedSeriesError as e:
        raise PriceAdjustmentError(str(e))
    ca_by_security = classification.actions_by_security(actions_df, lineage_map)
    list_a, list_b = classification.classify(isins_by_security, stored, ca_by_security, raw_days,
                                             forced_security_ids=queued.keys(), previous_run_ok=previous_ok)
    print(f"  [3A] {exchange}: {classification.summarize(list_a, list_b)}"
          + ("" if previous_ok else "  -- previous run did not finish (or a full recompute was forced): whole exchange rebuilt"))
    if previous_ok:
        for n, (sid, reason) in enumerate(sorted(list_b.items(), key=lambda kv: (kv[1], kv[0]))):
            if n >= LIST_B_DETAIL_LINES:
                print(f"       ... and {len(list_b) - LIST_B_DETAIL_LINES} more")
                break
            print(f"       list-B {sid} ({', '.join(isins_by_security.get(sid, []))}): {reason}")
    return list_a, list_b, isins_by_security, previous_ok, queued


def _incremental_rows(conn, exchange, list_a, isins_by_security, actions_df, lineage_map, isin_batch_size=2000):
    """
    list-A rows after each stock's bookmark, built exactly like FULL builds them (same build_adjusted_series,
    same actions): the bookmark row itself is fetched again so the first new row's prev_close comes from it,
    then dropped. Returns one DataFrame (exchange column set).
    """
    from_date_by_isin = {isin: bookmark for sid, bookmark in list_a.items() for isin in isins_by_security.get(sid, [])}
    isins = list(from_date_by_isin.keys())
    frames = []
    for i in range(0, len(isins), isin_batch_size):
        chunk = {isin: from_date_by_isin[isin] for isin in isins[i:i + isin_batch_size]}
        try:
            raw = fetch_raw_bhav_copy_from(conn, exchange, chunk)
        except AdjustedSeriesError as e:
            raise PriceAdjustmentError(str(e))
        if raw.empty:
            continue
        built = build_adjusted_series(raw, actions_df, lineage_map)
        if built.empty:
            continue
        bookmark = pd.to_datetime(built["security_id"].map(list_a))
        new_rows = built[bookmark.notna() & (pd.to_datetime(built["trade_date"]) > bookmark)].copy()
        if not new_rows.empty:
            new_rows["exchange"] = exchange
            frames.append(new_rows)
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def _full_rebuild(conn, exchange, isins, actions_df, lineage_map, label):
    """Rebuild + upsert the whole history of the stocks behind `isins` (the pre-TMT-MDL-US-0005 loop)."""
    totals = {"row_count": 0, "security_count": 0, "bridged_count": 0, "adjusted_row_count": 0,
              "written_count": 0, "date_min": None, "date_max": None, "batches": 0}
    if not isins:
        return totals
    batches = chunk_isins_by_security(isins, lineage_map)
    totals["batches"] = len(batches)
    print(f"  [{label}] {len(isins)} isin(s) -> {len(batches)} batch(es) of up to "
          f"{max((len(b) for b in batches), default=0)} isin(s) each")
    for batch_num, batch_isins in enumerate(batches, start=1):
        print(f"\n[4.3.{batch_num}/{len(batches)}] Fetching raw bhav_copy for this batch "
              f"({exchange}, {len(batch_isins)} isin(s)) ...")
        fetch_context = _fetch_raw_bhav_copy_for_exchange(conn, exchange, batch_isins)
        if fetch_context["row_count"] == 0:
            del fetch_context
            continue
        print(f"  Building adjusted series (lineage bridge + split/bonus adjustment) ...")
        build_context = _build_adjusted_series_for_exchange(fetch_context["df"], exchange, actions_df, lineage_map)
        print(f"  Upserting into bhav_copy_adjusted ...")
        upsert_context = _upsert_adjusted_series_for_exchange(conn, build_context["adjusted_df"], exchange)
        totals["row_count"] += fetch_context["row_count"]
        totals["security_count"] += build_context["security_count"]
        totals["bridged_count"] += build_context["bridged_count"]
        totals["adjusted_row_count"] += build_context["adjusted_row_count"]
        totals["written_count"] += upsert_context["written_count"]
        if fetch_context["date_min"] is not None:
            totals["date_min"] = fetch_context["date_min"] if totals["date_min"] is None \
                else min(totals["date_min"], fetch_context["date_min"])
            totals["date_max"] = fetch_context["date_max"] if totals["date_max"] is None \
                else max(totals["date_max"], fetch_context["date_max"])
        del fetch_context, build_context, upsert_context
        gc.collect()
    return totals


def _shadow_compare(conn, exchange, candidate, cycle_trade_date, list_a, list_b):
    """Compare the in-memory INCREMENTAL rows with what FULL just wrote; record + print the verdict."""
    if candidate.empty:
        actual = pd.DataFrame()
    else:
        try:
            actual = pd.read_sql(ACTUAL_ROWS_SQL, conn, params={
                "exchange": exchange,
                "sids": candidate["security_id"].astype(str).tolist(),
                "dates": candidate["trade_date"].tolist(),
            })
        except Exception as e:
            print(f"  [WARN] SHADOW price adjustment/{exchange}: could not read FULL rows to compare: {e}")
            return
    result = shadow.compare(candidate, actual, ["security_id", "trade_date"], SHADOW_VALUE_COLUMNS, SHADOW_TOLERANCES)
    line = shadow.record(conn, cycle_trade_date, recompute_mode.PRICE_ADJUSTMENT, exchange, result,
                         list_a_count=len(list_a), list_b_count=len(list_b))
    print(f"  {line}")


def run(env_values, exchange, start_date, end_date, mode_of_recompute=recompute_mode.FULL):
    """
    STEP 4 -- Price Adjustment Runner, for ONE exchange, in the given mode_of_recompute (see this module's
    header). Raises PriceAdjustmentError on any hard failure -- caller decides how to handle it.
    Returns {"outcome", "exchange", "mode_of_recompute", "result", "list_a_count", "list_b"}.
    """
    mode = recompute_mode.parse_mode(mode_of_recompute)
    print(f"STEP 4 -- Price Adjustment Runner [{exchange}] (mode_of_recompute={mode}, "
          f"{fmt_date(start_date)} to {fmt_date(end_date)})")

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
            print(f"\n[4.2] Planning {exchange} (continuity-eligible isins, STEP 3A classification) ...")
            try:
                isins = fetch_distinct_isins(conn, exchange)
            except AdjustedSeriesError as e:
                raise PriceAdjustmentError(f"Failed to fetch distinct isins for {exchange}: {e}")

            if not isins:
                print(f"  [WARNING] No eligible isins found for exchange={exchange} -- skipping.")
                processing_time_ms = int((time.time() - run_started_at) * 1000)
                try:
                    upsert_bhav_copy_adjusted_metadata(conn, exchange, "SUCCESS", None, 0, processing_time_ms)
                except AdjustedSeriesError as e:
                    print(f"  [WARN] {exchange} run succeeded, but bhav_copy_adjusted_metadata write failed: {e}")
                return {"outcome": "OK", "exchange": exchange, "mode_of_recompute": mode,
                        "result": {"written_count": 0}, "list_a_count": 0, "list_b": {}}

            list_a, list_b, isins_by_security, previous_ok, queued = _classify(
                conn, exchange, isins, lineage_map, actions_df)

            # From here a crash leaves RUNNING behind -> next run rebuilds the whole exchange.
            try:
                upsert_bhav_copy_adjusted_metadata(conn, exchange, "RUNNING", None, 0, None)
            except AdjustedSeriesError as e:
                raise PriceAdjustmentError(f"Could not mark {exchange} RUNNING: {e}")

            candidate = pd.DataFrame()
            if mode == recompute_mode.SHADOW and previous_ok and list_a:
                print(f"\n[4.S] SHADOW: computing INCREMENTAL rows for {len(list_a):,} list-A stock(s) (in memory) ...")
                candidate = _incremental_rows(conn, exchange, list_a, isins_by_security, actions_df, lineage_map)
                print(f"  [OK] {len(candidate):,} new row(s) computed.")

            if mode in (recompute_mode.FULL, recompute_mode.SHADOW) or not previous_ok:
                totals = _full_rebuild(conn, exchange, isins, actions_df, lineage_map, "FULL")
                incremental_written = 0
            else:
                b_isins = [isin for sid in list_b for isin in isins_by_security.get(sid, [])]
                totals = _full_rebuild(conn, exchange, b_isins, actions_df, lineage_map, "list-B FULL")
                print(f"\n[4.4] INCREMENTAL: rows after the bookmark for {len(list_a):,} list-A stock(s) ...")
                new_rows = _incremental_rows(conn, exchange, list_a, isins_by_security, actions_df, lineage_map)
                incremental_written = _upsert_adjusted_series_for_exchange(conn, new_rows, exchange)["written_count"] \
                    if not new_rows.empty else 0
                if not new_rows.empty:
                    nr_max = new_rows["trade_date"].max()
                    totals["date_max"] = nr_max if totals["date_max"] is None else max(totals["date_max"], nr_max)
                if list_a:
                    a_max = max(list_a.values())
                    totals["date_max"] = a_max if totals["date_max"] is None else max(totals["date_max"], a_max)
                print(f"  [OK] {incremental_written:,} list-A row(s) written.")

            written = totals["written_count"] + incremental_written

            if mode == recompute_mode.SHADOW and previous_ok:
                _shadow_compare(conn, exchange, candidate, end_date, list_a, list_b)

            # Downstream steps rebuild exactly the stocks whose adjusted history was rebuilt.
            downstream = {recompute_queue.ALL: classification.STATE_INCONSISTENT} if not previous_ok else list_b
            try:
                queued_n = recompute_queue.enqueue(conn, exchange, recompute_queue.DOWNSTREAM_OF_PRICE_ADJUSTMENT,
                                                   downstream, detail=f"STEP 4 {fmt_date(end_date)}")
                if queued:
                    recompute_queue.clear(conn, exchange, recompute_queue.TARGET_PRICE_ADJUSTMENT, queued.keys())
            except recompute_queue.RecomputeQueueError as e:
                raise PriceAdjustmentError(f"Could not queue list-B stocks for the rollups: {e}")
            if queued_n:
                print(f"  [OK] Queued {len(downstream):,} stock(s) for a full rebuild in the weekly / monthly rollups and rsi14d (rsi14w / rsi14m via the rollups).")

            processing_time_ms = int((time.time() - run_started_at) * 1000)
            try:
                upsert_bhav_copy_adjusted_metadata(conn, exchange, "SUCCESS", totals["date_max"], written, processing_time_ms)
            except AdjustedSeriesError as e:
                print(f"  [WARN] {exchange} run succeeded, but bhav_copy_adjusted_metadata write failed: {e}")

            print(f"\nSTEP 4 [{exchange}] complete ({mode}) -- {written:,} row(s) written "
                  f"({classification.summarize(list_a, list_b)}).")
            totals["written_count"] = written
            return {"outcome": "OK", "exchange": exchange, "mode_of_recompute": mode, "result": totals,
                    "list_a_count": len(list_a), "list_b": list_b}
        except PriceAdjustmentError as e:
            processing_time_ms = int((time.time() - run_started_at) * 1000)
            try:
                upsert_bhav_copy_adjusted_metadata(conn, exchange, "FAILED", None, 0, processing_time_ms, str(e))
            except AdjustedSeriesError:
                pass  # don't let a metadata-write failure mask the real error below
            raise
    finally:
        conn.close()
