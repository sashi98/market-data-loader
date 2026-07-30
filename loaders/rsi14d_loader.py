# loaders/rsi14d_loader.py
#
# market-data-loader -- RSI14 bulk backfill loader (Part 1 of the RSI
# scanner work, per the RSI scanner handover doc; Part 2 -- the
# event-driven daily path -- lives separately in the tmt Spring Boot app).
#
# FULL SCOPE --
#   Step 0: Load + validate config/.env, direct DB connectivity check.
#           No health check / auth step needed here (unlike
#           bhavcopy_loader) -- this script only ever talks to Postgres
#           directly, never to the TMT REST API.
#   Step 1: Fetch isin/exchange/series/symbol/trade_date/close from
#           bhav_copy, ONE EXCHANGE AT A TIME (NSE first, then BSE) --
#           see note below on why.
#   Step 2: Compute RSI14 (Wilder's smoothing, hand-implemented -- see
#           core/rsi/rsi_calculator.py for why pandas_ta.rma() was
#           rejected) per isin+exchange+series+symbol, for the current
#           exchange's rows only.
#   Step 3: Batch upsert gain/loss/avg_gain/avg_loss/rsi14 into
#           rsi14d_workbook for the current exchange, keyed on
#           (isin, exchange, series, symbol, trade_date), before moving
#           on to the next exchange.
#   Step 4: Combined summary report across all exchanges -- rows
#           processed, isins covered, date range, elapsed time.
#
# WHY PER-EXCHANGE, NOT ONE COMBINED PASS:
#   Fetching and computing RSI for NSE+BSE together (~4.36M rows,
#   ~15,700 isin+exchange+series+symbol combinations) was hitting real
#   memory pressure -- both a bare MemoryError during the Step 1 fetch
#   and a separate ArrayMemoryError in Step 2's summary code (since
#   fixed). Processing one exchange fully (fetch -> compute -> upsert)
#   before starting the next roughly halves peak memory, since the
#   other exchange's rows/computed frame are never resident at the same
#   time. The two exchanges' RSI series are already fully independent
#   (that's the whole point of EXCHANGE being part of the key), so
#   there's no correctness reason they need to be computed together --
#   this is a pure memory optimization, output is identical to a
#   combined pass.
#
# Always a FULL recompute over the entire bhav_copy history, every run,
# for each exchange -- no partial/incremental mode (Sashikant's call,
# matches the convergence math already documented: extending history
# backward needs a full recompute regardless).
#
# Exposes run() -- the standard entry point every loader under loaders/
# provides, so main.py can invoke it generically.
#
# Can also be run standalone for development/testing:
#   python rsi14d_loader.py

import gc
import sys
import time
from pathlib import Path

# Allow `from core.xxx import yyy` when run directly as a script
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from core.env_validator import load_and_validate_env, EnvValidationError
from core.db_client import get_connection, test_connection, DbConnectionError
from core.rsi.rsi_calculator import compute_rsi14_all
from core.rsi.rsi_persistence import fetch_bhav_copy_closes, upsert_rsi14d_workbook, RsiPersistenceError
from core.logging_setup import start_run_logging

EXCHANGES = ["NSE", "BSE"]


def step_0():
    print("=" * 60)
    print("  STEP 0 -- Environment validation, DB connectivity")
    print("=" * 60)

    print("\n[0.1] Validating config/.env ...")
    try:
        env_values = load_and_validate_env()
    except EnvValidationError as e:
        print(f"  [FAILED] {e}")
        sys.exit(1)
    print("  [OK] All required keys present.")

    print("\n[0.2] Checking direct DB connectivity ...")
    try:
        test_connection(env_values)
    except DbConnectionError as e:
        print(f"  [FAILED] {e}")
        sys.exit(1)
    print("  [OK] Connected to Postgres successfully.")

    print("\n" + "=" * 60)
    print("  STEP 0 COMPLETE")
    print("=" * 60)

    return {"env_values": env_values}


def step_1(conn, exchange):
    print("\n" + "=" * 60)
    print(f"  STEP 1 -- Fetch bhav_copy closes ({exchange} only, EQUITY series category)")
    print("=" * 60)

    try:
        df = fetch_bhav_copy_closes(conn, exchange)
    except RsiPersistenceError as e:
        print(f"  [FAILED] {e}")
        conn.close()
        sys.exit(1)

    isin_count = df["isin"].nunique()
    row_count = len(df)

    if row_count == 0:
        print(f"  [WARNING] No rows found for exchange={exchange} -- skipping.")
        date_min = date_max = None
        series_counts = {}
        combo_count = 0
    else:
        date_min = df["trade_date"].min()
        date_max = df["trade_date"].max()
        series_counts = df.groupby("series")["isin"].nunique().to_dict()
        combo_count = df.groupby(["isin", "series", "symbol"]).ngroups

        print(f"  [OK] Fetched {row_count} rows across {isin_count} isins "
              f"({combo_count} distinct isin+series+symbol combinations)")
        print(f"  Date range: {date_min} to {date_max}")
        print("  Isins by series:")
        for series, count in sorted(series_counts.items()):
            print(f"    {series:<8} {count}")

    print("\n" + "=" * 60)
    print(f"  STEP 1 COMPLETE ({exchange})")
    print("=" * 60)

    return {"df": df, "isin_count": isin_count, "row_count": row_count,
            "date_min": date_min, "date_max": date_max,
            "series_counts": series_counts, "combo_count": combo_count}


def step_2(df, exchange):
    print("\n" + "=" * 60)
    print(f"  STEP 2 -- Compute RSI14 (Wilder's smoothing) for {exchange}, per isin+series+symbol")
    print("=" * 60)

    combo_count = df.groupby(["isin", "series", "symbol"]).ngroups
    print(f"\n  Processing {combo_count} isin+series+symbol series for {exchange} ...")
    rsi_df = compute_rsi14_all(df)

    populated_count = rsi_df["rsi14"].notna().sum()
    pre_seed_count = len(rsi_df) - populated_count
    print(f"  [OK] Computed {len(rsi_df)} rows ({populated_count} with a non-NULL RSI14, "
          f"{pre_seed_count} pre-seed rows within each series' first 14 trading days)")

    print("\n" + "=" * 60)
    print(f"  STEP 2 COMPLETE ({exchange})")
    print("=" * 60)

    return {"rsi_df": rsi_df, "populated_count": populated_count, "pre_seed_count": pre_seed_count}


def step_3(conn, rsi_df, exchange):
    print("\n" + "=" * 60)
    print(f"  STEP 3 -- Upsert {exchange} rows into rsi14d_workbook")
    print("=" * 60)

    try:
        written_count = upsert_rsi14d_workbook(conn, rsi_df)
    except RsiPersistenceError as e:
        print(f"  [FAILED] {e}")
        conn.close()
        sys.exit(1)

    print(f"  [OK] Upserted {written_count} rows into rsi14d_workbook for {exchange}")

    print("\n" + "=" * 60)
    print(f"  STEP 3 COMPLETE ({exchange})")
    print("=" * 60)

    return {"written_count": written_count}


def step_4(per_exchange_results, elapsed_seconds):
    print("\n" + "=" * 60)
    print("  STEP 4 -- Combined summary report (all exchanges)")
    print("=" * 60)

    minutes, seconds = divmod(int(elapsed_seconds), 60)

    total_row_count = sum(r["row_count"] for r in per_exchange_results.values())
    total_isin_count = sum(r["isin_count"] for r in per_exchange_results.values())
    total_combo_count = sum(r["combo_count"] for r in per_exchange_results.values())
    total_populated = sum(r["populated_count"] for r in per_exchange_results.values())
    total_pre_seed = sum(r["pre_seed_count"] for r in per_exchange_results.values())
    total_written = sum(r["written_count"] for r in per_exchange_results.values())

    all_dates = [r["date_min"] for r in per_exchange_results.values() if r["date_min"] is not None] + \
                [r["date_max"] for r in per_exchange_results.values() if r["date_max"] is not None]
    date_min = min(all_dates) if all_dates else None
    date_max = max(all_dates) if all_dates else None

    rows_per_sec = total_row_count / elapsed_seconds if elapsed_seconds > 0 else 0

    print(f"\nDate range processed:                {date_min} to {date_max}")
    print(f"Isins processed (sum across exch.):  {total_isin_count}")
    print(f"Isin+exchange+series+symbol combos:  {total_combo_count}")
    print(f"Rows fetched:                        {total_row_count}")
    print(f"Rows with RSI14 value:               {total_populated}")
    print(f"Pre-seed rows (no RSI yet):          {total_pre_seed}")
    print(f"Rows written:                        {total_written}")

    print("\nPer-exchange breakdown:")
    for exchange, r in per_exchange_results.items():
        print(f"  {exchange}:")
        print(f"    Isins:                {r['isin_count']}")
        print(f"    Isin+series+symbol combos: {r['combo_count']}")
        print(f"    Rows fetched:         {r['row_count']}")
        print(f"    Rows with RSI14:      {r['populated_count']}")
        print(f"    Pre-seed rows:        {r['pre_seed_count']}")
        print(f"    Rows written:         {r['written_count']}")
        print("    Isins by series:")
        for series, count in sorted(r["series_counts"].items()):
            print(f"      {series:<8} {count}")

    print(f"\nElapsed time: {minutes}m {seconds}s  ({rows_per_sec:,.0f} rows/sec)")

    # Sanity check -- fetched vs written should match exactly PER EXCHANGE,
    # since this loader is a full-recompute-every-run tool with no
    # filtering between step 1 and step 3. A mismatch here means
    # something silently dropped rows (e.g. a duplicate-key collision on
    # upsert) and is worth investigating before trusting this run's
    # numbers.
    any_mismatch = False
    for exchange, r in per_exchange_results.items():
        if r["written_count"] != r["row_count"]:
            any_mismatch = True
            print(f"\n  [WARNING] {exchange}: rows written ({r['written_count']}) does not match "
                  f"rows fetched ({r['row_count']}) -- investigate before trusting this run.")
    if not any_mismatch:
        print("\n  [OK] Rows written matches rows fetched for every exchange.")

    print("\n" + "=" * 60)
    print("  RUN COMPLETE")
    print("=" * 60)


def run():
    """Standard entry point called by main.py (or directly, standalone)."""
    with start_run_logging("rsi14d_loader"):
        run_start_time = time.time()
        step_0_context = step_0()

        try:
            conn = get_connection(step_0_context["env_values"])
        except DbConnectionError as e:
            print(f"  [FAILED] {e}")
            sys.exit(1)

        per_exchange_results = {}
        try:
            for exchange in EXCHANGES:
                step_1_context = step_1(conn, exchange)

                if step_1_context["row_count"] == 0:
                    per_exchange_results[exchange] = {
                        **step_1_context,
                        "populated_count": 0, "pre_seed_count": 0, "written_count": 0,
                    }
                    continue

                step_2_context = step_2(step_1_context["df"], exchange)
                step_3_context = step_3(conn, step_2_context["rsi_df"], exchange)

                per_exchange_results[exchange] = {
                    "isin_count": step_1_context["isin_count"],
                    "row_count": step_1_context["row_count"],
                    "date_min": step_1_context["date_min"],
                    "date_max": step_1_context["date_max"],
                    "series_counts": step_1_context["series_counts"],
                    "combo_count": step_1_context["combo_count"],
                    "populated_count": step_2_context["populated_count"],
                    "pre_seed_count": step_2_context["pre_seed_count"],
                    "written_count": step_3_context["written_count"],
                }

                # Explicitly drop the large per-exchange frames before
                # moving to the next exchange, and force a collection --
                # this is the whole point of splitting the work: the
                # next exchange's fetch/compute should not have to share
                # peak memory with this exchange's now-finished frames.
                del step_1_context, step_2_context, step_3_context
                gc.collect()
        finally:
            conn.close()

        elapsed_seconds = time.time() - run_start_time
        step_4(per_exchange_results, elapsed_seconds)


if __name__ == "__main__":
    run()
