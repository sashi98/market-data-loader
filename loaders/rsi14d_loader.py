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
#   Step 1: Fetch isin/symbol/trade_date/close from bhav_copy
#           (series='EQ' filtered at query time)
#   Step 2: Compute RSI14 (Wilder's smoothing, hand-implemented -- see
#           core/rsi_calculator.py for why pandas_ta.rma() was rejected)
#           per isin
#   Step 3: Batch upsert gain/loss/avg_gain/avg_loss/rsi14 into
#           rsi14d_workbook, keyed on (isin, trade_date)
#   Step 4: Summary report -- rows processed, isins covered, date range,
#           elapsed time
#
# Always a FULL recompute over the entire bhav_copy history, every run --
# no partial/incremental mode (Sashikant's call, matches the convergence
# math already documented: extending history backward needs a full
# recompute regardless).
#
# Exposes run() -- the standard entry point every loader under loaders/
# provides, so main.py can invoke it generically.
#
# Can also be run standalone for development/testing:
#   python rsi14d_loader.py

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


def step_1(env_values):
    print("\n" + "=" * 60)
    print("  STEP 1 -- Fetch bhav_copy closes (EQ + SME, NSE + BSE)")
    print("=" * 60)

    try:
        conn = get_connection(env_values)
    except DbConnectionError as e:
        print(f"  [FAILED] {e}")
        sys.exit(1)

    try:
        df = fetch_bhav_copy_closes(conn)
    except RsiPersistenceError as e:
        print(f"  [FAILED] {e}")
        conn.close()
        sys.exit(1)

    isin_count = df["isin"].nunique()
    row_count = len(df)
    date_min = df["trade_date"].min()
    date_max = df["trade_date"].max()

    print(f"  [OK] Fetched {row_count} rows across {isin_count} isins")
    print(f"  Date range: {date_min} to {date_max}")

    print("\n" + "=" * 60)
    print("  STEP 1 COMPLETE")
    print("=" * 60)

    return {"conn": conn, "df": df, "isin_count": isin_count, "row_count": row_count,
            "date_min": date_min, "date_max": date_max}


def step_2(df):
    print("\n" + "=" * 60)
    print("  STEP 2 -- Compute RSI14 (Wilder's smoothing) per isin")
    print("=" * 60)

    print(f"\n  Processing {df['isin'].nunique()} isins ...")
    rsi_df = compute_rsi14_all(df)

    populated_count = rsi_df["rsi14"].notna().sum()
    print(f"  [OK] Computed {len(rsi_df)} rows ({populated_count} with a non-NULL RSI14 --"
          f" the rest are pre-seed rows within each isin's first 14 trading days)")

    print("\n" + "=" * 60)
    print("  STEP 2 COMPLETE")
    print("=" * 60)

    return {"rsi_df": rsi_df, "populated_count": populated_count}


def step_3(conn, rsi_df):
    print("\n" + "=" * 60)
    print("  STEP 3 -- Upsert into rsi14d_workbook")
    print("=" * 60)

    try:
        written_count = upsert_rsi14d_workbook(conn, rsi_df)
    except RsiPersistenceError as e:
        print(f"  [FAILED] {e}")
        conn.close()
        sys.exit(1)

    print(f"  [OK] Upserted {written_count} rows into rsi14d_workbook")

    print("\n" + "=" * 60)
    print("  STEP 3 COMPLETE")
    print("=" * 60)

    return {"written_count": written_count}


def step_4(step_1_context, step_2_context, step_3_context, elapsed_seconds):
    print("\n" + "=" * 60)
    print("  STEP 4 -- Summary report")
    print("=" * 60)

    minutes, seconds = divmod(int(elapsed_seconds), 60)

    print(f"\nDate range processed:  {step_1_context['date_min']} to {step_1_context['date_max']}")
    print(f"Isins processed:       {step_1_context['isin_count']}")
    print(f"Rows fetched:          {step_1_context['row_count']}")
    print(f"Rows with RSI14 value: {step_2_context['populated_count']}")
    print(f"Rows written:          {step_3_context['written_count']}")
    print(f"\nElapsed time: {minutes}m {seconds}s")

    print("\n" + "=" * 60)
    print("  RUN COMPLETE")
    print("=" * 60)


def run():
    """Standard entry point called by main.py (or directly, standalone)."""
    with start_run_logging("rsi14d_loader"):
        run_start_time = time.time()
        step_0_context = step_0()
        step_1_context = step_1(step_0_context["env_values"])
        conn = step_1_context["conn"]

        try:
            step_2_context = step_2(step_1_context["df"])
            step_3_context = step_3(conn, step_2_context["rsi_df"])
        finally:
            conn.close()

        elapsed_seconds = time.time() - run_start_time
        step_4(step_1_context, step_2_context, step_3_context, elapsed_seconds)


if __name__ == "__main__":
    run()
