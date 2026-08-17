# tests/repair_availfc_rsi_WRITE.py
#
# *** THIS SCRIPT WRITES TO rsi14d_workbook. ***
#
# Targeted repair for the confirmed-stale RSI continuity break on
# BSE:AVAILFC (isin INE325G01010), diagnosed via
# tests/diagnose_availfc_rsi_manual.py:
#   - rsi14d_workbook currently shows a reseed on 2026-04-22 (coinciding
#     with the stock's BSE series changing X -> B): avg_gain/avg_loss/
#     rsi14 go NULL for 13 rows, then reseed from a fresh 14-day simple
#     average on 2026-05-12.
#   - Recomputing from CURRENT bhav_copy data (read-only, verified via
#     the diagnose script) bridges straight through that point instead
#     -- no reseed -- and a full scan of the recomputed 2024-01-01 to
#     2026-08-10 series found exactly ONE seed point total, on
#     2024-01-18 (the legitimate first possible seed date given
#     bhav_copy's own history starts 2024-01-01 and RSI_PERIOD=14 needs
#     13 pre-seed rows). So the stored April 2026 reseed is confirmed
#     stale, not a live bug -- this script corrects it.
#
# This reuses the EXACT same fetch -> compute -> upsert pattern already
# used (and already shipped) by core/corporate_actions/pipeline.py's
# targeted single-isin reprocess for corporate-actions-triggered
# rebuilds -- not new logic, just scoped to this one isin+exchange
# instead of running the full-universe rsi14d_loader.py for both
# exchanges (~15,700 isins, 20+ minutes) to fix one stock.
#
# upsert_rsi14d_workbook is ON CONFLICT (isin, exchange, trade_date) DO
# UPDATE -- safe to re-run, and only ever touches rows for this isin+
# exchange.
#
# Run from repo root:
#   python tests/repair_availfc_rsi_WRITE.py

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from core.env_validator import load_and_validate_env, EnvValidationError
from core.db_client import get_connection, DbConnectionError
from core.rsi.rsi_persistence import fetch_bhav_copy_closes_for_isin, upsert_rsi14d_workbook, RsiPersistenceError
from core.rsi.rsi_calculator import compute_rsi14_for_isin
from core.rsi.rsi_continuity import fetch_trading_calendar, build_calendar_index, RsiContinuityError

ISIN = "INE325G01010"      # AVAILFC
EXCHANGE = "BSE"


def main():
    try:
        env_values = load_and_validate_env()
    except EnvValidationError as e:
        print(f"[FAILED] env validation: {e}")
        sys.exit(1)

    try:
        conn = get_connection(env_values)
    except DbConnectionError as e:
        print(f"[FAILED] DB connection: {e}")
        sys.exit(1)

    try:
        print(f"Fetching continuity-eligible bhav_copy closes for isin={ISIN} exchange={EXCHANGE} ...")
        df = fetch_bhav_copy_closes_for_isin(conn, ISIN, EXCHANGE)
        if df.empty:
            print("  [FAILED] No eligible rows found -- nothing to repair.")
            sys.exit(1)
        print(f"  [OK] {len(df)} eligible rows, {df['trade_date'].min()} to {df['trade_date'].max()}")

        print("\nFetching shared trading calendar for gap detection ...")
        calendar_index = build_calendar_index(fetch_trading_calendar(conn))
        print("  [OK] calendar loaded")

        print("\nRecomputing RSI14 (Wilder's smoothing, gap-aware) ...")
        rsi_df = compute_rsi14_for_isin(df.sort_values("trade_date"), calendar_index=calendar_index)

        print(f"\n*** About to UPSERT {len(rsi_df)} rows into rsi14d_workbook for "
              f"isin={ISIN} exchange={EXCHANGE}. ***")
        written = upsert_rsi14d_workbook(conn, rsi_df)
        print(f"  [OK] Upserted {written} rows.")

        latest = rsi_df.sort_values("trade_date").iloc[-1]
        print(f"\nLatest row after repair: {latest['trade_date']} rsi14={latest['rsi14']}")
        print("Re-run your original SQL query to confirm:")
        print("  SELECT * FROM public.rsi14d_workbook WHERE symbol='AVAILFC' AND exchange='BSE' "
              "ORDER BY trade_date DESC;")

    except (RsiPersistenceError, RsiContinuityError) as e:
        print(f"[FAILED] {e}")
        sys.exit(1)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
