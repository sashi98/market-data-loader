# tests/diagnose_availfc_rsi_manual.py
#
# Read-only diagnostic for suspected stale RSI-continuity breaks in
# rsi14d_workbook for BSE:AVAILFC (isin INE325G01010). This does NOT
# write anything to the DB -- it only fetches current bhav_copy data
# via the same continuity-eligible path the real backfill uses
# (fetch_bhav_copy_closes_for_isin) and recomputes RSI in memory via
# compute_rsi14_for_isin, so it's safe to run any time.
#
# Round 1 of this diagnostic already confirmed the 2026-04-22 reseed
# (stored in rsi14d_workbook, coinciding with the BSE series changing
# X -> B) is STALE: recomputing from current bhav_copy data bridges
# straight through it (avg_gain/avg_loss step normally from 2026-04-21
# into 2026-04-22, no reseed), and the resulting 2026-08-10 RSI
# (27.8812) matches a hand-verified "continuous" replay exactly.
#
# That fix alone doesn't fully close the gap to TradingView's 28.49
# (still ~0.6 off), and the raw rsi14d_workbook dump also showed a
# second blank-avg stretch around 2024-01-29 to 2024-02-14 -- this
# round scans the ENTIRE freshly recomputed series (not just hand-picked
# dates) to find every chunk boundary (every place compute_rsi14_for_isin
# decided to reseed rather than step), so we know definitively whether
# any OTHER discontinuity remains even against current, live data.
#
# Run from repo root:
#   python tests/diagnose_availfc_rsi_manual.py

import sys
from datetime import date
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from core.env_validator import load_and_validate_env, EnvValidationError
from core.db_client import get_connection, DbConnectionError
from core.rsi.rsi_persistence import fetch_bhav_copy_closes_for_isin, RsiPersistenceError
from core.rsi.rsi_calculator import compute_rsi14_for_isin
from core.rsi.rsi_continuity import fetch_trading_calendar, build_calendar_index, RsiContinuityError

ISIN = "INE325G01010"      # AVAILFC
EXCHANGE = "BSE"

CHECK_DATES = [
    date(2026, 4, 21), date(2026, 4, 22), date(2026, 4, 23), date(2026, 5, 8),
    date(2026, 5, 11), date(2026, 5, 12), date(2026, 7, 24), date(2026, 7, 27),
    date(2026, 8, 6), date(2026, 8, 7), date(2026, 8, 10),
]


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
        df["trade_date"] = pd.to_datetime(df["trade_date"])
        print(f"  [OK] {len(df)} eligible rows, {df['trade_date'].min().date()} to {df['trade_date'].max().date()}")

        print("\nFetching shared trading calendar for gap detection ...")
        calendar_dates = fetch_trading_calendar(conn)
        calendar_index = build_calendar_index(calendar_dates)
        print(f"  [OK] {len(calendar_dates)} real trading sessions loaded")

        print("\nRecomputing RSI14 fresh from current data (in-memory only, no DB writes) ...")
        result = compute_rsi14_for_isin(df, calendar_index=calendar_index)
        result["trade_date"] = pd.to_datetime(result["trade_date"])
        result = result.sort_values("trade_date").reset_index(drop=True)

        print("\n--- Recomputed values at key checkpoints ---")
        for d in CHECK_DATES:
            row = result[result["trade_date"] == pd.Timestamp(d)]
            if row.empty:
                print(f"  {d}: no row (not eligible / not in fetched range)")
                continue
            r = row.iloc[0]
            print(f"  {d}: series={r['series']:<3} gain={r['gain']:.4f} loss={r['loss']:.4f} "
                  f"avg_gain={r['avg_gain']} avg_loss={r['avg_loss']} rsi14={r['rsi14']}")

        # Full scan: every chunk boundary the CURRENT engine actually finds
        # against CURRENT data. A "seed row" is the first row of a chunk
        # with a non-null avg_gain immediately preceded by a null-avg row
        # (or the very first row of the whole series). The very first
        # chunk (starting at df's earliest date) is expected/legitimate --
        # bhav_copy simply has no data before that. Anything else is a
        # genuine, still-live discontinuity.
        avg_null = result["avg_gain"].isna()
        seed_rows = result[(~avg_null) & (avg_null.shift(1, fill_value=True))]

        print(f"\n--- Every seed/reseed point found in the FRESH recompute ({len(seed_rows)} total) ---")
        for _, r in seed_rows.iterrows():
            note = "  <-- expected: start of fetched history" if r["trade_date"] == result["trade_date"].min() else "  <-- LIVE DISCONTINUITY, still unexplained"
            print(f"  seed at {r['trade_date'].date()} (series={r['series']}) avg_gain={r['avg_gain']:.4f} avg_loss={r['avg_loss']:.4f}{note}")

        print("\nIf the only seed point above is the one marked 'expected: start of fetched")
        print("history', then a real (write) run of `python loaders/rsi14d_loader.py` for")
        print("BSE will fully repair AVAILFC's RSI walk from 2026-04-22 onward -- any")
        print("remaining gap vs TradingView's 28.49 is NOT a continuity bug and is worth")
        print("comparing bhav_copy closes directly against TradingView's OHLC instead.")
        print("If any OTHER seed point is marked 'LIVE DISCONTINUITY', that's a second,")
        print("still-active break worth investigating before trusting a full re-run.")

    except (RsiPersistenceError, RsiContinuityError) as e:
        print(f"[FAILED] {e}")
        sys.exit(1)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
