# tests/repair_split_face_values_WRITE.py
#
# *** THIS SCRIPT WRITES TO corporate_actions_raw AND corporate_actions,
#     AND MAY WRITE TO rsi14d_workbook (targeted reprocess only). ***
#
# Targeted repair for the "Re 1/-" split-ratio parser bug found via
# TEMBO (isin INE869Y01010, ex_date 2026-08-05): the old
# _SPLIT_FACE_VALUE_RE regex in core/corporate_actions/corporate_actions_
# parser.py only matched literal "Rs", so any split whose text used the
# grammatically-correct singular "Re 1/-" (i.e. the new OR old face
# value is exactly Re. 1) silently failed to parse, leaving
# face_value_old/face_value_new NULL in corporate_actions_raw and the
# row stuck unreconciled.
#
# The regex itself is already fixed (now r"r[se]" instead of "rs").
# This script does NOT re-download anything from NSE/BSE -- the text
# that needs re-parsing (raw_ratio_text) is already sitting in
# corporate_actions_raw from whenever each row was originally ingested.
# It just re-runs the (now-fixed) extraction against that stored text,
# in place, for every row still stuck with a NULL face value:
#
#   1. Find every corporate_actions_raw SPLIT row with
#      face_value_old/new still NULL.
#   2. Re-parse raw_ratio_text with the fixed
#      _extract_split_face_values().
#   3. UPDATE the row if it now parses.
#   4. Re-run reconcile() for every (isin, action_type, ex_date) key
#      touched, exactly like corporate_actions_persistence.py's own
#      contract -- this is what promotes NSE_ONLY -> MATCHED once BOTH
#      exchanges have a parseable ratio, and computes adjustment_factor.
#   5. For any key that newly became MATCHED this run, run the exact
#      same targeted RSI reprocess core/corporate_actions/pipeline.py's
#      run_pipeline() does -- scoped to just those isins, not a
#      full-universe rebuild.
#
# Run from repo root:
#   python tests/repair_split_face_values_WRITE.py

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from core.env_validator import load_and_validate_env, EnvValidationError
from core.db_client import get_connection, DbConnectionError
from core.corporate_actions.corporate_actions_parser import _extract_split_face_values
from core.corporate_actions.corporate_actions_persistence import (
    reconcile,
    CorporateActionsPersistenceError,
)
from core.rsi.rsi_persistence import fetch_bhav_copy_closes_for_isin, upsert_rsi14d_workbook, RsiPersistenceError
from core.rsi.rsi_calculator import compute_rsi14_for_isin
from core.rsi.rsi_continuity import fetch_trading_calendar, build_calendar_index, RsiContinuityError


def find_unparsed_split_rows(conn):
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT id, isin, symbol, exchange, action_type, ex_date, raw_ratio_text
              FROM corporate_actions_raw
             WHERE action_type = 'SPLIT'
               AND (face_value_old IS NULL OR face_value_new IS NULL)
             ORDER BY ex_date
            """
        )
        columns = ["id", "isin", "symbol", "exchange", "action_type", "ex_date", "raw_ratio_text"]
        return [dict(zip(columns, row)) for row in cur.fetchall()]


def repair_raw_rows(conn, rows):
    """Re-parses each row's raw_ratio_text with the fixed regex and
    updates face_value_old/new in place. Returns (touched_keys, still_unparsed)."""
    touched_keys = set()
    still_unparsed = []

    with conn.cursor() as cur:
        for row in rows:
            face_values = _extract_split_face_values(row["raw_ratio_text"])
            if face_values is None:
                still_unparsed.append(row)
                continue

            face_value_old, face_value_new = face_values
            cur.execute(
                """
                UPDATE corporate_actions_raw
                   SET face_value_old = %s, face_value_new = %s, ingested_at = CURRENT_TIMESTAMP
                 WHERE id = %s
                """,
                (face_value_old, face_value_new, row["id"]),
            )
            print(f"  [OK] id={row['id']} {row['exchange']} {row['symbol']} ({row['isin']}) "
                  f"ex_date={row['ex_date']}: face_value {face_value_old} -> {face_value_new}")
            touched_keys.add((row["isin"], row["action_type"], row["ex_date"]))

    return touched_keys, still_unparsed


def reprocess_newly_matched(conn, newly_matched_keys):
    """Same targeted-reprocess logic as core/corporate_actions/pipeline.py's
    run_pipeline() -- kept in lockstep with it deliberately, not
    reimplemented independently."""
    reprocess_results = []
    if not newly_matched_keys:
        return reprocess_results

    newly_matched_isins = sorted({isin for isin, _action_type, _ex_date in newly_matched_keys})

    try:
        calendar_index = build_calendar_index(fetch_trading_calendar(conn))
    except RsiContinuityError as e:
        calendar_index = None
        print(f"  [WARNING] Could not fetch trading calendar for gap-aware reprocess: {e} "
              f"-- continuing without gap detection for this batch.")

    for isin in newly_matched_isins:
        for exchange in ("NSE", "BSE"):
            try:
                df = fetch_bhav_copy_closes_for_isin(conn, isin, exchange)
            except RsiPersistenceError as e:
                reprocess_results.append({"isin": isin, "exchange": exchange, "written": 0, "error": str(e)})
                continue

            if df.empty:
                continue  # isin doesn't trade on this exchange -- normal, not an error

            rsi_df = compute_rsi14_for_isin(df.sort_values("trade_date"), calendar_index=calendar_index)

            try:
                written = upsert_rsi14d_workbook(conn, rsi_df)
            except RsiPersistenceError as e:
                reprocess_results.append({"isin": isin, "exchange": exchange, "written": 0, "error": str(e)})
                continue

            reprocess_results.append({"isin": isin, "exchange": exchange, "written": written, "error": None})

    return reprocess_results


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
        print("Finding corporate_actions_raw SPLIT rows with a still-NULL face value ...")
        rows = find_unparsed_split_rows(conn)
        print(f"  [OK] {len(rows)} row(s) found.")
        if not rows:
            print("Nothing to repair.")
            return

        print("\nRe-parsing raw_ratio_text with the fixed regex ...")
        touched_keys, still_unparsed = repair_raw_rows(conn, rows)
        conn.commit()

        if still_unparsed:
            print(f"\n  [WARNING] {len(still_unparsed)} row(s) STILL unparsed after the fix -- "
                  f"needs manual review, not the Re/Rs issue:")
            for row in still_unparsed:
                print(f"    id={row['id']} {row['exchange']} {row['symbol']} ({row['isin']}) "
                      f"ex_date={row['ex_date']}: {row['raw_ratio_text']!r}")

        if not touched_keys:
            print("\nNo rows were successfully repaired -- nothing to reconcile.")
            return

        print(f"\nRe-reconciling {len(touched_keys)} touched key(s) ...")
        try:
            newly_matched_keys = reconcile(conn, touched_keys)
            conn.commit()
        except CorporateActionsPersistenceError as e:
            conn.rollback()
            print(f"  [FAILED] reconcile: {e}")
            sys.exit(1)

        print(f"  [OK] {len(newly_matched_keys)} key(s) newly MATCHED this run: {newly_matched_keys}")

        if not newly_matched_keys:
            print("\nNo keys newly became MATCHED (still waiting on the other exchange, most likely) "
                  "-- skipping RSI reprocess.")
        else:
            print("\nRunning targeted RSI reprocess for newly-MATCHED isins ...")
            reprocess_results = reprocess_newly_matched(conn, newly_matched_keys)
            for r in reprocess_results:
                if r["error"]:
                    print(f"  [FAILED] {r['isin']} ({r['exchange']}): {r['error']}")
                else:
                    print(f"  [OK] {r['isin']} ({r['exchange']}): rebuilt {r['written']} rsi14d_workbook row(s).")

        print("\nDone. Re-run your original diagnostic SQL to confirm:")
        print("  SELECT isin, symbol, exchange, action_type, ex_date, adjustment_factor, "
              "reconciliation_status FROM public.corporate_actions "
              "WHERE isin IN ('INE869Y01010','INE811A01020');")

    except (RsiPersistenceError, RsiContinuityError) as e:
        print(f"[FAILED] {e}")
        sys.exit(1)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
