# core/recompute/shadow.py -- TMT-MDL-US-0005
#
# SHADOW comparison: the rows the INCREMENTAL path computed (in memory) against the rows FULL wrote to the
# real table, matched on the table's key. Prices must match to the paisa (tolerance 0.005 on NUMERIC(20,4)
# values that went through float math); percent / indicator columns within 0.0001 unless the caller says
# otherwise. Result goes to recompute_shadow_check and to one verdict log line.

import json
import math

import pandas as pd

PRICE_TOLERANCE = 0.005
DEFAULT_TOLERANCE = 0.0001
MAX_EXAMPLES = 10
LOGGED_EXAMPLES = 3


def compare(candidate_df, actual_df, key_columns, value_columns, tolerances=None):
    """
    candidate_df: INCREMENTAL result. actual_df: what FULL wrote, for the same keys (may contain more keys;
    only the candidate's keys are compared). A candidate key missing from actual counts as a mismatch.

    Returns {"rows_compared", "mismatches", "max_abs_diff", "examples": [..]}.
    """
    tolerances = tolerances or {}
    if candidate_df is None or candidate_df.empty:
        return {"rows_compared": 0, "mismatches": 0, "max_abs_diff": 0.0, "examples": []}

    cand = candidate_df[key_columns + value_columns].copy()
    act = actual_df[key_columns + value_columns].copy() if actual_df is not None and not actual_df.empty \
        else pd.DataFrame(columns=key_columns + value_columns)
    for df in (cand, act):
        for k in key_columns:
            if k == "trade_date":
                df[k] = pd.to_datetime(df[k]).dt.date
            else:
                df[k] = df[k].astype(str)
    merged = cand.merge(act, on=key_columns, how="left", suffixes=("_inc", "_full"), indicator=True)

    mismatches = 0
    max_abs_diff = 0.0
    examples = []
    for _, row in merged.iterrows():
        key = {k: str(row[k]) for k in key_columns}
        if row["_merge"] != "both":
            mismatches += 1
            if len(examples) < MAX_EXAMPLES:
                examples.append({**key, "problem": "row missing in FULL result"})
            continue
        bad = []
        for col in value_columns:
            a, b = row[f"{col}_inc"], row[f"{col}_full"]
            a_na = a is None or (isinstance(a, float) and math.isnan(a)) or pd.isna(a)
            b_na = b is None or (isinstance(b, float) and math.isnan(b)) or pd.isna(b)
            if a_na and b_na:
                continue
            if a_na != b_na:
                bad.append({"column": col, "incremental": None if a_na else float(a), "full": None if b_na else float(b)})
                continue
            diff = abs(float(a) - float(b))
            max_abs_diff = max(max_abs_diff, diff)
            if diff > tolerances.get(col, DEFAULT_TOLERANCE):
                bad.append({"column": col, "incremental": float(a), "full": float(b), "diff": diff})
        if bad:
            mismatches += 1
            if len(examples) < MAX_EXAMPLES:
                examples.append({**key, "columns": bad})
    return {"rows_compared": len(merged), "mismatches": mismatches, "max_abs_diff": max_abs_diff, "examples": examples}


def verdict_of(result):
    return "PASS" if result["mismatches"] == 0 else "FAIL"


def record(conn, cycle_trade_date, target, exchange, result, list_a_count=None, list_b_count=None):
    """Writes one recompute_shadow_check row (commits) and returns the verdict log line."""
    verdict = verdict_of(result)
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO recompute_shadow_check
                    (run_at, cycle_trade_date, target, exchange, list_a_count, list_b_count,
                     rows_compared, mismatches, max_abs_diff, verdict, examples)
                VALUES (CURRENT_TIMESTAMP, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (cycle_trade_date, target, exchange, list_a_count, list_b_count, result["rows_compared"],
                 result["mismatches"], result["max_abs_diff"], verdict,
                 json.dumps(result["examples"], default=str) if result["examples"] else None),
            )
        conn.commit()
    except Exception as e:
        conn.rollback()
        print(f"  [WARN] SHADOW {target}/{exchange}: could not record recompute_shadow_check row: {e}")
    line = (f"SHADOW {target} [{exchange}]: {result['rows_compared']} rows compared, "
            f"{result['mismatches']} mismatches, max diff {result['max_abs_diff']:.6f} -- {verdict}")
    # First few mismatches in the log too, so a FAIL can be read without querying recompute_shadow_check.
    for example in result["examples"][:LOGGED_EXAMPLES]:
        line += "\n      " + json.dumps(example, default=str)
    return line
