# core/price_series/classification.py -- TMT-MDL-US-0005, STEP 3A for STEP 4 (price adjustment)
#
# Splits one exchange's stocks into
#   list-A  (INCREMENTAL): stored bhav_copy_adjusted history is still right; only rows after the stock's
#           bookmark (its last stored trade_date) need adding.
#   list-B  (FULL for that stock): the stored history is (or may be) wrong.
#
# The bookmark is the stock's own last row in bhav_copy_adjusted -- no separate bookmark table to drift out
# of step: it only moves when a row is actually written.
#
# list-B reasons, all derived from the data itself (self-checking, no timestamps needed):
#   STATE_INCONSISTENT  the previous STEP 4 run for this exchange did not finish (bhav_copy_adjusted_metadata
#                       run_status RUNNING / FAILED) -- the whole exchange goes to list-B (= FULL).
#   NEW_STOCK           eligible raw bhav_copy rows exist but nothing is stored yet.
#   CA_EFFECTIVE /      the stored adjustment factor on the stock's first or last row no longer equals the
#   CA_CHANGED          product of today's matched corporate actions for that date: an action was added,
#                       amended or removed, so stored rows must be re-scaled. CA_EFFECTIVE when the action's
#                       ex_date is after the bookmark (a split / bonus taking effect in the new days), else
#                       CA_CHANGED (an older action added or corrected after the fact). A future-dated action
#                       that is already applied does not move a stock to list-B: the factor of a row does not
#                       depend on today's date, only on the actions known.
#   BACKDATED_DATA      the number of eligible raw trading days up to the bookmark differs from the number of
#                       stored rows (a back-dated / corrected bhav copy, or a half-written earlier run).
#   FORCED              an admin queued the stock in recompute_queue with target PRICE_ADJUSTMENT.

import math

import pandas as pd

from core.price_series.adjusted_series import resolve_security_id, AdjustedSeriesError
from core.rsi.rsi_continuity import ELIGIBLE_SERIES_EXISTS_SQL, MIN_LIQUIDITY_FILTER_SQL

STATE_INCONSISTENT = "STATE_INCONSISTENT"
NEW_STOCK = "NEW_STOCK"
CA_EFFECTIVE = "CA_EFFECTIVE"
CA_CHANGED = "CA_CHANGED"
BACKDATED_DATA = "BACKDATED_DATA"
FORCED = "FORCED"

FACTOR_REL_TOLERANCE = 1e-8

STORED_STATE_SQL = """
    WITH f AS (
        SELECT DISTINCT ON (security_id) security_id, trade_date AS first_date,
               adjustment_factor_applied AS first_factor
          FROM bhav_copy_adjusted WHERE exchange = %(exchange)s
         ORDER BY security_id, trade_date ASC
    ), l AS (
        SELECT DISTINCT ON (security_id) security_id, trade_date AS last_date,
               adjustment_factor_applied AS last_factor
          FROM bhav_copy_adjusted WHERE exchange = %(exchange)s
         ORDER BY security_id, trade_date DESC
    ), c AS (
        SELECT security_id, COUNT(*) AS row_count
          FROM bhav_copy_adjusted WHERE exchange = %(exchange)s
         GROUP BY security_id
    )
    SELECT f.security_id, f.first_date, f.first_factor, l.last_date, l.last_factor, c.row_count
      FROM f JOIN l USING (security_id) JOIN c USING (security_id)
"""

RAW_DAYS_UP_TO_BOOKMARK_SQL = f"""
    WITH b AS (
        SELECT * FROM unnest(%(isins)s::text[], %(security_ids)s::text[], %(bookmarks)s::date[])
               AS b(isin, security_id, bookmark)
    )
    SELECT b.security_id, COUNT(DISTINCT bc.trade_date) AS raw_days
      FROM b
      JOIN bhav_copy bc ON bc.isin = b.isin AND bc.trade_date <= b.bookmark
     WHERE bc.exchange = %(exchange)s
       AND {ELIGIBLE_SERIES_EXISTS_SQL}
       AND {MIN_LIQUIDITY_FILTER_SQL}
     GROUP BY b.security_id
"""


def fetch_stored_state(conn, exchange):
    """{security_id: {first_date, first_factor, last_date, last_factor, row_count}} from bhav_copy_adjusted."""
    try:
        df = pd.read_sql(STORED_STATE_SQL, conn, params={"exchange": exchange})
    except Exception as e:
        raise AdjustedSeriesError(f"Failed to read stored bhav_copy_adjusted state for {exchange}: {e}")
    return {r.security_id: {"first_date": r.first_date, "first_factor": float(r.first_factor),
                            "last_date": r.last_date, "last_factor": float(r.last_factor),
                            "row_count": int(r.row_count)}
            for r in df.itertuples(index=False)}


def fetch_raw_days_up_to_bookmark(conn, exchange, isins_by_security, bookmark_by_security):
    """{security_id: distinct eligible raw trading days with trade_date <= bookmark}."""
    isins, sids, bms = [], [], []
    for sid, bookmark in bookmark_by_security.items():
        for isin in isins_by_security.get(sid, []):
            isins.append(isin)
            sids.append(sid)
            bms.append(bookmark)
    if not isins:
        return {}
    try:
        with conn.cursor() as cur:
            cur.execute(RAW_DAYS_UP_TO_BOOKMARK_SQL,
                        {"exchange": exchange, "isins": isins, "security_ids": sids, "bookmarks": bms})
            return {sid: int(n) for sid, n in cur.fetchall()}
    except Exception as e:
        raise AdjustedSeriesError(f"Failed to count raw trading days for {exchange}: {e}")


def actions_by_security(actions_df, lineage_map):
    """{security_id: [(ex_date, factor), ...]} for the matched corporate actions."""
    out = {}
    if actions_df is None or actions_df.empty:
        return out
    for r in actions_df.itertuples(index=False):
        sid = resolve_security_id(r.isin, lineage_map)
        out.setdefault(sid, []).append((r.ex_date, float(r.adjustment_factor)))
    return out


def expected_factor(actions, trade_date):
    """Product of the factors of every action with ex_date > trade_date (same rule as build_adjusted_series)."""
    prod = 1.0
    for ex_date, factor in actions or []:
        if ex_date > trade_date:
            prod *= factor
    return prod


def _same(a, b):
    return math.isclose(a, b, rel_tol=FACTOR_REL_TOLERANCE, abs_tol=1e-12)


def classify(isins_by_security, stored_state, ca_by_security, raw_days_by_security,
             forced_security_ids=(), previous_run_ok=True):
    """
    Pure decision (no DB) -- see the module header. Returns (list_a, list_b):
      list_a: {security_id: bookmark_date}
      list_b: {security_id: reason}
    Only securities that have eligible raw rows (isins_by_security) are classified.
    """
    list_a, list_b = {}, {}
    forced = set(forced_security_ids)
    for sid in isins_by_security:
        if not previous_run_ok:
            list_b[sid] = STATE_INCONSISTENT
            continue
        if sid in forced:
            list_b[sid] = FORCED
            continue
        state = stored_state.get(sid)
        if state is None:
            list_b[sid] = NEW_STOCK
            continue
        actions = ca_by_security.get(sid, [])
        if not _same(state["first_factor"], expected_factor(actions, state["first_date"])) or \
           not _same(state["last_factor"], expected_factor(actions, state["last_date"])):
            # A newly matched action with ex_date after the bookmark = it takes effect in the new days.
            newer = any(ex_date > state["last_date"] for ex_date, _ in actions)
            list_b[sid] = CA_EFFECTIVE if newer else CA_CHANGED
            continue
        if raw_days_by_security.get(sid) != state["row_count"]:
            list_b[sid] = BACKDATED_DATA
            continue
        list_a[sid] = state["last_date"]
    return list_a, list_b


def summarize(list_a, list_b):
    """'list-A 8,291 | list-B 9 (CA_EFFECTIVE 6, NEW_STOCK 2, FORCED 1)'"""
    counts = {}
    for reason in list_b.values():
        counts[reason] = counts.get(reason, 0) + 1
    detail = ", ".join(f"{r} {n}" for r, n in sorted(counts.items(), key=lambda kv: -kv[1]))
    return f"list-A {len(list_a):,} | list-B {len(list_b):,}" + (f" ({detail})" if detail else "")
