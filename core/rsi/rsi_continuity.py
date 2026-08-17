# core/rsi/rsi_continuity.py
#
# Shared RSI-continuity logic -- used by BOTH the full backfill
# (rsi_calculator.py / rsi_persistence.py) and the daily incremental
# path (rsi_incremental.py), so the two can never drift apart the way
# the old per-file series/category filtering already had (see the
# corporate-actions-adjustment epic's session-handover doc for the full
# background on why this module exists: MEIL's RSI walk fragmenting
# across an EQ<->BE series relabel).
#
# Three separate concerns live here, all keyed off the same idea --
# "which bhav_copy rows actually belong in a stock's continuous RSI
# walk":
#
#   1. ELIGIBILITY -- which rows should feed RSI at all. Two layers:
#        a) security_series.include_in_price_continuity = TRUE -- an
#           explicit, human-reviewed flag (see the Liquibase seed data)
#           for known parallel-mechanism series (BL block deals) and
#           genuinely different instruments (E1 partly-paid shares).
#           This is deliberate data, not inferred -- the whole point is
#           to stop hardcoding this knowledge into every query.
#        b) MIN_LIQUIDITY_FILTER_SQL -- a generic safety net for rows
#           with zero/null traded quantity, catching any future series
#           nobody has explicitly classified yet. Deliberately
#           conservative (excludes only true zero-volume rows) since
#           there's no real trade-volume distribution to calibrate a
#           more aggressive cutoff against yet -- tune later with real
#           data, not guessed now.
#
#   2. SAME-DAY TIEBREAK -- on a date where more than one eligible row
#      exists for the same (isin, exchange) -- e.g. an EQ row and a BE
#      row both present during a surveillance-window overlap, or BSE's
#      trailing '#' remark-flag symbol variant alongside the unflagged
#      one -- pick the single highest-traded-quantity row rather than
#      blending them. A real executed reference price beats a
#      synthetic blended one.
#
#   3. GAP DETECTION -- a stock missing one or more REAL trading
#      sessions (suspension, not a weekend/holiday) should not have its
#      RSI walk silently bridge across the gap, since that computes a
#      Wilder step over an economically meaningless multi-week return.
#      Gap size is measured in trading SESSIONS elapsed on the shared
#      NSE/BSE calendar (bhav_copy_metadata's own SUCCESS history --
#      same source indicators_listener.py already treats as the single
#      source of truth for "which dates are real"), never raw calendar
#      days -- an ordinary Friday-to-Monday weekend must never be
#      mistaken for a 3-day gap.

import pandas as pd

RSI_PERIOD_IMPORT_NOTE = None  # see core.rsi.rsi_math.RSI_PERIOD -- not re-imported here to avoid a cycle; callers pass their own limit.

# Initial default -- number of consecutive REAL trading sessions (per
# the shared calendar) a stock can be absent for before its RSI walk is
# considered genuinely broken (real suspension) rather than a one-off
# ingestion hiccup. Tune once real suspension patterns are observed;
# not calibrated against real data yet, deliberately conservative to
# start (a short absence is more likely a data hiccup than a real
# halt -- don't reseed too eagerly).
GAP_THRESHOLD_SESSIONS = 5

# Excludes only true zero/null-traded-quantity rows -- a safety net,
# not a general liquidity cutoff. See module docstring.
MIN_LIQUIDITY_FILTER_SQL = "COALESCE(bc.tot_trd_qty, 0) > 0"

# EXISTS fragment -- a row is eligible for RSI only if its series maps
# to an EQUITY security_series row AND that row is explicitly flagged
# include_in_price_continuity = TRUE. Replaces the old bare
# `category = 'EQUITY'` check, which is how BL (block deals) and E1
# (partly-paid shares) were silently feeding RSI in the first place.
ELIGIBLE_SERIES_EXISTS_SQL = """
    EXISTS (
        SELECT 1 FROM security_series ss
         WHERE ss.exchange = bc.exchange
           AND bc.series LIKE ss.series_code_pattern
           AND ss.category = 'EQUITY'
           AND ss.include_in_price_continuity = TRUE
    )
"""

# Window-function fragment -- ranks same-day rows for the same
# (isin, exchange) by traded quantity, highest first. Callers wrap
# their base SELECT in a CTE and filter WHERE continuity_rank = 1.
# NULLS LAST so a row with no recorded quantity never wins over one
# that has any real trading behind it; SERIES ASC as a final
# deterministic tiebreak so re-runs are reproducible even on an exact
# volume tie.
TIEBREAK_RANK_SQL = """
    ROW_NUMBER() OVER (
        PARTITION BY bc.isin, bc.exchange, bc.trade_date
        ORDER BY bc.tot_trd_qty DESC NULLS LAST, bc.series ASC
    ) AS continuity_rank
"""


class RsiContinuityError(Exception):
    """Raised when fetching the shared trading calendar or a gap-reseed window fails."""
    pass


def fetch_trading_calendar(conn, start_date=None, end_date=None):
    """
    Ordered (ascending) list of every trade_date where BOTH NSE and BSE
    show upload_status='SUCCESS' in bhav_copy_metadata -- the same
    "real trading day" source indicators_listener.py already treats as
    authoritative, deliberately reused here rather than a second,
    independently-maintained calendar that could drift out of sync.
    """
    try:
        with conn.cursor() as cur:
            clauses = ["upload_status = 'SUCCESS'"]
            params = {}
            if start_date is not None:
                clauses.append("trade_date >= %(start_date)s")
                params["start_date"] = start_date
            if end_date is not None:
                clauses.append("trade_date <= %(end_date)s")
                params["end_date"] = end_date
            where_sql = " AND ".join(clauses)
            cur.execute(
                f"""
                SELECT trade_date FROM bhav_copy_metadata
                 WHERE {where_sql}
                 GROUP BY trade_date
                HAVING COUNT(DISTINCT exchange) = 2
                 ORDER BY trade_date ASC
                """,
                params,
            )
            return [row[0] for row in cur.fetchall()]
    except Exception as e:
        raise RsiContinuityError(f"Failed to fetch trading calendar: {e}")


def build_calendar_index(calendar_dates):
    """trade_date -> its position in the ordered calendar, for O(1) session-distance lookups."""
    return {d: i for i, d in enumerate(calendar_dates)}


def sessions_between(calendar_index, earlier_date, later_date):
    """
    Number of real trading sessions strictly between earlier_date and
    later_date (0 if they are calendar-adjacent, e.g. Friday then the
    following Monday with only a weekend between them). Returns None
    if either date is missing from the calendar -- a defensive signal
    to callers to skip the gap check rather than force a wrong answer,
    not something expected to happen in normal operation.
    """
    if earlier_date not in calendar_index or later_date not in calendar_index:
        return None
    return calendar_index[later_date] - calendar_index[earlier_date] - 1


def is_gap(calendar_index, earlier_date, later_date, threshold=GAP_THRESHOLD_SESSIONS):
    """True if the stock missed at least `threshold` real trading sessions between the two dates."""
    missed = sessions_between(calendar_index, earlier_date, later_date)
    if missed is None:
        return False
    return missed >= threshold


RECENT_ELIGIBLE_CLOSES_SQL = f"""
    WITH ranked AS (
        SELECT bc.isin, bc.exchange, bc.series, bc.symbol, bc.trade_date,
               bc.close * COALESCE(adj.factor, 1) AS close,
               bc.prev_close * COALESCE(adj.factor, 1) AS prev_close,
               {{tiebreak_rank}}
          FROM bhav_copy bc
          {{adjustment_join}}
         WHERE bc.isin = %(isin)s AND bc.exchange = %(exchange)s
           AND bc.trade_date <= %(as_of_date)s
           AND {{eligibility}}
           AND {MIN_LIQUIDITY_FILTER_SQL}
    )
    SELECT isin, exchange, series, symbol, trade_date, close, prev_close
      FROM ranked
     WHERE continuity_rank = 1
     ORDER BY trade_date DESC
     LIMIT %(limit)s
"""


def fetch_recent_eligible_closes(conn, isin, exchange, as_of_date, limit):
    """
    Last `limit` eligible, deduped, tiebroken closes for one isin+
    exchange up to and including as_of_date, oldest first -- used by
    the incremental path's gap-reseed: when a stock resumes after a
    real suspension, its Wilder average is restarted from scratch using
    this window rather than stepped from a now-stale prior average.

    Imports are local to avoid a circular import (adjustment.py and
    rsi_math don't depend on this module, but rsi_persistence.py does,
    and this function needs the same adjustment-factor join that module
    uses).
    """
    from core.corporate_actions.adjustment import ADJUSTMENT_FACTOR_JOIN_SQL

    query = RECENT_ELIGIBLE_CLOSES_SQL.format(
        tiebreak_rank=TIEBREAK_RANK_SQL,
        adjustment_join=ADJUSTMENT_FACTOR_JOIN_SQL,
        eligibility=ELIGIBLE_SERIES_EXISTS_SQL,
    )
    try:
        df = pd.read_sql(
            query, conn,
            params={"isin": isin, "exchange": exchange, "as_of_date": as_of_date, "limit": limit},
        )
    except Exception as e:
        raise RsiContinuityError(
            f"Failed to fetch recent eligible closes for isin={isin} exchange={exchange}: {e}"
        )
    return df.sort_values("trade_date").reset_index(drop=True)
