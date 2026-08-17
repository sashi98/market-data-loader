# core/rsi/rsi_persistence.py
#
# DB access for the RSI14 bulk backfill -- fetch closes from bhav_copy,
# batch upsert results into rsi14d_workbook.
#
# Full-recompute-every-run semantics (Sashikant's call): this script is
# always safe to re-run over the FULL bhav_copy history -- ON CONFLICT
# DO UPDATE on every row, no partial/incremental mode. This matches the
# convergence math already documented for this project: extending
# history backward requires a full recompute anyway, so there's no
# value in a partial mode for the bulk tool. (The daily event-driven
# path, built separately in the tmt Spring Boot app, is what does cheap
# single-day increments going forward.)
#
# CONTINUITY FIX (see core/rsi/rsi_continuity.py) -- rows are now
# deduped to exactly one per (isin, exchange, trade_date) before they
# ever reach the RSI walk: security_series' include_in_price_continuity
# flag excludes parallel-mechanism/different-instrument series (BL, E1)
# outright, a minimum-liquidity check excludes zero-volume rows as a
# generic safety net, and a same-day tiebreak (highest traded quantity)
# resolves any remaining collision -- e.g. a genuine EQ/BE surveillance
# overlap, or BSE's trailing '#' remark-flag symbol variant landing
# alongside the unflagged row on the same date. SERIES/SYMBOL stay in
# the output purely as an audit trail of which row won each date; they
# are no longer part of the grouping or uniqueness key -- see
# rsi14d_workbook's updated unique constraint.

import pandas as pd
from psycopg2.extras import execute_values

from core.corporate_actions.adjustment import ADJUSTMENT_FACTOR_JOIN_SQL
from core.rsi.rsi_continuity import ELIGIBLE_SERIES_EXISTS_SQL, MIN_LIQUIDITY_FILTER_SQL, TIEBREAK_RANK_SQL


class RsiPersistenceError(Exception):
    """Raised when fetching bhav_copy closes or upserting rsi14d_workbook fails."""
    pass


def fetch_bhav_copy_closes(conn, exchange):
    """
    Fetch continuity-eligible bhav_copy closes for a single exchange
    only, exactly one row per (isin, trade_date) after the eligibility
    filter and same-day tiebreak. Processing one exchange at a time
    (NSE, then BSE) instead of the full history at once roughly halves
    peak memory during Step 1's fetch and Step 2's per-isin RSI
    computation, since the other exchange's rows are never resident in
    memory simultaneously.

    close/prev_close are multiplied by each isin's cumulative
    corporate-actions adjustment factor as of trade_date (see
    core/corporate_actions/adjustment.py) -- a no-op (factor 1) for the
    overwhelming majority of isins that have no MATCHED action.
    bhav_copy itself is never modified; this is a read-time adjustment
    only.
    """
    try:
        query = f"""
            WITH ranked AS (
                SELECT bc.isin, bc.exchange, bc.series, bc.symbol, bc.trade_date,
                       bc.close * COALESCE(adj.factor, 1) AS close,
                       bc.prev_close * COALESCE(adj.factor, 1) AS prev_close,
                       {TIEBREAK_RANK_SQL}
                  FROM bhav_copy bc
                  {ADJUSTMENT_FACTOR_JOIN_SQL}
                 WHERE bc.exchange = %(exchange)s
                   AND {ELIGIBLE_SERIES_EXISTS_SQL}
                   AND {MIN_LIQUIDITY_FILTER_SQL}
            )
            SELECT isin, exchange, series, symbol, trade_date, close, prev_close
              FROM ranked
             WHERE continuity_rank = 1
             ORDER BY isin, trade_date ASC
        """
        return pd.read_sql(query, conn, params={"exchange": exchange})
    except Exception as e:
        raise RsiPersistenceError(f"Failed to fetch bhav_copy closes for {exchange}: {e}")


def fetch_bhav_copy_closes_for_isin(conn, isin, exchange):
    """
    Same shape/adjustment/continuity behavior as fetch_bhav_copy_closes(),
    scoped to a single isin+exchange -- used for the corporate-actions-
    triggered targeted rebuild (see loaders/corporate_actions_loader.py's
    step_3), which only needs to rebuild the ONE isin a newly-MATCHED
    action belongs to, not a full per-exchange recompute.

    Returns an empty DataFrame (not an error) if the isin doesn't trade
    on this exchange at all -- a normal, expected case for an isin
    listed on only one of NSE/BSE.
    """
    try:
        query = f"""
            WITH ranked AS (
                SELECT bc.isin, bc.exchange, bc.series, bc.symbol, bc.trade_date,
                       bc.close * COALESCE(adj.factor, 1) AS close,
                       bc.prev_close * COALESCE(adj.factor, 1) AS prev_close,
                       {TIEBREAK_RANK_SQL}
                  FROM bhav_copy bc
                  {ADJUSTMENT_FACTOR_JOIN_SQL}
                 WHERE bc.isin = %(isin)s AND bc.exchange = %(exchange)s
                   AND {ELIGIBLE_SERIES_EXISTS_SQL}
                   AND {MIN_LIQUIDITY_FILTER_SQL}
            )
            SELECT isin, exchange, series, symbol, trade_date, close, prev_close
              FROM ranked
             WHERE continuity_rank = 1
             ORDER BY trade_date ASC
        """
        return pd.read_sql(query, conn, params={"isin": isin, "exchange": exchange})
    except Exception as e:
        raise RsiPersistenceError(f"Failed to fetch bhav_copy closes for isin={isin} exchange={exchange}: {e}")


UPSERT_SQL = """
    INSERT INTO rsi14d_workbook (isin, exchange, series, symbol, trade_date, gain, loss, avg_gain, avg_loss, rsi14)
    VALUES %s
    ON CONFLICT (isin, exchange, trade_date) DO UPDATE SET
        series = EXCLUDED.series,
        symbol = EXCLUDED.symbol,
        gain = EXCLUDED.gain,
        loss = EXCLUDED.loss,
        avg_gain = EXCLUDED.avg_gain,
        avg_loss = EXCLUDED.avg_loss,
        rsi14 = EXCLUDED.rsi14
"""


def upsert_rsi14d_workbook(conn, rsi_df):
    raw_values = (
        rsi_df[["isin", "exchange", "series", "symbol", "trade_date", "gain", "loss", "avg_gain", "avg_loss", "rsi14"]]
        .itertuples(index=False, name=None)
    )
    values = [
        tuple(None if isinstance(v, float) and pd.isna(v) else v for v in row)
        for row in raw_values
    ]
    try:
        with conn.cursor() as cur:
            execute_values(cur, UPSERT_SQL, values, page_size=1000)
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise RsiPersistenceError(f"Failed to upsert rsi14d_workbook: {e}")
    return len(values)
