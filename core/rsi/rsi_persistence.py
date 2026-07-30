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

import pandas as pd
from psycopg2.extras import execute_values


class RsiPersistenceError(Exception):
    """Raised when fetching bhav_copy closes or upserting rsi14d_workbook fails."""
    pass


def fetch_bhav_copy_closes(conn, exchange):
    """
    Fetch EQUITY-category bhav_copy closes for a single exchange only.
    Processing one exchange at a time (NSE, then BSE) instead of the
    full history at once roughly halves peak memory during Step 1's
    fetch and Step 2's per-series RSI computation, since the other
    exchange's rows are never resident in memory simultaneously.
    """
    try:
        query = """
            SELECT bc.isin, bc.exchange, bc.series, bc.symbol, bc.trade_date, bc.close, bc.prev_close
              FROM bhav_copy bc
             WHERE bc.exchange = %(exchange)s
               AND EXISTS (
                 SELECT 1 FROM security_series ss
                  WHERE ss.exchange = bc.exchange
                    AND bc.series LIKE ss.series_code_pattern
                    AND ss.category = 'EQUITY'
             )
             ORDER BY bc.isin, bc.series, bc.symbol, bc.trade_date ASC
        """
        return pd.read_sql(query, conn, params={"exchange": exchange})
    except Exception as e:
        raise RsiPersistenceError(f"Failed to fetch bhav_copy closes for {exchange}: {e}")


UPSERT_SQL = """
    INSERT INTO rsi14d_workbook (isin, exchange, series, symbol, trade_date, gain, loss, avg_gain, avg_loss, rsi14)
    VALUES %s
    ON CONFLICT (isin, exchange, series, symbol, trade_date) DO UPDATE SET
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
