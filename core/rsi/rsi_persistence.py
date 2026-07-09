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


# Series included in the RSI computation, at query time only -- bhav_copy
# itself stays an unfiltered raw dump (see standing design decision).
# Scope = EQUITY + SME categories, both exchanges, per security_series
# (008.01.00-008.04.00 changelogs):
#   NSE EQUITY -> EQ
#   NSE SME    -> SM, SZ, N8
#   BSE EQUITY -> B
#   BSE SME    -> (no code exists in security_series -- known gap,
#                  BSE SME stocks are not covered until one is added)
# Deliberately excludes NSE's BE (categorized as DEBT/Bonds-Debentures
# in the seed data, not equity) and ST (STRUCTURED_PRODUCTS, not a
# stock) -- not stocks, out of scope for RSI.
# Kept as its own constant (not inlined into the SQL string) since this
# is meant to become a per-scanner_config parameter later, not stay
# hardcoded forever.
RSI_SERIES = ("EQ", "SM", "SZ", "N8", "B")


def fetch_bhav_copy_closes(conn):
    """
    Fetches isin, symbol, trade_date, close, prev_close from bhav_copy for
    ALL trading dates, filtered to series IN RSI_SERIES at query time
    (bhav_copy itself stays an unfiltered raw dump -- see standing
    design decision).

    prev_close is fetched alongside close specifically so gain/loss can
    be derived from the exchange-published previous close (see
    rsi_calculator.py's module docstring for why this matters -- it's
    NOT the same as close.diff() against the previous row in our own
    table).

    Returns a DataFrame sorted by isin, trade_date ASC -- the order
    compute_rsi14_all() expects (though it re-sorts per group defensively).

    Raises RsiPersistenceError on any failure.
    """
    try:
        query = """
            SELECT isin, symbol, trade_date, close, prev_close
              FROM bhav_copy
             WHERE series IN %(series)s
             ORDER BY isin, trade_date ASC
        """
        return pd.read_sql(query, conn, params={"series": RSI_SERIES})
    except Exception as e:
        raise RsiPersistenceError(f"Failed to fetch bhav_copy closes: {e}")


UPSERT_SQL = """
    INSERT INTO rsi14d_workbook (isin, symbol, trade_date, gain, loss, avg_gain, avg_loss, rsi14)
    VALUES %s
    ON CONFLICT (isin, trade_date) DO UPDATE SET
        symbol = EXCLUDED.symbol,
        gain = EXCLUDED.gain,
        loss = EXCLUDED.loss,
        avg_gain = EXCLUDED.avg_gain,
        avg_loss = EXCLUDED.avg_loss,
        rsi14 = EXCLUDED.rsi14
"""


def upsert_rsi14d_workbook(conn, rsi_df):
    """
    Batch upserts rsi_df (columns: isin, symbol, trade_date, gain, loss,
    avg_gain, avg_loss, rsi14) into rsi14d_workbook, keyed on
    (isin, trade_date). NaN values (rows before the RSI seed) are written
    as SQL NULL, not dropped -- every (isin, trade_date) in bhav_copy gets
    a row here, per the "full history, append-only" design.

    Runs as ONE transaction -- commits on success, rolls back and raises
    RsiPersistenceError on any failure. Caller does not need to
    commit()/rollback() itself.
    """
    # psycopg2 chokes on numpy NaN for numeric columns -- must be Python None
    clean_df = rsi_df.where(pd.notna(rsi_df), None)

    values = list(
        clean_df[["isin", "symbol", "trade_date", "gain", "loss", "avg_gain", "avg_loss", "rsi14"]]
        .itertuples(index=False, name=None)
    )

    try:
        with conn.cursor() as cur:
            execute_values(cur, UPSERT_SQL, values, page_size=1000)
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise RsiPersistenceError(f"Failed to upsert rsi14d_workbook: {e}")

    return len(values)
