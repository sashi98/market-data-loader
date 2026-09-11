# core/ma/ma_persistence.py
#
# DB access for the MA9/MA50/MA200 bulk backfill loaders -- fetch closes
# from bhav_copy_adjusted (daily) / bhav_copy_w (weekly) / bhav_copy_m
# (monthly), batch upsert results into ma{9,50,200}{d,w,m}_workbook
# (011.04.00-011.12.00's changelogs, moved there 2026-08-29).
#
# Generic over source_table (same "table name passed in by the caller"
# pattern as core/rollup/rollup_persistence.py and
# core/rsi/rsi_persistence.py's fetch_period_closes()) -- one function
# serves all three timeframes' source tables. The one wrinkle:
# bhav_copy_adjusted's isin column is named source_isin, not isin (see
# core/rsi/rsi_persistence.py's fetch_bhav_copy_closes() for the same
# asymmetry), while bhav_copy_w/bhav_copy_m both use isin directly --
# ISIN_COLUMN_BY_SOURCE below carries that mapping so callers never have
# to think about it.
#
# MA tables have NO series column (unlike rsi14*_workbook) -- see
# 011.04.00-011.12.00's own createTable changesets -- so neither the
# fetch nor the upsert here carries series through.

import pandas as pd
from psycopg2.extras import execute_values

from core.date_format import fmt_date


class MaPersistenceError(Exception):
    """Raised when fetching closes for MA, or upserting an MA workbook table, fails."""
    pass


ISIN_COLUMN_BY_SOURCE = {
    "bhav_copy_adjusted": "source_isin AS isin",
    "bhav_copy_w": "isin",
    "bhav_copy_m": "isin",
}

COMMIT_EVERY_ROWS = 100_000


def fetch_closes_for_ma(conn, source_table, exchange, start_date, end_date, window):
    """
    Fetch security_id/isin/exchange/symbol/trade_date/close rows for a
    single exchange from source_table (one of "bhav_copy_adjusted",
    "bhav_copy_w", "bhav_copy_m"), bounded to this cycle's
    [start_date, end_date] -- PLUS, per security, the (window - 1) most
    recent rows immediately BEFORE start_date, so compute_ma()'s
    rolling window still has enough real prior history to produce a
    valid (non-NaN) average from start_date onward, instead of treating
    every cycle's own [start_date, end_date] as if it were each
    security's entire history.

    REDESIGNED 2026-08-30 (second pass) -- this used to pull one
    exchange's ENTIRE history in a single unbounded pd.read_sql() call,
    every cycle, regardless of how small [start_date, end_date] actually
    was. On a large first backfill that alone was enough to crash
    Postgres (a multi-million-row DataFrame in memory) -- see
    bhavcopy_scheduler_main.py's STEP 8 comment for the incident. Result
    size is now bounded by (rows in [start_date, end_date]) plus
    (window - 1) * (number of securities on this exchange), not by the
    exchange's total history -- the lookback rows are fetched via a
    ROW_NUMBER() window function ranked by recency, per security, over
    only the rows strictly before start_date, so the extra read stays
    small and constant per cycle regardless of how much history has
    piled up before it.

    All three source tables are already exactly one row per (security_id,
    exchange, trade_date): eligibility, same-day tiebreak, isin-lineage
    bridging, and (for the daily source) split/bonus adjustment are all
    baked in upstream, so no further filtering is needed here, same as
    core/rsi/rsi_persistence.py's own reads of these same tables.
    """
    isin_expr = ISIN_COLUMN_BY_SOURCE.get(source_table)
    if isin_expr is None:
        raise MaPersistenceError(
            f"Unknown source_table={source_table!r} -- must be one of {sorted(ISIN_COLUMN_BY_SOURCE)}"
        )
    try:
        query = f"""
            WITH in_range AS (
                SELECT security_id, {isin_expr}, exchange, symbol, trade_date, close
                  FROM {source_table}
                 WHERE exchange = %(exchange)s AND trade_date BETWEEN %(start_date)s AND %(end_date)s
            ),
            lookback AS (
                SELECT security_id, {isin_expr}, exchange, symbol, trade_date, close,
                       ROW_NUMBER() OVER (PARTITION BY security_id ORDER BY trade_date DESC) AS rn
                  FROM {source_table}
                 WHERE exchange = %(exchange)s AND trade_date < %(start_date)s
            )
            SELECT security_id, isin, exchange, symbol, trade_date, close FROM in_range
            UNION ALL
            SELECT security_id, isin, exchange, symbol, trade_date, close FROM lookback WHERE rn <= %(lookback_rows)s
            ORDER BY security_id, trade_date ASC
        """
        params = {
            "exchange": exchange, "start_date": start_date, "end_date": end_date,
            "lookback_rows": window - 1,
        }
        return pd.read_sql(query, conn, params=params)
    except Exception as e:
        raise MaPersistenceError(
            f"Failed to fetch {source_table} closes for {exchange} in [{fmt_date(start_date)}, {fmt_date(end_date)}]: {e}"
        )


def upsert_ma_workbook(conn, target_table, ma_df, column_name):
    """
    Batch upserts ma_df (as produced by core.ma.ma_calculator.compute_ma())
    into target_table (e.g. "ma9d_workbook"), keyed on (security_id,
    exchange, trade_date) -- see 011.04.00-011.12.00's changelogs.
    column_name is the MA value column (e.g. "MA9") -- same name across
    all three timeframe tables for a given window, only the table
    changes.
    """
    upsert_sql = f"""
        INSERT INTO {target_table} (security_id, isin, exchange, symbol, trade_date, {column_name})
        VALUES %s
        ON CONFLICT (security_id, exchange, trade_date) DO UPDATE SET
            isin = EXCLUDED.isin,
            symbol = EXCLUDED.symbol,
            {column_name} = EXCLUDED.{column_name}
    """
    raw_values = (
        ma_df[["security_id", "isin", "exchange", "symbol", "trade_date", column_name]]
        .itertuples(index=False, name=None)
    )
    values = [
        tuple(None if isinstance(v, float) and pd.isna(v) else v for v in row)
        for row in raw_values
    ]
    if not values:
        return 0
    try:
        for start in range(0, len(values), COMMIT_EVERY_ROWS):
            chunk = values[start:start + COMMIT_EVERY_ROWS]
            with conn.cursor() as cur:
                execute_values(cur, upsert_sql, chunk, page_size=1000)
            conn.commit()
    except Exception as e:
        conn.rollback()
        raise MaPersistenceError(f"Failed to upsert {target_table}: {e}")
    return len(values)
