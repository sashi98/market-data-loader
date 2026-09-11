# core/rsi/rsi_persistence.py
#
# DB access for the RSI14 bulk backfill -- fetch closes from
# bhav_copy_adjusted, batch upsert results into rsi14d_workbook.
#
# ADDED 2026-08-26 -- fetch_period_closes()/upsert_rsi_workbook() at the
# bottom of this module are the WEEKLY/MONTHLY equivalents, used by
# rsi14w_loader.py/rsi14m_loader.py (bhav_copy_w -> rsi14w_workbook,
# bhav_copy_m -> rsi14m_workbook). Generic over source/target table name
# (same "table name passed in by the caller" pattern as
# core/rollup/rollup_persistence.py) rather than three near-identical
# copy-pasted fetch/upsert pairs, since bhav_copy_w and bhav_copy_m are
# already exactly the same shape as each other (unlike bhav_copy_adjusted,
# whose isin column is named source_isin, not isin -- see
# fetch_bhav_copy_closes() below). The original daily-specific
# fetch_bhav_copy_closes()/upsert_rsi14d_workbook() functions are left
# completely untouched by this addition.
#
# REPOINTED 2026-08-24 (security_id migration, see
# claude/bhav-copy-adjusted-clean-price-series-design-2026-08-23.md in
# the project docs) -- this module used to read straight from raw
# bhav_copy, with a read-time corporate-actions adjustment join
# (ADJUSTMENT_FACTOR_JOIN_SQL) and eligibility/same-day-tiebreak
# filtering (core/rsi/rsi_continuity.py), grouped by (isin, exchange).
# All of that work -- eligibility filtering, tiebreak, and split/bonus
# + isin-lineage-bridge adjustment -- is now done ONCE, upstream, by
# loaders/bhav_copy_adjustment_loader.py when it (re)builds
# bhav_copy_adjusted (see core/price_series/adjusted_series.py). This
# module just reads that table's already-clean, already-continuous
# output -- no adjustment join, no eligibility EXISTS check, no
# same-day tiebreak needed here anymore.
#
# Grouping moves from (isin, exchange) to (security_id, exchange) --
# a stock whose isin changed mid-history (e.g. MWL's SME-to-mainboard
# migration, bundled with a split on the same ex_date) is now ONE
# continuous RSI walk instead of fragmenting into a dead old-isin
# series plus a freshly-reseeded new-isin series (the exact bug that
# gave MWL a false 13-day-null RSI gap in August 2026). ISIN is still
# carried through as SOURCE_ISIN -- an audit trail of which isin this
# particular trade_date's row actually traded under, same role
# SERIES/SYMBOL already played after the 014.02.00 continuity-key
# migration -- but it is no longer part of the uniqueness key.
#
# Full-recompute-every-run semantics unchanged (Sashikant's call): this
# script is always safe to re-run over the FULL bhav_copy_adjusted
# history -- ON CONFLICT DO UPDATE on every row, no partial/incremental
# mode.

import pandas as pd
from psycopg2.extras import execute_values


class RsiPersistenceError(Exception):
    """Raised when fetching bhav_copy_adjusted closes or upserting rsi14d_workbook fails."""
    pass


def fetch_bhav_copy_closes(conn, exchange):
    """
    Fetch bhav_copy_adjusted rows for a single exchange, already
    exactly one row per (security_id, trade_date) -- eligibility,
    same-day tiebreak, isin-lineage bridging, and split/bonus
    adjustment are all baked in upstream by
    loaders/bhav_copy_adjustment_loader.py. Processing one exchange at
    a time (NSE, then BSE) matches rsi14d_loader.py's existing
    per-exchange memory-management strategy -- unchanged by this
    repoint, just against a smaller/cleaner source table now.
    """
    try:
        query = """
            SELECT security_id, source_isin AS isin, exchange, series, symbol,
                   trade_date, close, prev_close
              FROM bhav_copy_adjusted
             WHERE exchange = %(exchange)s
             ORDER BY security_id, trade_date ASC
        """
        return pd.read_sql(query, conn, params={"exchange": exchange})
    except Exception as e:
        raise RsiPersistenceError(f"Failed to fetch bhav_copy_adjusted closes for {exchange}: {e}")


def fetch_bhav_copy_closes_for_security_id(conn, security_id, exchange):
    """
    Same shape as fetch_bhav_copy_closes(), scoped to a single
    security_id+exchange -- for ad hoc/manual targeted rebuilds (e.g.
    from a REPL or a one-off diagnostic script).

    NOT currently called by any active production path:
    core/corporate_actions/pipeline.py's old CA-triggered targeted RSI
    reprocess was retired (2026-08-24) rather than repointed to call
    this -- the new sequential pipeline (bhav_copy adjustment loader ->
    W/M rollups -> RSI listeners) is the only path that recomputes RSI
    after a corporate action now. See that module's docstring.

    Returns an empty DataFrame (not an error) if the security doesn't
    trade on this exchange at all.
    """
    try:
        query = """
            SELECT security_id, source_isin AS isin, exchange, series, symbol,
                   trade_date, close, prev_close
              FROM bhav_copy_adjusted
             WHERE security_id = %(security_id)s AND exchange = %(exchange)s
             ORDER BY trade_date ASC
        """
        return pd.read_sql(query, conn, params={"security_id": security_id, "exchange": exchange})
    except Exception as e:
        raise RsiPersistenceError(
            f"Failed to fetch bhav_copy_adjusted closes for security_id={security_id} exchange={exchange}: {e}"
        )


UPSERT_SQL = """
    INSERT INTO rsi14d_workbook (security_id, isin, exchange, series, symbol, trade_date, gain, loss, avg_gain, avg_loss, rsi14)
    VALUES %s
    ON CONFLICT (security_id, exchange, trade_date) DO UPDATE SET
        isin = EXCLUDED.isin,
        series = EXCLUDED.series,
        symbol = EXCLUDED.symbol,
        gain = EXCLUDED.gain,
        loss = EXCLUDED.loss,
        avg_gain = EXCLUDED.avg_gain,
        avg_loss = EXCLUDED.avg_loss,
        rsi14 = EXCLUDED.rsi14
"""


# FIXED 2026-08-26 -- same fix as core/price_series/adjusted_series.py's
# upsert_bhav_copy_adjusted() (see that function's comment for the full
# explanation): committing only once, after the ENTIRE upsert, is fine
# the first time a table is populated (all INSERTs) but risks a
# Postgres "out of memory" on every subsequent full-recompute run, once
# nearly every row goes through ON CONFLICT DO UPDATE instead. This
# loader's own bhav_copy_adjustment_loader.py counterpart hit exactly
# that on a 2026-08-26 rerun. rsi14d_loader.py's most recent run was
# still all-inserts (rsi14d_workbook had just been truncated by
# 014.03.00), so it hasn't hit this yet -- but its NEXT full rerun,
# against an already-populated table, would have. Fixed proactively
# here, same COMMIT_EVERY_ROWS chunking, same idempotent-upsert
# trade-off rationale (full-recompute-every-run, ON CONFLICT DO UPDATE
# makes a partial-then-rerun safe).
COMMIT_EVERY_ROWS = 100_000


def upsert_rsi14d_workbook(conn, rsi_df):
    """
    rsi_df must carry a `security_id` column now (see 014.03.00's
    migration and core/rsi/rsi_calculator.py) -- a DataFrame built by
    an old, not-yet-repointed caller without one will raise a
    KeyError here rather than silently write a NULL/broken row, which
    is the intended failure mode for anything still calling the old
    isin-only shape.
    """
    raw_values = (
        rsi_df[["security_id", "isin", "exchange", "series", "symbol", "trade_date", "gain", "loss", "avg_gain", "avg_loss", "rsi14"]]
        .itertuples(index=False, name=None)
    )
    values = [
        tuple(None if isinstance(v, float) and pd.isna(v) else v for v in row)
        for row in raw_values
    ]
    try:
        for start in range(0, len(values), COMMIT_EVERY_ROWS):
            chunk = values[start:start + COMMIT_EVERY_ROWS]
            with conn.cursor() as cur:
                execute_values(cur, UPSERT_SQL, chunk, page_size=1000)
            conn.commit()
    except Exception as e:
        conn.rollback()
        raise RsiPersistenceError(f"Failed to upsert rsi14d_workbook: {e}")
    return len(values)


# ---------------------------------------------------------------------------
# WEEKLY/MONTHLY RSI (ADDED 2026-08-26) -- see module docstring.
# ---------------------------------------------------------------------------

def fetch_period_closes(conn, source_table, exchange):
    """
    Fetch bhav_copy_w/bhav_copy_m rows for a single exchange, already
    exactly one row per (security_id, exchange, trade_date) -- these
    tables are themselves already security_id-keyed, isin-lineage-
    bridged rollups of bhav_copy_adjusted (see
    core/rollup/rollup_persistence.py's own 2026-08-26 repoint), so no
    further eligibility/tiebreak/adjustment work is needed here, same
    as fetch_bhav_copy_closes() above. Unlike bhav_copy_adjusted, these
    tables' isin column is literally named `isin` (not `source_isin`),
    so the SELECT list differs slightly from fetch_bhav_copy_closes()'s
    -- source_table is expected to be "bhav_copy_w" or "bhav_copy_m"
    only, never a table with a differently-named isin column.
    """
    try:
        query = f"""
            SELECT security_id, isin, exchange, series, symbol,
                   trade_date, close, prev_close
              FROM {source_table}
             WHERE exchange = %(exchange)s
             ORDER BY security_id, trade_date ASC
        """
        return pd.read_sql(query, conn, params={"exchange": exchange})
    except Exception as e:
        raise RsiPersistenceError(f"Failed to fetch {source_table} closes for {exchange}: {e}")


def upsert_rsi_workbook(conn, target_table, rsi_df):
    """
    Same upsert shape as upsert_rsi14d_workbook(), generalized over
    target_table ("rsi14w_workbook" or "rsi14m_workbook" -- both created
    with the exact same columns/key as rsi14d_workbook, see 011.02.00/
    011.03.00's changelogs). rsi_df must carry a `security_id` column
    (see compute_rsi14_all()'s output shape) -- a DataFrame without one
    will raise a KeyError here rather than silently write a broken row.
    """
    upsert_sql = f"""
        INSERT INTO {target_table} (security_id, isin, exchange, series, symbol, trade_date, gain, loss, avg_gain, avg_loss, rsi14)
        VALUES %s
        ON CONFLICT (security_id, exchange, trade_date) DO UPDATE SET
            isin = EXCLUDED.isin,
            series = EXCLUDED.series,
            symbol = EXCLUDED.symbol,
            gain = EXCLUDED.gain,
            loss = EXCLUDED.loss,
            avg_gain = EXCLUDED.avg_gain,
            avg_loss = EXCLUDED.avg_loss,
            rsi14 = EXCLUDED.rsi14
    """
    raw_values = (
        rsi_df[["security_id", "isin", "exchange", "series", "symbol", "trade_date", "gain", "loss", "avg_gain", "avg_loss", "rsi14"]]
        .itertuples(index=False, name=None)
    )
    values = [
        tuple(None if isinstance(v, float) and pd.isna(v) else v for v in row)
        for row in raw_values
    ]
    try:
        for start in range(0, len(values), COMMIT_EVERY_ROWS):
            chunk = values[start:start + COMMIT_EVERY_ROWS]
            with conn.cursor() as cur:
                execute_values(cur, upsert_sql, chunk, page_size=1000)
            conn.commit()
    except Exception as e:
        conn.rollback()
        raise RsiPersistenceError(f"Failed to upsert {target_table}: {e}")
    return len(values)
