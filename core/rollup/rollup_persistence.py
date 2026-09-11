# core/rollup/rollup_persistence.py
#
# DB access for Story A's weekly/monthly OHLC rollup (bhav_copy_w /
# bhav_copy_m + their metadata/run_audit companions -- see
# claude/user-story-weekly-monthly-ohlc-rsi-candlestick.md, this
# project's Claude Project docs). Table names are passed in by the
# caller (bhav_copy_w vs bhav_copy_m, etc.) so this module's logic is
# SHARED between the weekly and monthly paths, not duplicated --
# see loaders/bhavcopy/bhav_copy_w_loader.py / bhav_copy_m_loader.py and
# bhav_copy_wm_rollup_listener.py for the thin per-period-type entry
# points that call into this module.
#
# REPOINTED 2026-08-26 (same security_id migration as
# core/rsi/rsi_persistence.py -- see
# claude/bhav-copy-adjusted-clean-price-series-design-2026-08-23.md in
# the project docs) -- this used to query raw bhav_copy directly,
# reapplying RSI's own eligibility/same-day-tiebreak SQL fragments
# (core/rsi/rsi_continuity.py's ELIGIBLE_SERIES_EXISTS_SQL /
# MIN_LIQUIDITY_FILTER_SQL / TIEBREAK_RANK_SQL) at read time, grouped
# by (isin, exchange). All of that -- eligibility filtering, same-day
# tiebreak, isin-lineage bridging, and split/bonus adjustment -- is now
# done ONCE, upstream, by loaders/bhavcopy/bhav_copy_d_adjustment_loader.py
# when it (re)builds bhav_copy_adjusted (see
# core/price_series/adjusted_series.py). This module now just reads
# that table's already-clean, already-continuous output -- no
# adjustment join, no eligibility EXISTS check, no same-day tiebreak
# needed here anymore, exactly mirroring core/rsi/rsi_persistence.py's
# own repoint.
#
# Grouping/keying moves from (isin, exchange) to (security_id,
# exchange) throughout -- a stock whose isin changed mid-history (e.g.
# MWL's SME-to-mainboard migration) now rolls up as ONE continuous
# weekly/monthly series instead of fragmenting into a dead old-isin
# series plus a freshly-reseeded new-isin series, same fix as RSI's
# own. ISIN is still carried through as an ordinary audit column (which
# source isin a given trade_date's row actually traded under -- for a
# rolled-up period, the LAST trading day's isin), but it is no longer
# part of the uniqueness key.

import pandas as pd
from psycopg2.extras import execute_values

from core.date_format import fmt_date


class RollupPersistenceError(Exception):
    """Raised when fetching daily rows, or reading/writing any rollup table, fails."""
    pass


ADJUSTED_ROWS_SQL = """
    SELECT security_id, source_isin AS isin, exchange, series, symbol, trade_date,
           open, high, low, close, last, tot_trd_qty, tot_trd_val, total_trades
      FROM bhav_copy_adjusted
     WHERE exchange = %(exchange)s
"""


def fetch_earliest_trade_date(conn, exchange):
    """
    MIN(trade_date) in bhav_copy_adjusted for one exchange -- None if
    that exchange has no rows there yet. ADDED 2026-08-30 so
    _run_rollup_step() (bhavcopy_listener.py) can bootstrap a NULL
    rollup cursor itself instead of requiring a separate manual bulk
    backfill loader first -- see that function's own comment.
    """
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT MIN(trade_date) FROM bhav_copy_adjusted WHERE exchange = %s", (exchange,))
            row = cur.fetchone()
            return row[0] if row and row[0] is not None else None
    except Exception as e:
        raise RollupPersistenceError(f"Failed to find earliest bhav_copy_adjusted trade date for {exchange}: {e}")


def fetch_daily_rows(conn, exchange, from_date=None, to_date=None):
    """
    bhav_copy_adjusted rows for one exchange -- optionally bounded to
    [from_date, to_date] (inclusive both ends; either or both may be
    None for an open-ended bound). Already exactly one row per
    (security_id, trade_date): eligibility, same-day tiebreak,
    isin-lineage bridging, and split/bonus adjustment are all baked in
    upstream by loaders/bhavcopy/bhav_copy_d_adjustment_loader.py -- see
    module docstring. Part 1 calls this with no bounds (full history,
    one exchange at a time, same memory-pressure precedent as
    rsi14d_loader.py); Part 2 calls it bounded to just the single
    period it is rolling up.
    """
    query = ADJUSTED_ROWS_SQL
    params = {"exchange": exchange}
    if from_date is not None:
        query += " AND trade_date >= %(from_date)s"
        params["from_date"] = from_date
    if to_date is not None:
        query += " AND trade_date <= %(to_date)s"
        params["to_date"] = to_date
    query += " ORDER BY security_id, trade_date"
    try:
        return pd.read_sql(query, conn, params=params)
    except Exception as e:
        raise RollupPersistenceError(f"Failed to fetch bhav_copy_adjusted rows for {exchange}: {e}")


def fetch_prior_close_lookup(conn, rollup_table, security_id_exchange_period_starts):
    """
    For a batch of (security_id, exchange, period_start) keys whose
    PRECEDING period is not part of the current call's own input (the
    Part 2 listener's normal case -- it only ever fetches one new
    period's worth of daily rows per call), looks up each one's prior
    period's close directly from rollup_table itself: the most recent
    existing row for that security_id+exchange with trade_date <
    period_start.

    Returns {(security_id, exchange, period_start): prior_close}. A key
    with no prior row at all (the security's first-ever period in this
    table) is simply absent from the returned dict -- prevClose
    correctly stays NULL for it, not an error.
    """
    if not security_id_exchange_period_starts:
        return {}

    lookup = {}
    try:
        with conn.cursor() as cur:
            for security_id, exchange, p_start in security_id_exchange_period_starts:
                cur.execute(
                    f"""
                    SELECT close FROM {rollup_table}
                     WHERE security_id = %s AND exchange = %s AND trade_date < %s
                     ORDER BY trade_date DESC
                     LIMIT 1
                    """,
                    (security_id, exchange, p_start),
                )
                row = cur.fetchone()
                if row is not None and row[0] is not None:
                    # CAST TO float HERE -- raw psycopg2 cursors return
                    # Postgres NUMERIC columns as Python decimal.Decimal,
                    # unlike pd.read_sql() (used by fetch_daily_rows()
                    # above), which auto-converts numeric columns to
                    # float64. Without this cast, compute_rollup() ends up
                    # subtracting a Decimal from a float64 "close" column
                    # in _percent_change() -- "TypeError: unsupported
                    # operand type(s) for -: 'float' and 'decimal.Decimal'"
                    # -- confirmed via a live crash on period 2 of a
                    # weekly rollup bootstrap (period 1 never hit this,
                    # since it has no prior close to look up at all).
                    lookup[(security_id, exchange, p_start)] = float(row[0])
    except Exception as e:
        raise RollupPersistenceError(f"Failed to fetch prior-close lookup from {rollup_table}: {e}")
    return lookup


UPSERT_COLUMNS = [
    "security_id", "isin", "exchange", "series", "symbol", "open", "high", "low", "close", "last",
    "prev_close", "tot_trd_qty", "tot_trd_val", "trade_date", "total_trades", "ltp_percent_change",
]


def upsert_rollup_rows(conn, rollup_table, rollup_df):
    """
    Batch upserts rollup_df (as produced by
    rollup_calculator.compute_rollup()) into rollup_table, keyed on
    (security_id, exchange, trade_date) -- the table's own unique
    constraint (002.04.00 for bhav_copy_w / 002.05.00 for bhav_copy_m).
    Commits on success; caller must rollback() on RollupPersistenceError.
    """
    if rollup_df.empty:
        return 0

    raw_values = rollup_df[UPSERT_COLUMNS].itertuples(index=False, name=None)
    values = [
        tuple(None if isinstance(v, float) and pd.isna(v) else v for v in row)
        for row in raw_values
    ]

    columns_sql = ", ".join(UPSERT_COLUMNS)
    upsert_sql = f"""
        INSERT INTO {rollup_table} ({columns_sql}) VALUES %s
        ON CONFLICT (security_id, exchange, trade_date) DO UPDATE SET
            isin = EXCLUDED.isin,
            series = EXCLUDED.series,
            symbol = EXCLUDED.symbol,
            open = EXCLUDED.open,
            high = EXCLUDED.high,
            low = EXCLUDED.low,
            close = EXCLUDED.close,
            last = EXCLUDED.last,
            prev_close = EXCLUDED.prev_close,
            tot_trd_qty = EXCLUDED.tot_trd_qty,
            tot_trd_val = EXCLUDED.tot_trd_val,
            total_trades = EXCLUDED.total_trades,
            ltp_percent_change = EXCLUDED.ltp_percent_change
    """
    try:
        with conn.cursor() as cur:
            execute_values(cur, upsert_sql, values, page_size=1000)
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise RollupPersistenceError(f"Failed to upsert {rollup_table}: {e}")
    return len(values)


def upsert_metadata(conn, metadata_table, trade_date, exchange, total_stocks, period_end_date,
                     processing_time_ms=None, status="SUCCESS", error_message=None):
    """
    Upserts one bhav_copy_w_metadata/bhav_copy_m_metadata row for
    (trade_date, exchange) -- same upsert-by-natural-key shape as daily
    bhavcopy_persistence._upsert_metadata(), minus file_name (no source
    CSV for a rollup run). Commits immediately.

    ADDED 2026-08-30 -- period_start_date/period_end_date (017.01.00/
    017.02.00's own migration). period_start_date is written as the
    SAME value as trade_date -- deliberate duplication, not a new
    concept: trade_date already means "period start" on this table
    (the chart's own convention), period_start_date just makes that
    explicit without renaming the column the chart relies on. caller
    passes period_end_date explicitly (rollup_runner.py's
    _roll_up_one_period() already computes this -- next_period_start()
    minus one day -- so no new computation needed here, just threading
    the existing value through).
    """
    try:
        with conn.cursor() as cur:
            cur.execute(f"SELECT id FROM {metadata_table} WHERE trade_date = %s AND exchange = %s",
                        (trade_date, exchange))
            existing = cur.fetchone()
            if existing:
                cur.execute(
                    f"""
                    UPDATE {metadata_table}
                       SET upload_status = %s, total_stocks = %s, processing_time_ms = %s,
                           error_message = %s, period_start_date = %s, period_end_date = %s,
                           updated_at = CURRENT_TIMESTAMP
                     WHERE id = %s
                    """,
                    (status, total_stocks, processing_time_ms, error_message,
                     trade_date, period_end_date, existing[0]),
                )
            else:
                cur.execute(
                    f"""
                    INSERT INTO {metadata_table}
                        (trade_date, exchange, upload_status, total_stocks, processing_time_ms,
                         error_message, period_start_date, period_end_date, created_at, updated_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                    """,
                    (trade_date, exchange, status, total_stocks, processing_time_ms, error_message,
                     trade_date, period_end_date),
                )
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise RollupPersistenceError(f"Failed to upsert {metadata_table} for {fmt_date(trade_date)}/{exchange}: {e}")


def get_metadata_freshness(conn, metadata_table, exchange):
    """
    MAX(period_end_date) WHERE upload_status = 'SUCCESS' for this
    exchange -- an O(1) answer to "how far has this table's rollup
    actually gotten", in calendar-day terms rather than period-start
    terms. ADDED 2026-08-30 alongside 002.04.03/002.05.03's own
    period_end_date column. REMOVED 2026-08-30 (fourth pass):
    get_metadata_cursor() (MAX(trade_date), used to resume an
    incremental "only new periods" walk) -- rollup_runner.py no longer
    resumes incrementally at all, it walks EVERY period from scratch
    every run, gated only by THIS function deciding skip-vs-recompute
    for the whole run.
    """
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT MAX(period_end_date) FROM {metadata_table} WHERE exchange = %s AND upload_status = 'SUCCESS'",
                (exchange,),
            )
            row = cur.fetchone()
            return row[0] if row and row[0] is not None else None
    except Exception as e:
        raise RollupPersistenceError(f"Failed to read freshness from {metadata_table} for {exchange}: {e}")


def record_run_audit(conn, run_audit_table, exchange, period_trade_date, run_started_at,
                      run_finished_at, isin_count, row_count, status, error_message=None):
    """
    Inserts one bhav_copy_w_run_audit/bhav_copy_m_run_audit row.
    Simplified shape (no gap-detection columns) -- see
    db/changelog/002.04.02's comment for why. Commits immediately; a
    failure here is logged and swallowed by the caller (same "an audit
    write failure never blocks the actual rollup" convention as daily
    bhav_copy_run_audit's record_run()), never raised further up to
    interrupt the rollup itself.

    isin_count param name matches the run_audit table's own isin_count
    column (002.04.02) -- callers now pass a distinct-security_id count
    into it (security_id is the rollup's real grouping key as of the
    2026-08-26 repoint), not a literal isin count. Left as isin_count
    here rather than renamed, since renaming the column itself wasn't
    part of this change; a future changelog could rename it to
    security_id_count for accuracy.
    """
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                INSERT INTO {run_audit_table}
                    (exchange, period_trade_date, run_started_at, run_finished_at,
                     isin_count, row_count, status, error_message, created_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
                """,
                (exchange, period_trade_date, run_started_at, run_finished_at,
                 isin_count, row_count, status, error_message),
            )
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise RollupPersistenceError(
            f"Failed to write {run_audit_table} row for {fmt_date(period_trade_date)}/{exchange}: {e}"
        )
