# core/bhavcopy/bhavcopy_persistence.py
#
# NSE and BSE are always treated as completely independent datasets --
# each exchange's rows are simply batch-inserted as their own rows,
# exactly as-is, every time. No cross-exchange lookup, no merge, no
# update case at all -- mirrors BhavCopyPersistenceServiceHandler.java's
# insertBhavCopies() exactly.
#
# This deliberately replaces an earlier two-tier NSE/BSE merge design
# (see git history for SERIES_MERGE_GROUPS / _resolve_merge_group /
# MULTI_CODE_GROUP_LABELS if that reasoning is ever needed again) --
# that design tried to collapse a dual-listed stock's NSE and BSE rows
# into one row per (isin, trade_date), but only ever appended to the
# `exchange` string on a successful merge. BSE's own OHLC data was
# silently never written anywhere. Rather than fix the series-equivalence
# guess that design depended on, the merge concept itself was removed --
# bhav_copy now always holds one row per exchange per (isin, trade_date),
# with no attempt to decide which exchange's price "wins".
#
#   Metadata (bhav_copy_metadata) is written ONLY after all bhav_copy rows
#   for that exchange/date persist successfully -- upsert by
#   (trade_date, exchange), matching the unique constraint.
#
#   Each of persist_nse() / persist_bse() runs as ONE transaction (caller
#   passes a psycopg2 connection with autocommit=False -- see db_client.py).
#   On any exception, the caller is expected to conn.rollback(); on success,
#   these functions call conn.commit() themselves before returning.
#
# Table schema (confirmed against Liquibase changelogs, not just entities):
#   bhav_copy:          id (default nextval('bhav_copy_seq')), symbol,
#                        exchange, series, open, high, low, close, last,
#                        prev_close, tot_trd_qty, tot_trd_val, trade_date,
#                        total_trades, ltp_percent_change, isin
#   bhav_copy_metadata: id, trade_date, exchange, upload_status,
#                        total_stocks, file_name, processing_time_ms,
#                        error_message, created_at, updated_at
#                        UNIQUE(trade_date, exchange)

import os
import time

from psycopg2.extras import execute_values


class BhavCopyPersistenceError(Exception):
    """Raised when NSE/BSE persistence fails. Caller should roll back the transaction."""
    pass


def is_already_persisted(conn, trade_date, exchange_code):
    """
    Checks bhav_copy_metadata for an existing SUCCESS row for this
    (trade_date, exchange). Used by the loader to make re-running a range
    idempotent -- persist_nse() always does a fresh batch insert with no
    duplicate check (matching BhavCopyPersistenceServiceHandler.java,
    where this only ever runs once per date via a manual UI click), so
    callers MUST skip already-persisted dates themselves before calling
    persist_nse()/persist_bse() again, or re-running a range will insert
    duplicate rows for every date that already succeeded.
    """
    with conn.cursor() as cur:
        cur.execute(
            "SELECT 1 FROM bhav_copy_metadata WHERE trade_date = %s AND exchange = %s AND upload_status = 'SUCCESS'",
            (trade_date, exchange_code),
        )
        return cur.fetchone() is not None


def get_latest_success_date(conn, exchange_code):
    """
    Returns the most recent trade_date with a SUCCESS row in
    bhav_copy_metadata for this exchange, or None if this exchange has
    no successfully-processed date at all yet (e.g. a brand-new database,
    before the historical backfill loader -- bhav_copy_with_corporate_
    action_loader.py -- has ever been run for it).

    ADDED 2026-08-20 for bhav_copy_schedule_listener.py's continuity
    check: the scheduler needs to know exactly where NSE/BSE's
    successfully-integrated history currently ends, per exchange, before
    it can decide whether the next date it's about to fetch would leave
    a gap. See that listener's own comments for the full reasoning --
    this is a plain read-only lookup, no other caller needed it before.
    """
    with conn.cursor() as cur:
        cur.execute(
            "SELECT MAX(trade_date) FROM bhav_copy_metadata WHERE exchange = %s AND upload_status = 'SUCCESS'",
            (exchange_code,),
        )
        row = cur.fetchone()
        return row[0] if row and row[0] is not None else None


INSERT_COLUMNS = [
    "symbol", "exchange", "series", "open", "high", "low", "close", "last",
    "prev_close", "tot_trd_qty", "tot_trd_val", "trade_date", "total_trades",
    "isin", "ltp_percent_change",
]


def _row_to_tuple(row):
    return (
        row["symbol"], row["exchange"], row["series"], row["open"], row["high"],
        row["low"], row["close"], row["last"], row["prevClose"], row["totTrdQty"],
        row["totTrdVal"], row["tradeDate"], row["totalTrades"], row["isin"],
        row["ltpPercentChange"],
    )


def _upsert_metadata(cur, trade_date, exchange_code, total_stocks, file_name, processing_time_ms):
    cur.execute(
        "SELECT id FROM bhav_copy_metadata WHERE trade_date = %s AND exchange = %s",
        (trade_date, exchange_code),
    )
    existing = cur.fetchone()

    if existing:
        cur.execute(
            """
            UPDATE bhav_copy_metadata
               SET upload_status = 'SUCCESS',
                   total_stocks = %s,
                   file_name = %s,
                   processing_time_ms = %s,
                   error_message = NULL,
                   updated_at = CURRENT_TIMESTAMP
             WHERE id = %s
            """,
            (total_stocks, file_name, processing_time_ms, existing[0]),
        )
    else:
        cur.execute(
            """
            INSERT INTO bhav_copy_metadata
                (trade_date, exchange, upload_status, total_stocks, file_name,
                 processing_time_ms, error_message, created_at, updated_at)
            VALUES
                (%s, %s, 'SUCCESS', %s, %s, %s, NULL, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            """,
            (trade_date, exchange_code, total_stocks, file_name, processing_time_ms),
        )


def persist_nse(conn, parsed_rows, trade_date, csv_file_path, start_time_ms):
    """
    Batch-inserts all parsed NSE rows into bhav_copy, then writes
    bhav_copy_metadata (exchange='NSE') on success.

    Runs as a single transaction -- commits on success, caller must
    rollback() on BhavCopyPersistenceError.

    Raises BhavCopyPersistenceError on any failure (transaction NOT
    rolled back here -- caller does that, matching the NSE/BSE
    independent-transaction contract).
    """
    try:
        with conn.cursor() as cur:
            values = [_row_to_tuple(row) for row in parsed_rows]
            columns_sql = ", ".join(INSERT_COLUMNS)
            execute_values(
                cur,
                f"INSERT INTO bhav_copy ({columns_sql}) VALUES %s",
                values,
            )

            processing_time_ms = int((time.time() * 1000) - start_time_ms)
            _upsert_metadata(
                cur, trade_date, "NSE", len(parsed_rows),
                os.path.basename(csv_file_path), processing_time_ms,
            )

        conn.commit()
    except Exception as e:
        conn.rollback()
        raise BhavCopyPersistenceError(f"NSE persistence failed for {trade_date}: {e}")


def persist_bse(conn, parsed_rows, trade_date, csv_file_path, start_time_ms):
    """
    Batch-inserts all parsed BSE rows into bhav_copy, then writes
    bhav_copy_metadata (exchange='BSE') on success.

    NSE and BSE are always treated as completely independent datasets --
    no cross-exchange lookup, no merge, no update case at all. This
    deliberately replaces the old two-tier NSE/BSE merge logic (see git
    history for SERIES_MERGE_GROUPS / _resolve_merge_group /
    MULTI_CODE_GROUP_LABELS if that reasoning is ever needed again) --
    that logic silently discarded BSE's own OHLC data on every
    successful merge, only ever appending to the exchange string, never
    writing BSE's actual price fields.

    Runs as a single transaction, independent from persist_nse()'s
    transaction -- commits on success, rolls back and raises
    BhavCopyPersistenceError on any failure.
    """
    try:
        with conn.cursor() as cur:
            values = [_row_to_tuple(row) for row in parsed_rows]
            columns_sql = ", ".join(INSERT_COLUMNS)
            execute_values(
                cur,
                f"INSERT INTO bhav_copy ({columns_sql}) VALUES %s",
                values,
            )

            processing_time_ms = int((time.time() * 1000) - start_time_ms)
            _upsert_metadata(
                cur, trade_date, "BSE", len(parsed_rows),
                os.path.basename(csv_file_path), processing_time_ms,
            )

        conn.commit()
    except Exception as e:
        conn.rollback()
        raise BhavCopyPersistenceError(f"BSE persistence failed for {trade_date}: {e}")
