# core/bhavcopy_persistence.py
#
# Mirrors BhavCopyPersistenceServiceHandler.java exactly:
#
#   NSE -- batch insert all rows fresh (NSE always processed first, no
#          duplicates expected against an empty date).
#   BSE -- for each row, look up existing bhav_copy by (isin, trade_date):
#            found     -- if existing.exchange doesn't already contain the
#                          new row's exchange value (case-insensitive),
#                          append ", {new_exchange}" to it. If it already
#                          contains it, skip (no update).
#            not found -- insert fresh, exchange = new row's own value.
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
    Upserts all parsed BSE rows into bhav_copy (merge into existing NSE
    row by appending to `exchange`, or insert fresh if not found), then
    writes bhav_copy_metadata (exchange='BSE') on success.

    Batched (not row-by-row like the Java forEach it mirrors, for
    performance over many dates), but functionally identical:
      1. One SELECT -- fetch all existing bhav_copy rows for trade_date,
         keyed by isin (first-seen id wins if duplicates exist, matching
         the ORDER BY id ASC LIMIT 1 behavior a row-by-row lookup would give).
      2. Partition parsed_rows into to_insert (isin not found) and
         to_update (isin found, existing exchange doesn't already contain
         the new value).
      3. One batch INSERT for to_insert.
      4. One batch UPDATE (via UPDATE ... FROM (VALUES ...)) for to_update.

    Runs as a single transaction, independent from persist_nse()'s
    transaction -- commits on success, rolls back and raises
    BhavCopyPersistenceError on any failure.
    """
    try:
        with conn.cursor() as cur:
            # -- 1. Fetch existing rows for this trade_date, keyed by isin --
            cur.execute(
                "SELECT id, isin, exchange FROM bhav_copy WHERE trade_date = %s ORDER BY id ASC",
                (trade_date,),
            )
            existing_by_isin = {}
            for row_id, isin, exchange in cur.fetchall():
                if isin not in existing_by_isin:
                    existing_by_isin[isin] = (row_id, exchange)

            # -- 2. Partition --
            to_insert = []
            to_update = []  # (id, new_exchange_value)

            for row in parsed_rows:
                existing = existing_by_isin.get(row["isin"])
                if existing is None:
                    to_insert.append(row)
                else:
                    existing_id, existing_exchange = existing
                    new_exchange = row["exchange"]
                    if new_exchange and new_exchange.lower() not in (existing_exchange or "").lower():
                        to_update.append((existing_id, f"{existing_exchange}, {new_exchange}"))
                    # else: already contains it -- no-op, matches Java's skip behavior

            # -- 3. Batch insert --
            if to_insert:
                values = [_row_to_tuple(row) for row in to_insert]
                columns_sql = ", ".join(INSERT_COLUMNS)
                execute_values(
                    cur,
                    f"INSERT INTO bhav_copy ({columns_sql}) VALUES %s",
                    values,
                )

            # -- 4. Batch update --
            if to_update:
                execute_values(
                    cur,
                    """
                    UPDATE bhav_copy AS bc
                       SET exchange = v.new_exchange
                      FROM (VALUES %s) AS v (id, new_exchange)
                     WHERE bc.id = v.id
                    """,
                    to_update,
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
