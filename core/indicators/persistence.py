# core/indicators/persistence.py
#
# DB access for the Indicators Framework's own bookkeeping tables --
# indicators_registry, indicators_workbook_metadata (IWM),
# indicators_open_failures -- plus the "which BhavCopy dates are fully
# integrated" query indicators_listener.py polls on.
#
# Every write here is an explicit, direct conn.commit() call -- same
# philosophy as core/rsi/rsi_persistence.py, and the whole reason this
# framework replaced the abandoned Java RsiDailyUpdateListener (see
# docs/epics/indicators-framework/session-handover-12-jul-2026.md for
# the full story: its writes, made from inside a Spring
# @TransactionalEventListener(AFTER_COMMIT) callback, were silently
# never committing at all).

class IndicatorsPersistenceError(Exception):
    """Raised when any Indicators Framework bookkeeping query/write fails."""
    pass


def fetch_active_indicators(conn):
    """Returns a list of indicator_id strings for every ACTIVE row in indicators_registry."""
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT indicator_id FROM indicators_registry WHERE status = 'ACTIVE'")
            return [row[0] for row in cur.fetchall()]
    except Exception as e:
        raise IndicatorsPersistenceError(f"Failed to fetch active indicators: {e}")


def fetch_iwm_cursor(conn, indicator_id):
    """
    Returns indicator_id's indicators_workbook_metadata.latest_trade_date,
    or None if it has not run yet under this framework (a fresh NULL
    cursor -- see dispatch.py's bootstrap contract for what happens next).

    Raises IndicatorsPersistenceError if indicator_id has no IWM row at
    all -- every indicators_registry row must have a matching IWM row
    (012.02.00's changelog comment); a missing one means the two tables
    have drifted out of sync, a real problem, not something to silently
    paper over.
    """
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT latest_trade_date FROM indicators_workbook_metadata WHERE indicator_id = %s",
                (indicator_id,),
            )
            row = cur.fetchone()
    except Exception as e:
        raise IndicatorsPersistenceError(f"Failed to fetch IWM cursor for {indicator_id}: {e}")

    if row is None:
        raise IndicatorsPersistenceError(
            f"indicator_id={indicator_id} has no indicators_workbook_metadata row at all -- "
            f"registry and IWM have drifted out of sync."
        )
    return row[0]


def fetch_complete_trade_dates_after(conn, after_date):
    """
    Returns the ordered list (ascending) of trade_dates strictly after
    after_date where BOTH NSE and BSE show upload_status='SUCCESS' in
    bhav_copy_metadata. If after_date is None, returns every complete
    date in the table's entire history.

    Deliberately sourced from bhav_copy_metadata's own actual upload
    history rather than an independent weekend/holiday calendar -- this
    can never drift out of sync with which days the market actually had
    data, unlike a separately-maintained calendar could.
    """
    try:
        with conn.cursor() as cur:
            if after_date is None:
                cur.execute(
                    """
                    SELECT trade_date FROM bhav_copy_metadata
                     WHERE upload_status = 'SUCCESS'
                     GROUP BY trade_date
                    HAVING COUNT(DISTINCT exchange) = 2
                     ORDER BY trade_date ASC
                    """
                )
            else:
                cur.execute(
                    """
                    SELECT trade_date FROM bhav_copy_metadata
                     WHERE upload_status = 'SUCCESS' AND trade_date > %s
                     GROUP BY trade_date
                    HAVING COUNT(DISTINCT exchange) = 2
                     ORDER BY trade_date ASC
                    """,
                    (after_date,),
                )
            return [row[0] for row in cur.fetchall()]
    except Exception as e:
        raise IndicatorsPersistenceError(f"Failed to fetch complete trade dates: {e}")


def record_success(conn, indicator_id, trade_date):
    """
    Advances IWM.latest_trade_date to trade_date for indicator_id, and
    deletes any indicators_open_failures row for (indicator_id,
    trade_date) -- covers both the normal-success case and the
    retry-succeeded case in one place. Commits immediately.
    """
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE indicators_workbook_metadata
                   SET latest_trade_date = %s, status = 'SUCCESS', date_of_run = now()
                 WHERE indicator_id = %s
                """,
                (trade_date, indicator_id),
            )
            cur.execute(
                "DELETE FROM indicators_open_failures WHERE indicator_id = %s AND trade_date = %s",
                (indicator_id, trade_date),
            )
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise IndicatorsPersistenceError(f"Failed to record success for {indicator_id}/{trade_date}: {e}")


def record_bootstrap(conn, indicator_id, trade_date):
    """
    Same write as record_success() -- kept as its own named function
    even though the SQL is identical, so the two call sites in
    indicators_listener.py stay readable about WHY each write is
    happening (auto-bootstrapping a NULL cursor vs. a real incremental
    success).
    """
    record_success(conn, indicator_id, trade_date)


def record_failure(conn, indicator_id, trade_date, error_message):
    """
    Upserts an indicators_open_failures row for (indicator_id,
    trade_date) with error_message, and flips
    indicators_registry.status to DEACTIVE for indicator_id. Commits
    immediately.
    """
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO indicators_open_failures (indicator_id, trade_date, error_message, failed_at)
                VALUES (%s, %s, %s, now())
                ON CONFLICT (indicator_id, trade_date) DO UPDATE SET
                    error_message = EXCLUDED.error_message,
                    failed_at = EXCLUDED.failed_at
                """,
                (indicator_id, trade_date, error_message),
            )
            cur.execute(
                "UPDATE indicators_registry SET status = 'DEACTIVE' WHERE indicator_id = %s",
                (indicator_id,),
            )
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise IndicatorsPersistenceError(f"Failed to record failure for {indicator_id}/{trade_date}: {e}")
