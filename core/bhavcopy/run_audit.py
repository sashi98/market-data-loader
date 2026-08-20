# core/bhavcopy/run_audit.py
#
# Write-only helper for bhav_copy_run_audit -- the "run report" table
# bhav_copy_schedule_listener.py writes to after each check that
# produces something worth recording: an actual processing attempt
# (OK/SKIPPED/FAILED), a multi-day gap it refused to auto-advance past
# (WAITING), a holiday-calendar lookup failure (WARN), or an exchange
# with no prior successfully-processed date at all yet (NO_HISTORY).
#
# Pure no-op checks ("already caught up, nothing due") are deliberately
# NOT recorded -- at the listener's 15-minute poll interval across a
# 14-hour active window, logging every idle tick would produce ~100+
# near-identical "nothing happened" rows per night for zero information
# value. See the listener's own comments for the full reasoning.
#
# Table schema (see tmt's
# db/changelog/016.01.00-changelog_create_table_bhav_copy_run_audit.xml,
# the real source of truth -- this comment just mirrors it for quick
# reference):
#   bhav_copy_run_audit: id, exchange, ceiling_date, latest_success_date,
#     target_date, pending_trading_days, pending_dates, outcome,
#     message, processing_time_ms, created_at
#
# Written DIRECTLY from Python via psycopg2 -- unlike every actual
# bhav-copy WRITE (which goes through tmt's REST API so
# BhavCopyProcessingService remains the one place that owns
# bhav_copy_metadata/bhav_copy), this listener owns bhav_copy_run_audit
# itself end-to-end, by explicit choice: it's purely a log of this
# listener's own decisions, not shared application state tmt's Java
# side needs to reason about.

class RunAuditError(Exception):
    """Raised if writing a bhav_copy_run_audit row fails."""
    pass


def record_run(conn, exchange, ceiling_date, latest_success_date, target_date,
                pending_trading_days, pending_dates, outcome, message,
                processing_time_ms=None):
    """
    Inserts one row into bhav_copy_run_audit and commits. Opens no
    connection itself -- caller passes one (bhav_copy_schedule_listener.py
    opens a short-lived connection just for this write, right after each
    exchange's outcome is decided -- see that module's own comments).

    `pending_dates` should be a list of datetime.date, or None/[] --
    stored as a comma-separated DD-MMM-YYYY string, or NULL if empty.

    Raises RunAuditError on failure. The caller decides whether that
    should ever block anything -- currently: no. A failed audit write is
    logged and swallowed by the caller, never allowed to interrupt the
    actual bhav-copy check/processing flow it's reporting on.
    """
    pending_dates_str = None
    if pending_dates:
        pending_dates_str = ", ".join(d.strftime("%d-%b-%Y") for d in pending_dates)

    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO bhav_copy_run_audit
                    (exchange, ceiling_date, latest_success_date, target_date,
                     pending_trading_days, pending_dates, outcome, message,
                     processing_time_ms, created_at)
                VALUES
                    (%s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
                """,
                (exchange, ceiling_date, latest_success_date, target_date,
                 pending_trading_days, pending_dates_str, outcome, message,
                 processing_time_ms),
            )
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise RunAuditError(f"Failed to write bhav_copy_run_audit row for {exchange}: {e}")
