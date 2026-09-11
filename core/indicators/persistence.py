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
#
# EXTENDED 2026-08-28 -- bhavcopy_scheduler_main.py needs a way to
# ACTIVATE an indicator (flip indicators_registry to ACTIVE, its own
# step 7) and listener/technicals/indicators_listener.py needs a way to
# DEACTIVATE one once its thread cleanly finishes a cycle (not just on
# failure, which record_failure() already handled).
#
# REVERTED 2026-08-29 -- 012.05.00 (START_TRADE_DATE/END_TRADE_DATE on
# indicators_workbook_metadata) is gone -- Sashikant asked for it back
# out after review found neither column was pulling its weight:
# START_TRADE_DATE was written every activation but never read by
# anything (listener/technicals/indicators_listener.py's own
# catch_up_indicator() discarded it into `_start_trade_date`), and
# END_TRADE_DATE only mattered for rsi14d's incremental walk -- and
# even there, fetch_complete_trade_dates_after() below already bounds
# that walk to whatever bhav_copy_metadata actually has, so the extra
# ceiling wasn't adding anything a fresher read of that same table
# wouldn't already give for free. activate_indicator() below is back to
# a plain status flip; fetch_iwm_date_range() (the listener's read side
# of those two columns) is deleted outright, not deprecated -- nothing
# calls it anymore. See claude/bhavcopy-scheduler-and-indicator-listener-2026-08-28.md
# (TrackMyTrade project) for the SQL that dropped the columns and the
# 012.05.00 changelog file/include.
#
# EXTENDED 2026-09-06 (BLIND-UPSERT REDESIGN) -- added
# fetch_iwm_freshness() below. bhavcopy_scheduler_main.py's STEP 7
# ("Indicators Registry Update") used to activate every registered
# indicator unconditionally every cycle (fetch_all_indicator_ids());
# per Sashikant's own confirmed call, activation is now gated PURELY on
# each indicator's own IWM freshness -- indicators_workbook_metadata.
# latest_trade_date IS NULL (never completed a run) or behind
# LATEST_TRADE_DATE. fetch_iwm_freshness() is the one query that answers
# that for every registered indicator in a single round trip, so the
# scheduler doesn't need to call fetch_iwm_cursor() (below) once per
# indicator_id in a loop.


from core.date_format import fmt_date


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


def fetch_all_indicator_ids(conn):
    """
    Returns every indicator_id registered in indicators_registry,
    regardless of status. Superseded, for bhavcopy_scheduler_main.py's
    own STEP 7 purposes, by fetch_iwm_freshness() below (which returns
    the same roster PLUS each one's own IWM freshness in one query) --
    kept here since other callers may still want just the bare roster.
    """
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT indicator_id FROM indicators_registry ORDER BY indicator_id")
            return [row[0] for row in cur.fetchall()]
    except Exception as e:
        raise IndicatorsPersistenceError(f"Failed to fetch registered indicators: {e}")


def fetch_iwm_freshness(conn):
    """
    Returns {indicator_id: latest_trade_date} for EVERY indicator_id
    registered in indicators_registry, LEFT JOINed against
    indicators_workbook_metadata (IWM) -- latest_trade_date is None for
    an indicator that has never completed a run (a fresh NULL IWM
    cursor). Every indicators_registry row has exactly one matching IWM
    row by construction (012.02.00's changelog comment -- one row per
    indicator_id, inserted at registration), so the LEFT JOIN is purely
    defensive; a genuinely missing IWM row still comes back as None
    here rather than raising, unlike fetch_iwm_cursor() below (which
    treats a missing row as a hard drift-out-of-sync error) -- STEP 7
    activation is better served by degrading a drifted row to "never
    run" than by aborting the whole cycle's indicator activation over
    one bad row.

    This is bhavcopy_scheduler_main.py's STEP 7
    ("Indicators Registry Update") sole basis for activation as of the
    2026-09-06 blind-upsert redesign -- see that file's
    _activate_indicators() for how the result is used: an indicator
    activates iff its own value here is None or behind
    LATEST_TRADE_DATE, with no reference to gap[] or any source table's
    freshness at all.
    """
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT r.indicator_id, iwm.latest_trade_date
                  FROM indicators_registry r
                  LEFT JOIN indicators_workbook_metadata iwm ON iwm.indicator_id = r.indicator_id
                 ORDER BY r.indicator_id
                """
            )
            return {row[0]: row[1] for row in cur.fetchall()}
    except Exception as e:
        raise IndicatorsPersistenceError(f"Failed to fetch indicators_workbook_metadata freshness: {e}")


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
        raise IndicatorsPersistenceError(f"Failed to record success for {indicator_id}/{fmt_date(trade_date)}: {e}")


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
        raise IndicatorsPersistenceError(f"Failed to record failure for {indicator_id}/{fmt_date(trade_date)}: {e}")


def activate_indicator(conn, indicator_id):
    """
    bhavcopy_scheduler_main.py's step 7 ("Indicators Registry Update")
    for ONE indicator -- flips indicators_registry.status to ACTIVE.
    A plain status flip is all step 7 does -- indicators_workbook_metadata
    itself is only ever updated by record_success()/record_bootstrap()/
    record_failure() above, once the indicator's own runner actually
    completes (or fails) its recompute.
    """
    try:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE indicators_registry SET status = 'ACTIVE' WHERE indicator_id = %s",
                (indicator_id,),
            )
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise IndicatorsPersistenceError(f"Failed to activate indicator {indicator_id}: {e}")


def deactivate_indicator(conn, indicator_id):
    """
    Flips indicators_registry.status back to DEACTIVE for indicator_id.
    Called by bhavcopy_scheduler_main.py's STEP 8 once an activated
    indicator's runner cleanly completes its cycle. Distinct from
    record_failure()'s own DEACTIVE flip: that one also opens an
    indicators_open_failures row; this one does not -- a clean
    completion is not a failure.
    """
    try:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE indicators_registry SET status = 'DEACTIVE' WHERE indicator_id = %s",
                (indicator_id,),
            )
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise IndicatorsPersistenceError(f"Failed to deactivate indicator {indicator_id}: {e}")


def fetch_complete_trade_dates_between(conn, start_date, end_date):
    """
    Same completeness rule as fetch_complete_trade_dates_after() above
    (both NSE and BSE show upload_status='SUCCESS' in bhav_copy_metadata
    for the date) but bounded on BOTH ends. NOTE: confirmed dead code as
    of the 2026-09-06 review -- nothing in this codebase calls it
    (kept, not deleted, in case a future incremental-window indicator
    runner wants it; see claude/bhavcopy-scheduler-end-to-end-flow-2026-09-06.md,
    TrackMyTrade project, for the correction this was originally
    believed to be load-bearing for rsi14d's own catch-up walk).
    """
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT trade_date FROM bhav_copy_metadata
                 WHERE upload_status = 'SUCCESS' AND trade_date >= %s AND trade_date <= %s
                 GROUP BY trade_date
                HAVING COUNT(DISTINCT exchange) = 2
                 ORDER BY trade_date ASC
                """,
                (start_date, end_date),
            )
            return [row[0] for row in cur.fetchall()]
    except Exception as e:
        raise IndicatorsPersistenceError(f"Failed to fetch complete trade dates between {fmt_date(start_date)} and {fmt_date(end_date)}: {e}")
