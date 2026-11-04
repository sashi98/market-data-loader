# core/recompute/queue.py -- TMT-MDL-US-0005
#
# recompute_queue (tmt changelog 026.01.00): stocks a downstream step must rebuild in FULL because their
# history changed upstream. STEP 4 queues its list-B stocks for every downstream target; each downstream
# step reads its own rows, rebuilds those stocks, and only then deletes the rows -- so a crashed run leaves
# them queued and the rebuild is retried next run. security_id = ALL ('*') means the whole exchange.

ALL = "*"

TARGET_PRICE_ADJUSTMENT = "PRICE_ADJUSTMENT"   # admin-forced full recompute of a stock from STEP 4 onwards
TARGET_WEEKLY_ROLLUP = "WEEKLY_ROLLUP"
TARGET_MONTHLY_ROLLUP = "MONTHLY_ROLLUP"
TARGET_RSI14D = "RSI14D"
TARGET_RSI14W = "RSI14W"
TARGET_RSI14M = "RSI14M"

# Targets STEP 4 feeds when a stock's adjusted history was rebuilt. The weekly / monthly RSI are fed by
# the weekly / monthly rollup instead (DOWNSTREAM_OF_ROLLUP), only once that rollup has rebuilt the stock --
# so a failed rollup never lets RSI rebuild from stale weekly / monthly rows and then forget the stock.
DOWNSTREAM_OF_PRICE_ADJUSTMENT = (TARGET_WEEKLY_ROLLUP, TARGET_MONTHLY_ROLLUP, TARGET_RSI14D)
DOWNSTREAM_OF_ROLLUP = {TARGET_WEEKLY_ROLLUP: TARGET_RSI14W, TARGET_MONTHLY_ROLLUP: TARGET_RSI14M}

# A step writes this reason under ALL when it starts and deletes it only when it finished: a run that dies
# midway leaves it, and the next run rebuilds the whole exchange for that step.
RUN_INCOMPLETE = "RUN_INCOMPLETE"


class RecomputeQueueError(Exception):
    pass


def enqueue(conn, exchange, targets, reasons_by_security, detail=None):
    """
    reasons_by_security: {security_id: reason}. Inserts one row per (security, target); an existing row
    for the same (security_id, exchange, target) keeps its place and gets the newer reason. Commits.
    """
    if not reasons_by_security or not targets:
        return 0
    rows = [(sid, exchange, target, reason, detail)
            for sid, reason in reasons_by_security.items() for target in targets]
    try:
        with conn.cursor() as cur:
            for row in rows:
                cur.execute(
                    """
                    INSERT INTO recompute_queue (security_id, exchange, target, reason, detail, queued_at)
                    VALUES (%s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
                    ON CONFLICT (security_id, exchange, target)
                    DO UPDATE SET reason = EXCLUDED.reason, detail = EXCLUDED.detail
                    """,
                    row,
                )
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise RecomputeQueueError(f"Failed to queue {len(rows)} recompute row(s) for {exchange}: {e}")
    return len(rows)


def fetch_queued(conn, exchange, target):
    """{security_id: reason} queued for (exchange, target); may contain ALL."""
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT security_id, reason FROM recompute_queue WHERE exchange = %s AND target = %s",
                (exchange, target),
            )
            return {sid: reason for sid, reason in cur.fetchall()}
    except Exception as e:
        raise RecomputeQueueError(f"Failed to read recompute_queue for {exchange}/{target}: {e}")


def clear(conn, exchange, target, security_ids):
    """Delete the rows for these security_ids (ALL included if passed) once they have been rebuilt. Commits."""
    ids = list(security_ids)
    if not ids:
        return 0
    try:
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM recompute_queue WHERE exchange = %s AND target = %s AND security_id = ANY(%s)",
                (exchange, target, ids),
            )
            deleted = cur.rowcount
        conn.commit()
        return deleted
    except Exception as e:
        conn.rollback()
        raise RecomputeQueueError(f"Failed to clear recompute_queue for {exchange}/{target}: {e}")
