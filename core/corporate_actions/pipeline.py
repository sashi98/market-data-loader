# core/corporate_actions/pipeline.py
#
# Shared "parse -> persist -> reconcile -> targeted RSI reprocess"
# pipeline -- the actual work corporate_actions_loader.py's step_3 used
# to do inline, factored out here so BOTH the interactive, manually-
# triggered loader AND corporate_actions_listener.py's unattended poll
# loop (see that module) call the exact same logic instead of two
# copies drifting apart over time.
#
# Nothing here prompts for input, prints a step banner, or opens/closes
# the DB connection -- callers own their own UX and connection
# lifecycle; this module takes an already-open connection and returns a
# plain dict summary.

from core.corporate_actions.corporate_actions_parser import (
    parse_nse_corporate_actions,
    parse_bse_corporate_actions,
    CorporateActionsParseError,
)
from core.corporate_actions.corporate_actions_persistence import (
    persist_raw,
    reconcile,
    resolve_bse_scrip_codes,
    CorporateActionsPersistenceError,
)
# RETIRED 2026-08-24 (Sashikant's call) -- this module used to end with
# a targeted, isin-scoped RSI reprocess triggered the instant a
# corporate action got newly MATCHED (see the git history for the old
# imports/logic: compute_rsi14_for_isin, fetch_bhav_copy_closes_for_isin,
# upsert_rsi14d_workbook, fetch_trading_calendar/build_calendar_index).
# It read straight from raw bhav_copy, bypassing bhav_copy_adjusted
# entirely, and was keyed by isin, not security_id -- so it would have
# completely missed an isin-lineage-bridge case like MWL's. Retired in
# favor of the new sequential pipeline (corporate action loader ->
# bhav_copy adjustment loader -> W/M rollups -> RSI14D/W/M listeners,
# see claude/bhav-copy-adjusted-clean-price-series-design-2026-08-23.md
# in the project docs) -- that chain is now the ONLY path that
# recomputes RSI after a corporate action. run_pipeline() below still
# parses/persists/reconciles corporate actions exactly as before; it
# just no longer reprocesses RSI itself afterward.


class CorporateActionsPipelineError(Exception):
    """Raised when parsing or persistence/reconciliation fails hard
    enough that the whole run should stop -- see run_pipeline()'s
    docstring for which failures are soft (per-isin, logged and
    skipped) vs hard (this one)."""
    pass


def run_pipeline(conn, nse_raw_rows, bse_raw_rows, nse_source_url, bse_source_url):
    """
    conn: an open psycopg2 connection, autocommit=False. This function
    commits on success (after persist_raw + reconcile), rolls back and
    raises on a hard failure. The targeted-RSI-reprocess step runs
    AFTER that commit, on its own -- an isin's RSI rebuild failing does
    NOT roll back the corporate-actions data that was already
    successfully persisted/reconciled.

    nse_raw_rows/bse_raw_rows: raw JSON rows straight from
    corporate_actions_downloader.py's download_*() functions -- an
    empty list is fine (e.g. one exchange's download failed upstream
    and the caller chose to continue with just the other).

    Returns a summary dict:
        nse_parsed_count, bse_parsed_count, unparsed_ratio_count,
        bse_unresolved_isin_count (BSE rows dropped because their
            scrip_code had no stock_universe match -- see
            resolve_bse_scrip_codes()'s docstring for why this is
            expected, not an error),
        touched_keys (set of (isin, action_type, ex_date)),
        newly_matched_keys (list, same shape)

    Raises CorporateActionsPipelineError on a BSE scrip_code resolution
    failure, a parse failure, or a persistence/reconciliation failure --
    the whole batch is structurally broken at that point, nothing safe
    to salvage.

    Does NOT reprocess RSI itself -- see the RETIRED note at the top of
    this module. newly_matched_keys tells the caller which isins got a
    newly-MATCHED action this run; downstream RSI recompute now happens
    via the sequential pipeline (bhav_copy adjustment loader -> W/M
    rollups -> RSI listeners), not from inside this function.
    """
    try:
        bse_resolved_rows, bse_unresolved_isin_count = resolve_bse_scrip_codes(conn, bse_raw_rows)
    except CorporateActionsPersistenceError as e:
        raise CorporateActionsPipelineError(f"BSE scrip_code resolution failed: {e}")

    try:
        nse_parsed = parse_nse_corporate_actions(nse_raw_rows)
        bse_parsed = parse_bse_corporate_actions(bse_resolved_rows)
    except CorporateActionsParseError as e:
        raise CorporateActionsPipelineError(f"Parsing failed: {e}")

    unparsed_ratio_count = sum(
        1 for row in (nse_parsed + bse_parsed)
        if row["face_value_old"] is None or row["face_value_new"] is None
    )

    try:
        touched_keys = set()
        touched_keys |= persist_raw(conn, nse_parsed, source_url=nse_source_url)
        touched_keys |= persist_raw(conn, bse_parsed, source_url=bse_source_url)
        newly_matched_keys = reconcile(conn, touched_keys)
        conn.commit()
    except CorporateActionsPersistenceError as e:
        conn.rollback()
        raise CorporateActionsPipelineError(f"Persistence/reconciliation failed: {e}")

    # RSI reprocess retired here -- see the module-level note above.
    # newly_matched_keys is still returned below so a caller (e.g. a
    # future step in the sequential pipeline) can see which isins got a
    # newly-MATCHED action this run, but this function no longer acts
    # on it itself.

    return {
        "nse_parsed_count": len(nse_parsed),
        "bse_parsed_count": len(bse_parsed),
        "unparsed_ratio_count": unparsed_ratio_count,
        "bse_unresolved_isin_count": bse_unresolved_isin_count,
        "touched_keys": touched_keys,
        "newly_matched_keys": newly_matched_keys,
    }
