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
from core.rsi.rsi_calculator import compute_rsi14_for_isin
from core.rsi.rsi_persistence import (
    fetch_bhav_copy_closes_for_isin,
    upsert_rsi14d_workbook,
    RsiPersistenceError,
)
from core.rsi.rsi_continuity import fetch_trading_calendar, build_calendar_index, RsiContinuityError


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
        newly_matched_keys (list, same shape),
        reprocess_results (list of
            {"isin", "exchange", "written", "error"})

    Raises CorporateActionsPipelineError on a BSE scrip_code resolution
    failure, a parse failure, or a persistence/reconciliation failure --
    the whole batch is structurally broken at that point, nothing safe
    to salvage.

    The targeted-RSI-reprocess step is deliberately SOFT per
    isin+exchange instead of raising: one isin's rebuild failing (e.g.
    a transient DB hiccup) is recorded in reprocess_results and the
    rest of the newly-matched isins still get processed, rather than
    aborting the whole run over one bad isin.
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

    reprocess_results = []
    if newly_matched_keys:
        newly_matched_isins = sorted({isin for isin, _action_type, _ex_date in newly_matched_keys})

        # Fetched once for the whole reprocess batch, not per isin --
        # same trading-session calendar rsi14d_loader.py and
        # rsi_incremental.py use for gap detection (see
        # core/rsi/rsi_continuity.py). A newly-matched corporate action
        # doesn't imply anything about this isin's own suspension
        # history, but the walk still needs to be gap-aware here for the
        # same reason it is everywhere else -- this IS the full-history
        # rebuild path for these isins, same as rsi14d_loader.py, just
        # scoped to a handful of isins instead of the whole market.
        try:
            calendar_index = build_calendar_index(fetch_trading_calendar(conn))
        except RsiContinuityError as e:
            calendar_index = None
            print(f"  [WARNING] Could not fetch trading calendar for gap-aware reprocess: {e} "
                  f"-- continuing without gap detection for this batch.")

        for isin in newly_matched_isins:
            for exchange in ("NSE", "BSE"):
                try:
                    df = fetch_bhav_copy_closes_for_isin(conn, isin, exchange)
                except RsiPersistenceError as e:
                    reprocess_results.append({"isin": isin, "exchange": exchange, "written": 0, "error": str(e)})
                    continue

                if df.empty:
                    continue  # isin doesn't trade on this exchange -- normal, not an error

                # ONE walk per isin+exchange, not per series/symbol --
                # df is already deduped to one eligible, tiebroken row
                # per (isin, exchange, trade_date) by
                # fetch_bhav_copy_closes_for_isin(), so grouping by
                # series/symbol here would re-fragment the exact
                # continuity this fix exists to preserve.
                rsi_df = compute_rsi14_for_isin(df.sort_values("trade_date"), calendar_index=calendar_index)

                try:
                    written = upsert_rsi14d_workbook(conn, rsi_df)
                except RsiPersistenceError as e:
                    reprocess_results.append({"isin": isin, "exchange": exchange, "written": 0, "error": str(e)})
                    continue

                reprocess_results.append({"isin": isin, "exchange": exchange, "written": written, "error": None})

    return {
        "nse_parsed_count": len(nse_parsed),
        "bse_parsed_count": len(bse_parsed),
        "unparsed_ratio_count": unparsed_ratio_count,
        "bse_unresolved_isin_count": bse_unresolved_isin_count,
        "touched_keys": touched_keys,
        "newly_matched_keys": newly_matched_keys,
        "reprocess_results": reprocess_results,
    }
