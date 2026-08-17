# core/corporate_actions/csv_pipeline.py
#
# CSV-export-based counterpart to pipeline.py's run_pipeline() -- adds
# the exchange-specific glue (download via the CSV endpoints, reshape
# each exchange's raw CSV columns into the shape corporate_actions_parser.py
# already expects, resolve isin) that run_pipeline() itself doesn't do,
# then delegates the actual parse/persist/reconcile/RSI-reprocess work
# to run_pipeline() UNCHANGED -- zero duplicated classification/
# reconciliation logic.
#
# Both NSE and BSE are standardized on their CSV export endpoints here
# (NSE_CORPORATE_ACTIONS_CSV_URL / BSE_CORPORATE_ACTIONS_CSV_URL in
# corporate_actions_downloader.py) rather than NSE's older JSON
# endpoint -- both were confirmed (2026-08-13, narrow-vs-wide-window
# probes) to genuinely respect from_date/to_date, unlike BSE's
# DefaultData/w JSON endpoint (a "forthcoming actions" widget, not a
# historical archive -- see corporate_actions_downloader.py's
# module docstring).
#
# This is the single entry point BOTH loaders/corporate_actions_loader.py
# (manual, interactive) AND stock-py-services' corporate_actions_service.py
# (triggered automatically after every bhav-copy load, via TMT's
# BhavCopyProcessingController -> CorporateActionsProcessingService)
# call -- one code path, not two, kept in this shared module
# specifically so a future fix (another parser regex bug, a new
# reconciliation edge case) only has to happen once.
#
# NOT YET VERIFIED against a live response for the NSE CSV reshape
# specifically (no network access in the environment this was written
# in -- see corporate_actions_downloader.py's history for why). The BSE
# reshape below is low-risk (already proven against a real downloaded
# CSV this session, see tests/import_bse_corporate_actions_csv_WRITE.py).
# The NSE reshape is built from a real probe response's header, but
# run it once for a narrow, cheap date range and check
# corporate_actions_raw before trusting it for anything large.

from core.corporate_actions.corporate_actions_downloader import (
    download_nse_corporate_actions_csv,
    download_bse_corporate_actions_csv,
    NSE_CORPORATE_ACTIONS_CSV_URL,
    BSE_CORPORATE_ACTIONS_CSV_URL,
)
from core.corporate_actions.corporate_actions_persistence import (
    resolve_nse_symbols,
    CorporateActionsPersistenceError,
)
from core.corporate_actions.pipeline import run_pipeline, CorporateActionsPipelineError

SUPPORTED_EXCHANGES = ("NSE", "BSE")


class CorporateActionsCsvPipelineError(Exception):
    """Raised only for an unsupported exchange value here. Download
    failures (CorporateActionsDownloadError) and persistence/
    reconciliation failures (CorporateActionsPipelineError,
    CorporateActionsPersistenceError) propagate as-is, unwrapped -- see
    run_csv_pipeline()'s docstring."""
    pass


def _get_ci(row, *candidates):
    """
    Case/whitespace-tolerant column lookup. Both exchanges' CSV headers
    are expected to be the exact casing documented in
    corporate_actions_downloader.py's docstrings (confirmed via real
    probe responses), but this tolerates minor header casing/whitespace
    drift rather than silently resolving a row to an empty string --
    an empty symbol/subject/ex_date is a HARD failure downstream
    (corporate_actions_parser.py's CorporateActionsParseError aborts
    the whole batch, not just that row), so it's worth being tolerant
    here rather than brittle.
    """
    for key in candidates:
        if key in row and row[key] not in (None, ""):
            return row[key]
    lowered = {str(k).strip().lower(): v for k, v in row.items()}
    for key in candidates:
        value = lowered.get(str(key).strip().lower())
        if value not in (None, ""):
            return value
    return ""


def _reshape_nse_row(row):
    """CSV columns (SYMBOL/PURPOSE/EX-DATE/...) -> the dict shape
    corporate_actions_parser.parse_nse_corporate_actions()'s field_map
    already expects (isin/symbol/subject/exDate). "isin" is injected
    separately by resolve_nse_symbols() before this runs -- preserved
    here via **row."""
    return {
        **row,
        "symbol": _get_ci(row, "SYMBOL"),
        "subject": _get_ci(row, "PURPOSE"),
        "exDate": _get_ci(row, "EX-DATE"),
    }


def _reshape_bse_row(row):
    """CSV columns (Security Code/Security Name/Purpose/Ex Date) -> the
    dict shape resolve_bse_scrip_codes()/parse_bse_corporate_actions()'s
    field_map already expect (scrip_code/short_name/Purpose/Ex_date).
    Same mapping already proven against a real downloaded CSV via
    a real downloaded CSV during this session (see
    corporate_actions_downloader.py's module docstring for the endpoint
    history)."""
    return {
        "scrip_code": _get_ci(row, "Security Code"),
        "short_name": _get_ci(row, "Security Name"),
        "Purpose": _get_ci(row, "Purpose"),
        "Ex_date": _get_ci(row, "Ex Date"),
    }


def download_corporate_actions_csv(exchange, from_date, to_date):
    """
    Download-only half of run_csv_pipeline() below -- no DB, no isin
    resolution, no reshaping. Exists so callers that need to genuinely
    separate "download for both exchanges" from "process for both
    exchanges" (bhav_copy_with_corporate_action_loader.py's Step 2/3, mirroring the real
    Step 2/Step 3 split bhav copy itself already does) can do so,
    instead of only having the all-in-one run_csv_pipeline() call.

    Returns the exchange's raw CSV rows (list of dicts), completely
    unprocessed. Raises CorporateActionsCsvPipelineError for an
    unsupported exchange value; download failures
    (CorporateActionsDownloadError) propagate as-is.
    """
    if exchange not in SUPPORTED_EXCHANGES:
        raise CorporateActionsCsvPipelineError(
            f"Unsupported exchange '{exchange}' -- expected one of {SUPPORTED_EXCHANGES}."
        )

    if exchange == "NSE":
        return download_nse_corporate_actions_csv(from_date, to_date)
    return download_bse_corporate_actions_csv(from_date, to_date)


def process_corporate_actions_rows(conn, exchange, raw_rows, from_date, to_date):
    """
    Process-only half of run_csv_pipeline() below -- takes raw_rows
    already fetched via download_corporate_actions_csv() instead of
    downloading them itself (isin resolution, reshaping, then delegates
    to run_pipeline() exactly as run_csv_pipeline() always has).

    conn: an open psycopg2 connection, autocommit=False -- same
    contract as run_pipeline() (commits on success, rolls back and
    raises on a hard persistence failure).

    from_date/to_date: python date objects -- not used for filtering
    here (raw_rows is already whatever download_corporate_actions_csv()
    returned), only echoed back into the summary for logging/metadata
    convenience.

    Returns run_pipeline()'s summary dict (see that function's
    docstring: nse_parsed_count, bse_parsed_count, unparsed_ratio_count,
    touched_keys, newly_matched_keys, reprocess_results), plus:
      - "unresolved_isin_count" -- rows dropped because no
        stock_universe match was found (symbol for NSE, scrip_code for
        BSE) -- normalized under one key regardless of exchange.
      - "total_rows_downloaded" -- len(raw_rows), for metadata/logging.
      - "exchange", "from_date", "to_date" -- echoed back.

    Raises CorporateActionsCsvPipelineError for an unsupported exchange
    value. Persistence/reconciliation failures
    (CorporateActionsPipelineError, CorporateActionsPersistenceError)
    propagate as-is.
    """
    if exchange not in SUPPORTED_EXCHANGES:
        raise CorporateActionsCsvPipelineError(
            f"Unsupported exchange '{exchange}' -- expected one of {SUPPORTED_EXCHANGES}."
        )

    if exchange == "NSE":
        resolved_rows, unresolved_count = resolve_nse_symbols(conn, raw_rows)
        reshaped_rows = [_reshape_nse_row(r) for r in resolved_rows]
        summary = run_pipeline(
            conn,
            nse_raw_rows=reshaped_rows,
            bse_raw_rows=[],
            nse_source_url=NSE_CORPORATE_ACTIONS_CSV_URL,
            bse_source_url="N/A -- this call is NSE-only",
        )
        summary["unresolved_isin_count"] = unresolved_count
    else:  # BSE
        reshaped_rows = [_reshape_bse_row(r) for r in raw_rows]
        summary = run_pipeline(
            conn,
            nse_raw_rows=[],
            bse_raw_rows=reshaped_rows,
            nse_source_url="N/A -- this call is BSE-only",
            bse_source_url=BSE_CORPORATE_ACTIONS_CSV_URL,
        )
        summary["unresolved_isin_count"] = summary.pop("bse_unresolved_isin_count", 0)

    summary["exchange"] = exchange
    summary["from_date"] = from_date
    summary["to_date"] = to_date
    summary["total_rows_downloaded"] = len(raw_rows)
    return summary


def run_csv_pipeline(conn, exchange, from_date, to_date):
    """
    All-in-one download+process, for callers that don't need the two
    phases kept separate (corporate_actions_loader.py's standalone
    manual run, stock-py-services' auto-triggered single-date call).
    Simply composes download_corporate_actions_csv() +
    process_corporate_actions_rows() -- same conn/return-value/
    exception contract as process_corporate_actions_rows() above (this
    function used to contain that logic directly; now it just calls
    through, so existing callers are unaffected).
    """
    raw_rows = download_corporate_actions_csv(exchange, from_date, to_date)
    return process_corporate_actions_rows(conn, exchange, raw_rows, from_date, to_date)
