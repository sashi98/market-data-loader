# runners/price_actions/corporate_actions_runner.py
#
# STEP 3 -- Corporate Action Runner.
#
# BLIND-UPSERT REDESIGN 2026-09-06 -- per Sashikant's own confirmed
# call (review point 2): corporate actions involve no computation of
# their own (unlike STEP 4/5/6's recompute-from-scratch), so this step
# no longer re-requests a full [BACKFILL_START_DATE, end_date] window
# on every non-skipped run, and no longer runs an internal freshness
# check against corporate_actions_metadata at all -- both of those
# decisions now live entirely in bhavcopy_scheduler_main.py, which only
# calls this module when it has already detected a per-exchange bhav
# copy gap, passing the SAME incremental [start_date, end_date] window
# used for STEP 2. An amendment to a corporate action landing outside
# that incremental window is an accepted trade-off (Sashikant's own
# call), not something this step tries to catch retroactively anymore.
#
# Single exchange per call now (was both NSE+BSE internally) -- the
# scheduler calls this once per exchange, same as STEP 2/4.
#
# Via core/corporate_actions/csv_pipeline.py's CSV-export path (NOT the
# older JSON downloaders -- see this module's git history for why: BSE's
# JSON endpoint ignores from_date/to_date entirely; both exchanges' CSV
# export endpoints were separately confirmed to respect the requested
# range).
#
# Always writes ONE corporate_actions_metadata row for this exchange
# before returning (download failure, pipeline failure, nothing to do,
# or a clean success).

from core.db_client import get_connection, DbConnectionError
from core.corporate_actions.corporate_actions_downloader import CorporateActionsDownloadError
from core.date_format import fmt_date
from core.corporate_actions.pipeline import CorporateActionsPipelineError
from core.corporate_actions.corporate_actions_persistence import (
    upsert_corporate_actions_metadata, CorporateActionsPersistenceError,
)
from core.corporate_actions.csv_pipeline import (
    download_corporate_actions_csv, process_corporate_actions_rows, CorporateActionsCsvPipelineError,
)


def _write_ca_metadata(env_values, exchange, start_date, end_date, run_status, error_message,
                       total_rows_downloaded, own_parsed_count, unresolved_isin_count, newly_matched_count):
    try:
        conn = get_connection(env_values)
    except DbConnectionError as e:
        print(f"  [WARN] Could not write corporate_actions_metadata for {exchange} (DB connection failed): {e}")
        return
    try:
        upsert_corporate_actions_metadata(
            conn, exchange, start_date, end_date, run_status,
            summary={
                "total_rows_downloaded": total_rows_downloaded,
                "nse_parsed_count": own_parsed_count if exchange == "NSE" else 0,
                "bse_parsed_count": own_parsed_count if exchange == "BSE" else 0,
                "unresolved_isin_count": unresolved_isin_count,
                "newly_matched_keys": [None] * newly_matched_count,
            },
            error_message=error_message,
        )
        conn.commit()
    except CorporateActionsPersistenceError as e:
        conn.rollback()
        print(f"  [WARN] Could not write corporate_actions_metadata for {exchange}: {e}")
    finally:
        conn.close()


def run(env_values, exchange, start_date, end_date):
    """
    STEP 3 -- Corporate Action Runner, for ONE exchange. Blind upsert
    over [start_date, end_date] -- no freshness check, no full-history
    window; the scheduler has already decided this should run. Returns
    {"outcome": "OK"|"SKIPPED"|"FAILED", "summary", "newly_matched_total"}.
    """
    print(f"STEP 3 -- Corporate Action Runner [{exchange}] (range: {fmt_date(start_date)} to {fmt_date(end_date)})")

    download_error = None
    try:
        raw_rows = download_corporate_actions_csv(exchange, start_date, end_date)
        print(f"  [OK] {exchange}: {len(raw_rows)} raw row(s).")
    except CorporateActionsDownloadError as e:
        print(f"  [FAILED] {exchange} download: {e}")
        raw_rows = []
        download_error = str(e)

    if not raw_rows:
        outcome = "FAILED" if download_error else "SKIPPED"
        print(f"  [{outcome}] {exchange}: nothing to process this cycle.")
        _write_ca_metadata(env_values, exchange, start_date, end_date,
                            "FAILED" if download_error else "SUCCESS", download_error, len(raw_rows), 0, 0, 0)
        return {"outcome": outcome}

    try:
        conn = get_connection(env_values)
    except DbConnectionError as e:
        message = str(e)
        print(f"  [FAILED] Could not open a connection: {message}")
        _write_ca_metadata(env_values, exchange, start_date, end_date, "FAILED", message, len(raw_rows), 0, 0, 0)
        return {"outcome": "FAILED", "message": message}

    summary = None
    pipeline_error = None
    try:
        summary = process_corporate_actions_rows(conn, exchange, raw_rows, start_date, end_date)
    except (CorporateActionsPipelineError, CorporateActionsPersistenceError, CorporateActionsCsvPipelineError) as e:
        print(f"  [FAILED] {exchange} pipeline error: {e}")
        pipeline_error = str(e)
    finally:
        conn.close()

    parsed_count_key = "nse_parsed_count" if exchange == "NSE" else "bse_parsed_count"
    status = "FAILED" if (download_error or pipeline_error) else "SUCCESS"

    _write_ca_metadata(
        env_values, exchange, start_date, end_date, status, download_error or pipeline_error,
        len(raw_rows), summary[parsed_count_key] if summary else 0,
        summary["unresolved_isin_count"] if summary else 0,
        len(summary["newly_matched_keys"]) if summary else 0,
    )

    newly_matched_total = len(summary["newly_matched_keys"]) if summary else 0
    print(f"  [{status if status == 'SUCCESS' else 'FAILED'}] {exchange}: "
          f"{summary[parsed_count_key] if summary else 0} row(s) persisted, "
          f"{newly_matched_total} newly MATCHED this cycle.")

    return {
        "outcome": "OK" if status == "SUCCESS" else "FAILED",
        "summary": summary,
        "newly_matched_total": newly_matched_total,
    }
