# runners/price_actions/bhav_copy_d_unadjusted_price_runner.py
#
# STEP 2 -- Daily Bhav Copy Runner.
#
# BLIND-UPSERT REDESIGN 2026-09-06 -- this module used to decide its
# own [start_date, end_date] window itself, from bhav_copy_metadata's
# own combined-both-exchange MAX(trade_date)
# (get_latest_success_date_overall() -- see this module's git history
# for _determine_date_range(), now removed). That is itself where a
# real bug lived: combining both exchanges into one MAX() meant a
# lagging exchange's own gap could be silently masked by the other
# exchange being current. Per Sashikant's own direction, that decision
# -- and per-exchange freshness itself -- now lives ENTIRELY in
# bhavcopy_scheduler_main.py; this module is now a pure "download this
# ONE exchange's [start_date, end_date] window, parse, persist" blind
# upsert executor, called once per exchange per cycle by the scheduler.
# It runs whenever the scheduler calls it, with no freshness check or
# date-range decision of its own left inside it at all.
#
# token/tmt_app_base_url are STILL needed here -- NOT for bhav copy
# itself (that's a direct NSE zip / BSE CSV download, no TMT API call
# involved), but for compute_trading_date_range_between()'s holiday-
# calendar check, which hits a DIFFERENT tmt endpoint
# (/api/holidays/sync/{year}).
#
# Walked date-major within the ONE exchange passed in: for each
# holiday-aware trading date in [start_date, end_date] (oldest first),
# downloads -> parses -> persists, upserting bhav_copy_metadata as it
# goes (persist_nse()/persist_bse() already do that upsert themselves).
#
# Can also be run standalone for a single exchange/cycle, for
# testing/development:
#   python runners/price_actions/bhav_copy_d_unadjusted_price_runner.py

import sys
import time
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from core.db_client import get_connection, DbConnectionError
from core.bhavcopy.bhavcopy_downloader import (
    download_bhavcopy, is_nse_legacy_format, BhavCopyNotFoundError, BhavCopyDownloadError,
)
from core.bhavcopy.bhavcopy_parser import parse_bhavcopy_csv, BhavCopyParseError
from core.bhavcopy.bhavcopy_persistence import (
    persist_nse, persist_bse, is_already_persisted, BhavCopyPersistenceError,
)
from core.bhavcopy import run_audit
from core.trading_calendar import compute_trading_date_range_between, TradingCalendarError
from core.date_format import fmt_date

# Sleep between actual network requests to nseindia.com/bseindia.com --
# only when a real request was made (a same-file skip needs no pause).
RATE_LIMIT_SECONDS = 1.5


def _process_one_exchange(env_values, exchange, trade_date, download_dir):
    """
    Download (skip if already on disk) -> parse -> persist (skip if
    already persisted in bhav_copy_metadata) for ONE exchange/date.
    Never raises -- returns {"outcome": "OK"|"SKIPPED"|"FAILED",
    "message": str, "processing_time_ms": int}.

    persist_nse()/persist_bse() already upsert bhav_copy_metadata
    themselves on success (see core/bhavcopy/bhavcopy_persistence.py) --
    nothing further needed here for that.
    """
    start_ms = time.time() * 1000

    try:
        downloaded_path, was_skipped, _is_legacy = download_bhavcopy(exchange, trade_date, download_dir)
    except BhavCopyNotFoundError as e:
        message = f"not available: {e}"
        print(f"    [SKIPPED] {exchange} {fmt_date(trade_date)} -- {message}")
        return {"outcome": "SKIPPED", "message": message, "processing_time_ms": int(time.time() * 1000 - start_ms)}
    except BhavCopyDownloadError as e:
        message = f"download error: {e}"
        print(f"    [FAILED] {exchange} {fmt_date(trade_date)} -- {message}")
        return {"outcome": "FAILED", "message": message, "processing_time_ms": int(time.time() * 1000 - start_ms)}

    # Only rate-limit when an actual network request was made -- no need
    # to pause after a same-file skip, since nothing was sent.
    if not was_skipped:
        time.sleep(RATE_LIMIT_SECONDS)

    # is_legacy (from download_bhavcopy() above) is only accurate for a
    # FRESH download -- it's always False when was_skipped, since the
    # skip check happens before the format lookup (see that function's
    # own docstring). is_nse_legacy_format() is a pure function of the
    # date, reliable either way -- used here just for an informative log
    # line, not for any actual parsing/persistence branching.
    if exchange == "NSE" and is_nse_legacy_format(trade_date):
        print(f"    [INFO] {exchange} {fmt_date(trade_date)} is a legacy pre-UDiFF-format file.")

    try:
        conn = get_connection(env_values)
    except DbConnectionError as e:
        message = f"DB connection failed: {e}"
        print(f"    [FAILED] {exchange} {fmt_date(trade_date)} -- {message}")
        return {"outcome": "FAILED", "message": message, "processing_time_ms": int(time.time() * 1000 - start_ms)}

    try:
        if is_already_persisted(conn, trade_date, exchange):
            message = "already persisted (re-run safe)"
            print(f"    [SKIPPED] {exchange} {fmt_date(trade_date)} -- {message}")
            return {"outcome": "SKIPPED", "message": message,
                    "processing_time_ms": int(time.time() * 1000 - start_ms)}

        try:
            rows = parse_bhavcopy_csv(downloaded_path, trade_date)
        except BhavCopyParseError as e:
            message = f"parse error: {e}"
            print(f"    [FAILED] {exchange} {fmt_date(trade_date)} -- {message}")
            return {"outcome": "FAILED", "message": message,
                    "processing_time_ms": int(time.time() * 1000 - start_ms)}

        persist_fn = persist_nse if exchange == "NSE" else persist_bse
        try:
            persist_fn(conn, rows, trade_date, downloaded_path, start_ms)
        except BhavCopyPersistenceError as e:
            message = f"persistence error: {e}"
            print(f"    [FAILED] {exchange} {fmt_date(trade_date)} -- {message}")
            return {"outcome": "FAILED", "message": message,
                    "processing_time_ms": int(time.time() * 1000 - start_ms)}

        message = f"{len(rows)} rows persisted"
        print(f"    [OK] {exchange} {fmt_date(trade_date)} -- {message}")
        return {"outcome": "OK", "message": message, "processing_time_ms": int(time.time() * 1000 - start_ms)}
    finally:
        conn.close()


def _write_daily_audit(env_values, exchange, ceiling_date, start_date, target_date,
                        pending_trading_days, pending_dates, outcome, message, processing_time_ms=None):
    try:
        conn = get_connection(env_values)
    except DbConnectionError as e:
        print(f"    [WARN] Could not write run-audit row for {exchange} (DB connection failed): {e}")
        return
    try:
        run_audit.record_run(
            conn, exchange, ceiling_date, start_date, target_date,
            pending_trading_days, pending_dates, outcome, message, processing_time_ms,
        )
    except run_audit.RunAuditError as e:
        print(f"    [WARN] Could not write run-audit row for {exchange}: {e}")
    finally:
        conn.close()


def run(env_values, tmt_app_base_url, token, download_dir, exchange, start_date, end_date, ceiling_date):
    """
    STEP 2 -- Daily Bhav Copy Runner, for ONE exchange. Blind upsert:
    the scheduler has already decided this exchange has a gap and
    computed [start_date, end_date] -- this function just walks that
    range date-major (holiday-aware, via
    compute_trading_date_range_between()) and downloads/parses/persists
    every trading date in it, with no freshness check of its own.

    ceiling_date is carried through only for the run_audit row's own
    "as-of" column -- it plays no part in deciding what to process
    (that's entirely [start_date, end_date], supplied by the caller).

    Returns {"start_date", "end_date", "any_new_data", "outcome",
    "daily"}.
    """
    print(f"STEP 2 -- Daily Bhav Copy Runner [{exchange}] (range: {fmt_date(start_date)} to {fmt_date(end_date)})")

    if start_date > end_date:
        return {"start_date": start_date, "end_date": end_date, "any_new_data": False,
                "outcome": "CAUGHT_UP", "daily": {"dates_ok": [], "outcome": "CAUGHT_UP"}}

    try:
        result = compute_trading_date_range_between(start_date, end_date, tmt_app_base_url, token)
    except TradingCalendarError as e:
        message = f"could not check trading-day continuity (holiday calendar lookup failed): {e}"
        print(f"  [WARN] {message}. Skipping this cycle -- will retry next cycle.")
        _write_daily_audit(env_values, exchange, ceiling_date, start_date, None, 0, None, "WARN", message)
        return {"start_date": start_date, "end_date": end_date, "any_new_data": False,
                "outcome": "WARN", "daily": {"dates_ok": [], "outcome": "WARN"}}

    pending = result["trading_date_list"]
    if not pending:
        return {"start_date": start_date, "end_date": end_date, "any_new_data": False,
                "outcome": "CAUGHT_UP", "daily": {"dates_ok": [], "outcome": "CAUGHT_UP"}}

    print(f"  [{exchange}] {len(pending)} trading day(s) to process "
          f"({pending[0].strftime('%d-%b-%Y')} to {pending[-1].strftime('%d-%b-%Y')}).")

    dates_ok = []
    daily_outcome = "DONE"
    for target_date in pending:
        result_info = _process_one_exchange(env_values, exchange, target_date, download_dir)
        _write_daily_audit(env_values, exchange, ceiling_date, start_date, target_date,
                            len(pending), pending, result_info["outcome"], result_info["message"],
                            result_info["processing_time_ms"])

        if result_info["outcome"] == "FAILED":
            daily_outcome = "FAILED"
            break

        # OK or SKIPPED (e.g. a date a human already processed manually, or a
        # single-exchange holiday) -- either way this date is accounted for,
        # keep walking forward.
        dates_ok.append(target_date)

    daily_result = {"dates_ok": dates_ok, "outcome": daily_outcome}
    return {
        "start_date": start_date,
        "end_date": end_date,
        "any_new_data": bool(dates_ok),
        "outcome": daily_outcome,
        "daily": daily_result,
    }


def _standalone_run():
    """Standalone single-exchange/cycle run, for testing/development."""
    from core import auth_client
    from core.env_validator import load_and_validate_env, EnvValidationError
    from core.logging_setup import start_run_logging

    with start_run_logging("bhav_copy_d_unadjusted_price_runner"):
        try:
            env_values = load_and_validate_env()
        except EnvValidationError as e:
            print(f"[FAILED] {e}")
            sys.exit(1)
        try:
            token = auth_client.login(
                env_values["TMT_APP_BASE_URL"], env_values["TMT_ADMIN_USER_ID"], env_values["TMT_ADMIN_PASSWORD"]
            )
        except auth_client.AuthError as e:
            print(f"[FAILED] Could not authenticate: {e}")
            sys.exit(1)
        today = date.today()
        summary = run(env_values, env_values["TMT_APP_BASE_URL"], token,
                       env_values["BHAV_COPY_CSV_DOWNLOAD_DIR"], "NSE", today, today, today)
        print(f"\nSummary: {summary}")


if __name__ == "__main__":
    _standalone_run()
