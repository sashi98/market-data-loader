# bhav_copy_schedule_listener.py
#
# Weekday-evening/pre-market scheduled trigger for the BhavCopy "process"
# flow -- the exact same thing the "Process NSE Bhav Copy" / "Process BSE
# Bhav Copy" buttons in the UI already do (BhavCopyProcessingController ->
# BhavCopyProcessingService -> download + parse + persist, then
# corporate-actions processing right after -- see that controller's own
# comments), just fired automatically instead of requiring someone to
# click both buttons by hand every trading day.
#
# Run manually for testing/development (single check-and-process cycle,
# ignores the active-window gate below):
#   python bhav_copy_schedule_listener.py --once
#
# Real, ongoing usage (no args):
#   python bhav_copy_schedule_listener.py
#
# ---------------------------------------------------------------------
# REDESIGNED 2026-08-20 -- see prior design in git history if needed.
#
# WHY THIS ISN'T A SIMPLE "WAIT UNTIL 7PM, FIRE ONCE" LOOP (the original
# design): this listener runs inside the UAT stack on the admin's own
# laptop -- it is only alive while that laptop is on and Docker is
# running. A pure wall-clock "sleep until the next 19:00 IST" approach
# silently does nothing useful if the laptop happens to be off at 7pm
# and only gets turned on again the next morning -- the missed day is
# just... missed, with no automatic way to notice or recover, and by the
# time the NEXT 7pm rolls around, TWO trading days would be outstanding.
#
# Instead, this listener:
#   1. Only actively checks anything inside an ACTIVE WINDOW -- 19:00
#      IST through 09:00 IST the next calendar day (after market close,
#      through before market open). Outside that window (market hours),
#      there is nothing new to find, so it just sleeps.
#   2. On every check inside that window, it asks the DATABASE (not the
#      wall clock) what the real next date to process is: it reads
#      bhav_copy_metadata directly (same table BhavCopyProcessingService
#      itself already treats as the source of truth for "already done")
#      to find the most recently, successfully-integrated trade_date for
#      each exchange, INDEPENDENTLY -- bhav_copy_metadata has one row per
#      (trade_date, exchange), so NSE and BSE naturally track separately,
#      matching how every other part of this flow already treats them.
#   3. It compares that against "today" (if it's >= 19:00 IST) or
#      "yesterday" (if it's before 09:00 IST) using the SAME
#      weekend/holiday-aware trading-day walk the historical backfill
#      loader uses (core.trading_calendar.compute_trading_date_range_
#      between) -- so a real NSE/BSE holiday is correctly recognized as
#      "not a missing day" and never gets this stuck waiting forever.
#   4. If exactly ONE trading day is outstanding (the normal case --
#      routine daily use), it fetches that one date, same as the UI
#      button would. If MORE than one trading day is outstanding (the
#      laptop, or this listener, was off across more than one missed
#      trading day), it deliberately does NOTHING and logs a clear
#      message -- this listener only ever advances ONE trading day at a
#      time; it will never silently jump ahead and leave a gap behind.
#      Closing a multi-day gap is the historical loader's / admin's job
#      via the UI (process the missing dates manually, oldest first),
#      not something this listener will ever do on its own.
#   5. No separate weekend/holiday PRE-check exists before attempting a
#      fetch -- step 3's trading-day walk already excludes weekends and
#      known holidays from ever becoming "the one outstanding date" to
#      begin with, so there's normally nothing to gain from a redundant
#      check. If the holiday table itself is stale or an ad-hoc holiday
#      isn't in it yet, the fetch is simply attempted anyway and the
#      exchange's own HTTP 404 ("no data published yet") is still
#      caught and logged clearly as [SKIPPED] by _process_one_exchange()
#      below -- that's the real safety net, not a pre-check.
#   6. Every check that produces something worth recording (an actual
#      attempt, a WAITING gap, a WARN, or a NO_HISTORY case -- see
#      point 4/core.bhavcopy.run_audit's own header) is written to the
#      new bhav_copy_run_audit table, one row per exchange, as a
#      queryable "run report" -- see core/bhavcopy/run_audit.py.
#
# IST is a FIXED UTC+5:30 offset with no DST -- deliberately using
# datetime.timezone(timedelta(hours=5, minutes=30)) instead of
# zoneinfo/pytz, which need an IANA timezone database that may not be
# installed in this repo's slim Docker image (see Dockerfile -- no
# tzdata package added). A fixed offset is exact, correct for IST
# specifically (it never observes DST), and needs zero extra
# dependencies or system timezone data to get right.
#
# AUTHENTICATION: /api/data-integration/** requires ROLE_ADMIN + a JWT
# (see tmt's SecurityConfig.java). core/auth_client.py's existing
# authenticate() PROMPTS interactively for credentials on stdin --
# unusable here, since this listener runs unattended in a Docker
# container with nobody present to type a password. This listener
# instead reads TMT_ADMIN_USER_ID/TMT_ADMIN_PASSWORD directly from
# config/.env (an existing admin account's real credentials -- see this
# repo's own docs for why a dedicated service account was considered and
# explicitly NOT chosen) and calls auth_client.login() -- the
# non-interactive half of that same module -- itself. Logs in FRESH on
# every check cycle (not cached across the sleep between checks) --
# simpler than reasoning about the JWT's 8-hour expiry, and a login call
# every CHECK_INTERVAL_SECONDS is cheap.
#
# DIRECT DB ACCESS: unlike the original design (which only ever talked
# to tmt over its REST API, matching every request the UI itself makes),
# this listener ALSO opens direct Postgres connections (core/db_client.py
# -- the same module the historical backfill loader uses), for two
# distinct purposes:
#   - READING bhav_copy_metadata for the continuity check (point 2
#     above). All actual bhav-copy processing still goes through tmt's
#     REST API exactly as before, so BhavCopyProcessingService's own
#     idempotency guard, download/parse/persist logic, and
#     corporate-actions trigger remain the single real code path for
#     THAT write.
#   - WRITING bhav_copy_run_audit (point 6 above). This one IS written
#     directly by this listener, by explicit choice -- it's a log of
#     this listener's own decisions, not shared application state tmt's
#     Java side needs to own. See core/bhavcopy/run_audit.py's header.
#
# IDEMPOTENCY: enforced SERVER-SIDE for the actual bhav-copy write --
# BhavCopyProcessingService (tmt, Java) checks bhav_copy_metadata before
# downloading anything and returns status="SKIPPED" if this (date,
# exchange) already succeeded. This listener's own continuity check
# above is a different, additional thing -- it decides WHICH date (if
# any) is even worth attempting in the first place; the server-side
# guard remains the final word on whether to actually write.
#
# WHAT THIS LISTENER DOES NOT DO (by design, already handled elsewhere):
#   - Corporate-actions processing: BhavCopyProcessingController already
#     triggers CorporateActionsProcessingService right after every
#     bhav-copy call, synchronously, in the same request. Nothing extra
#     needed here.
#   - RSI (or any other indicator) incremental processing: a completely
#     separate, already-running listener (indicators_listener.py) polls
#     bhav_copy_metadata every 60 seconds for newly-complete trade_dates
#     and walks each ACTIVE indicator forward on its own. The moment
#     this listener's fetch succeeds and bhav_copy_metadata gets a new
#     SUCCESS row, that listener picks it up independently -- no
#     coordination needed between the two.

import argparse
import sys
import time
from datetime import datetime, timedelta, timezone
from datetime import time as dtime
from pathlib import Path

import requests

sys.path.insert(0, str(Path(__file__).resolve().parent))

from core.env_validator import load_and_validate_env, EnvValidationError
from core.logging_setup import start_run_logging
from core import auth_client
from core.db_client import get_connection, DbConnectionError
from core.bhavcopy.bhavcopy_persistence import get_latest_success_date
from core.bhavcopy import run_audit
from core.trading_calendar import compute_trading_date_range_between, TradingCalendarError

IST = timezone(timedelta(hours=5, minutes=30))

# Active window: 19:00 IST through 09:00 IST the next calendar day.
# Outside this window (09:00-19:00, market hours), there is nothing new
# to check -- see the module header comment above.
WINDOW_START_HOUR_IST = 19
WINDOW_END_HOUR_IST = 9

EXCHANGES = ["NSE", "BSE"]

# A full download+parse+persist(+corporate-actions right after, on the
# Java side) for one exchange can genuinely take a while for a full
# day's bhav copy -- generous on purpose, there's no cost to waiting.
REQUEST_TIMEOUT_SECONDS = 300

# How often to re-check DB continuity while inside the active window.
# Not a hard real-time requirement -- this listener only runs while the
# laptop happens to be on -- so 15 minutes balances "catches a
# late-published bhav copy or a just-turned-on laptop reasonably
# promptly" against "doesn't hammer the login/holiday-sync endpoints all
# night for no reason".
CHECK_INTERVAL_SECONDS = 900


def _is_within_window(now):
    """True if `now` (IST, timezone-aware) falls in [19:00, 09:00-next-day)."""
    t = now.time()
    return t >= dtime(WINDOW_START_HOUR_IST, 0) or t < dtime(WINDOW_END_HOUR_IST, 0)


def _seconds_until_window_open(now):
    """
    Seconds from `now` (IST) until the next 19:00 IST. Only meaningful
    when _is_within_window(now) is False (i.e. currently mid-day).
    """
    next_open = now.replace(hour=WINDOW_START_HOUR_IST, minute=0, second=0, microsecond=0)
    if next_open <= now:
        next_open += timedelta(days=1)
    return max((next_open - now).total_seconds(), 0)


def _ceiling_date(now):
    """
    The most recent calendar date whose bhav copy could plausibly
    already be published, given the current time. At/after 19:00 IST
    that's today; before 19:00 IST (including the whole pre-09:00
    morning window) it's still yesterday -- today's file simply can't
    exist yet before market close.
    """
    if now.time() >= dtime(WINDOW_START_HOUR_IST, 0):
        return now.date()
    return now.date() - timedelta(days=1)


def _process_one_exchange(tmt_app_base_url, token, exchange, date_str):
    """
    Calls POST /api/data-integration/bhav-copy/{exchange}/{date} -- the
    EXACT same endpoint the UI's "Process Bhav Copy" button calls, which
    on the Java side also triggers corporate-actions processing right
    after (see BhavCopyProcessingController) -- nothing extra needed
    here for that.

    Never raises -- every outcome (success, already-processed, no data
    published yet, or a genuine failure) is caught, printed as a clear
    [OK]/[SKIPPED]/[FAILED] line, AND returned as a dict so the caller
    can write it to bhav_copy_run_audit:
        {"outcome": "OK"|"SKIPPED"|"FAILED", "message": str,
         "processing_time_ms": int}
    """
    url = f"{tmt_app_base_url.rstrip('/')}/api/data-integration/bhav-copy/{exchange}/{date_str}"
    headers = {"Authorization": f"Bearer {token}"}
    start_ms = time.time() * 1000

    try:
        response = requests.post(url, headers=headers, timeout=REQUEST_TIMEOUT_SECONDS)
    except requests.exceptions.RequestException as e:
        message = f"could not reach {url}: {e}"
        print(f"  [FAILED] {exchange} {date_str} -- {message}")
        return {"outcome": "FAILED", "message": message,
                "processing_time_ms": int(time.time() * 1000 - start_ms)}

    processing_time_ms = int(time.time() * 1000 - start_ms)

    if response.status_code == 200:
        try:
            body = response.json()
        except ValueError:
            body = {}
        # BhavCopyProcessingService returns status="SKIPPED" when this
        # (date, exchange) already has a SUCCESS row in
        # bhav_copy_metadata -- e.g. a human already processed it
        # manually via the UI earlier the same day. See that service's
        # own comments for why this guard exists.
        if body.get("status") == "SKIPPED":
            message = body.get("reason", "already processed")
            print(f"  [SKIPPED] {exchange} {date_str} -- {message}")
            return {"outcome": "SKIPPED", "message": message, "processing_time_ms": processing_time_ms}
        message = str(body)
        print(f"  [OK] {exchange} {date_str} -- {message}")
        return {"outcome": "OK", "message": message, "processing_time_ms": processing_time_ms}

    try:
        error_body = response.json()
        message = error_body.get("message", "")
    except ValueError:
        message = response.text

    # BhavCopyDownloadServiceHandler raises IllegalArgumentException
    # (-> HTTP 400 via tmt's GlobalExceptionHandler) specifically for
    # "no file published yet -- holiday/weekend/not published" (an HTTP
    # 404 from NSE/BSE themselves). Distinguish that EXPECTED, routine
    # case from a genuine failure. Under the current design this should
    # be rare (see module header, point 5) -- it mainly catches a stale
    # or incomplete holiday calendar, not the normal weekend case.
    if response.status_code == 400 and "holiday" in message.lower():
        full_message = f"no data published yet (holiday/weekend/not yet published): {message}"
        print(f"  [SKIPPED] {exchange} {date_str} -- {full_message}")
        return {"outcome": "SKIPPED", "message": full_message, "processing_time_ms": processing_time_ms}

    full_message = f"HTTP {response.status_code}: {message}"
    print(f"  [FAILED] {exchange} {date_str} -- {full_message}")
    return {"outcome": "FAILED", "message": full_message, "processing_time_ms": processing_time_ms}


def _write_audit(env_values, exchange, ceiling_date, latest_success_date, target_date,
                  pending_trading_days, pending_dates, outcome, message, processing_time_ms=None):
    """
    Opens a short-lived DB connection just for this one bhav_copy_run_
    audit write, writes it, closes. A failure here is logged and
    swallowed -- never allowed to interrupt the actual check/processing
    flow it's reporting on. See core/bhavcopy/run_audit.py.
    """
    try:
        conn = get_connection(env_values)
    except DbConnectionError as e:
        print(f"  [WARN] Could not write run-audit row for {exchange} (DB connection failed): {e}")
        return
    try:
        run_audit.record_run(
            conn, exchange, ceiling_date, latest_success_date, target_date,
            pending_trading_days, pending_dates, outcome, message, processing_time_ms,
        )
    except run_audit.RunAuditError as e:
        print(f"  [WARN] Could not write run-audit row for {exchange}: {e}")
    finally:
        conn.close()


def _check_one_exchange(env_values, tmt_app_base_url, token, exchange, latest_success_date, ceiling_date):
    """
    Compares this exchange's last successfully-integrated trade_date
    (read fresh from bhav_copy_metadata) against `ceiling_date`, using
    the weekend/holiday-aware trading-day walk
    (compute_trading_date_range_between) so a real NSE/BSE holiday never
    counts as a missing day. See the module header comment for the full
    reasoning. Four outcomes, each (except the first) written to
    bhav_copy_run_audit via _write_audit():

      - Nothing outstanding (already caught up as of ceiling_date, or
        the gap between them contains no actual trading day) -- does
        nothing, silently, NO audit row (see run_audit.py's header for
        why).
      - No prior successfully-processed date at all for this exchange
        (NO_HISTORY) -- logs a WARN and returns; needs an initial
        historical load first.
      - Exactly ONE trading day outstanding -- the normal, routine
        case -- fetches that single date via _process_one_exchange()
        and records its outcome (OK/SKIPPED/FAILED).
      - MORE than one trading day outstanding (WAITING) -- a real
        backlog -- refuses to touch it and logs a clear message asking
        for manual catch-up via the UI. Never advances more than one
        trading day per check, and never guesses which of several
        missing dates to pick.
    """
    if latest_success_date is None:
        message = ("no successfully-processed date found in bhav_copy_metadata at all -- this exchange "
                    "needs an initial historical load (bhav_copy_with_corporate_action_loader.py) before "
                    "this scheduler has anything to track continuity from.")
        print(f"  [WARN] {exchange} -- {message}")
        _write_audit(env_values, exchange, ceiling_date, None, None, 0, None, "NO_HISTORY", message)
        return

    from_date = latest_success_date + timedelta(days=1)
    if from_date > ceiling_date:
        # Already caught up as of the most recent date that could exist right now.
        return

    try:
        result = compute_trading_date_range_between(from_date, ceiling_date, tmt_app_base_url, token)
    except TradingCalendarError as e:
        message = f"could not check trading-day continuity (holiday calendar lookup failed): {e}"
        print(f"  [WARN] {exchange} -- {message}. Skipping this check -- will retry next cycle.")
        _write_audit(env_values, exchange, ceiling_date, latest_success_date, None, 0, None, "WARN", message)
        return

    pending = result["trading_date_list"]

    if not pending:
        # from_date..ceiling_date contained no actual trading day (a
        # pure weekend/holiday span) -- nothing to do, no audit row.
        return

    if len(pending) > 1:
        pending_display = ", ".join(d.strftime("%d-%b-%Y") for d in pending)
        message = (f"{len(pending)} trading days behind ({pending_display}). This scheduler only ever "
                   f"advances one trading day at a time and will NOT skip ahead, leaving a gap. Please "
                   f"process the missing date(s) manually via the UI's 'Process Bhav Copy' button "
                   f"(oldest first) -- this scheduler will resume automatically once bhav_copy_metadata "
                   f"is caught up to exactly one day behind.")
        print(f"  [WAITING] {exchange} -- {message}")
        _write_audit(env_values, exchange, ceiling_date, latest_success_date, None,
                     len(pending), pending, "WAITING", message)
        return

    # Exactly one trading day outstanding -- the normal case.
    target_date = pending[0]
    date_str = target_date.strftime("%d-%b-%Y").upper()
    result_info = _process_one_exchange(tmt_app_base_url, token, exchange, date_str)
    _write_audit(env_values, exchange, ceiling_date, latest_success_date, target_date,
                 1, [target_date], result_info["outcome"], result_info["message"],
                 result_info["processing_time_ms"])


def check_and_process(env_values):
    """
    One full check cycle: logs in fresh, reads bhav_copy_metadata's
    latest successful date for each exchange, then checks + (maybe)
    processes each exchange independently. Called on every tick while
    inside the active window, and once immediately for --once.
    """
    tmt_app_base_url = env_values["TMT_APP_BASE_URL"]
    now = datetime.now(IST)
    ceiling_date = _ceiling_date(now)

    print(f"  Checking bhav-copy continuity as of {now.isoformat()} "
          f"(ceiling date: {ceiling_date.strftime('%d-%b-%Y')})")

    try:
        token = auth_client.login(
            tmt_app_base_url, env_values["TMT_ADMIN_USER_ID"], env_values["TMT_ADMIN_PASSWORD"]
        )
    except auth_client.AuthError as e:
        print(f"  [FAILED] Could not authenticate -- skipping this check: {e}")
        return

    try:
        conn = get_connection(env_values)
    except DbConnectionError as e:
        print(f"  [FAILED] Could not connect to DB to check bhav_copy_metadata -- skipping this check: {e}")
        return

    try:
        latest_by_exchange = {
            exchange: get_latest_success_date(conn, exchange) for exchange in EXCHANGES
        }
    finally:
        conn.close()

    for exchange in EXCHANGES:
        _check_one_exchange(env_values, tmt_app_base_url, token, exchange, latest_by_exchange[exchange], ceiling_date)


def run():
    """Standard entry point -- also callable via main.py, matching every other loader under loaders/."""
    parser = argparse.ArgumentParser(description="BhavCopy schedule listener")
    parser.add_argument("--once", action="store_true",
                         help="Run a single check-and-process cycle immediately (ignoring the active-window "
                              "gate) and exit. Useful for a first test run.")
    args = parser.parse_args()

    with start_run_logging("bhav_copy_schedule_listener"):
        try:
            env_values = load_and_validate_env()
        except EnvValidationError as e:
            print(f"[FAILED] {e}")
            sys.exit(1)

        if args.once:
            print("BhavCopy schedule listener -- running a single check-and-process cycle (--once), then exiting.")
            check_and_process(env_values)
            return

        print(f"BhavCopy schedule listener starting -- active window "
              f"{WINDOW_START_HOUR_IST:02d}:00-{WINDOW_END_HOUR_IST:02d}:00 IST, checking every "
              f"{CHECK_INTERVAL_SECONDS // 60} min while inside it. Ctrl+C to stop.")
        try:
            while True:
                now = datetime.now(IST)
                if _is_within_window(now):
                    print("=" * 60)
                    print(f"  BhavCopy schedule listener -- check at {now.isoformat()}")
                    print("=" * 60)
                    check_and_process(env_values)
                    print("  Check complete.")
                    time.sleep(CHECK_INTERVAL_SECONDS)
                else:
                    sleep_seconds = _seconds_until_window_open(now)
                    print(f"  Outside active window -- sleeping {round(sleep_seconds / 60, 1)} min "
                          f"until it reopens at {WINDOW_START_HOUR_IST:02d}:00 IST.")
                    time.sleep(sleep_seconds)
        except KeyboardInterrupt:
            print("\nShutting down.")


if __name__ == "__main__":
    run()
