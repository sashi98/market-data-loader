# mf_scheduler_main.py -- TMT-MDL-US-0007
#
# Mutual fund scheduler ("MFS"), the BCS of mutual funds. Own long-running process (own container), like
# bhavcopy_scheduler_main.py. Every evening from 22:30 IST (AMFI publishes the day's NAVs late evening) it downloads
# AMFI's NAVAll.txt -- every scheme of every Indian fund house with its NAV -- and upserts mf_amc / mf_scheme / mf_nav
# (core/mutual_funds). Each run is logged in mf_scheduler_run for the Mutual Funds page's status card.
#
# Rules (Sashikant, 28-Nov-2026):
#   - own scheduler, never at the same time as BCS: before a run it waits while bhavcopy_scheduler_main
#     (maintenance_status.scheduler_running) or stock universe enrichment (enrichment_running) is running;
#   - admin data only -- no login to tmt, no maintenance mode (a NAV upsert takes seconds; nothing is blocked);
#   - no data loss / self-recovery (TMT-MDL-US-0006): the download retries and waits out internet loss; the file is
#     saved in one transaction; a run killed mid-way is marked ABANDONED at startup and simply redone.
#
# Window: 22:30 -> 02:00 IST. Inside it, a run is repeated every 30 min until the file carries the expected NAV date
# (today on a weekday, else the last weekday); after 02:00 whatever was loaded stands (holiday / late AMFI) and the
# next evening catches up. Same file twice = no change.
#
# Usage:
#   python mf_scheduler_main.py          # long-running scheduler
#   python mf_scheduler_main.py --once   # one NAV run now, then exit (first test run)
#   python mf_scheduler_main.py --portfolios 2026-10   # one-off: scheme portfolios of that month (TMT-MDL-US-0008)
#   python mf_scheduler_main.py --backfill-days 365   # one-off: NAV history of the last N days (7-day chunks,
#                                                     # resumable -- chunks already loaded are skipped), then exit
#
# Later steps of the same scheduler: monthly scheme portfolios (TMT-MDL-US-0008), quarterly shareholding pattern
# (TMT-MDL-US-0009).

import argparse
import hashlib
import logging
import sys
import time
from datetime import date, datetime, timedelta, timezone

from core.db_client import get_connection, DbConnectionError
from core.env_validator import load_and_validate_env, EnvValidationError
from core.logging_setup import start_run_logging
from core.retry import call_with_retry
from core.browser_identity import browser_get
from core.date_format import fmt_date, fmt_datetime
from core.stock_universe.persistence import is_scheduler_running, is_enrichment_running, StockUniversePersistenceError
from core.mutual_funds import amfi_nav, portfolio_loader, portfolio_sources
from core.mutual_funds import persistence as mfdb

logger = logging.getLogger("mf_scheduler_main")

IST = timezone(timedelta(hours=5, minutes=30))
STEP_NAV = "AMFI_NAV"
WINDOW_START = (22, 30)          # IST
WINDOW_END_HOUR = 2              # IST, next morning
RETRY_SECONDS = 30 * 60
BUSY_WAIT_SECONDS = 5 * 60
MIN_SCHEMES = 5000               # a real NAVAll.txt has ~15k scheme options; fewer = an error page, not data


def expected_nav_date(now):
    """NAV date a complete file should carry for the window that `now` belongs to: that evening if a weekday, else
    the last weekday before it. The window opened at 22:30 on day D (or on D-1 when now is before 02:00)."""
    d = now.date() if now.hour >= WINDOW_START[0] - 1 else now.date() - timedelta(days=1)
    while d.weekday() >= 5:
        d -= timedelta(days=1)
    return d


def in_window(now):
    start = now.replace(hour=WINDOW_START[0], minute=WINDOW_START[1], second=0, microsecond=0)
    return now >= start or now.hour < WINDOW_END_HOUR


def seconds_until_window(now):
    start = now.replace(hour=WINDOW_START[0], minute=WINDOW_START[1], second=0, microsecond=0)
    if now >= start:
        start += timedelta(days=1)
    return max(60, (start - now).total_seconds())


def _with_conn(env, fn, *args):
    conn = get_connection(env)
    try:
        return fn(conn, *args)
    finally:
        conn.close()


def wait_while_other_jobs_run(env):
    """Never at the same time as BCS or enrichment: wait (logging why) until both are idle."""
    while True:
        try:
            busy = _with_conn(env, lambda c: ("BCS" if is_scheduler_running(c) else None) or
                                                    ("stock universe enrichment" if is_enrichment_running(c) else None))
        except (DbConnectionError, StockUniversePersistenceError) as e:
            logger.warning(f"  Could not read maintenance_status ({e}) -- retrying in {BUSY_WAIT_SECONDS // 60} min.")
            time.sleep(BUSY_WAIT_SECONDS)
            continue
        if not busy:
            return
        logger.info(f"  {busy} is running -- the MF scheduler waits {BUSY_WAIT_SECONDS // 60} min (never runs alongside it).")
        time.sleep(BUSY_WAIT_SECONDS)


def download_nav_file():
    def get():
        r = browser_get(amfi_nav.AMFI_NAV_URL, timeout=(15, 120))
        if r.status_code != 200:
            raise RuntimeError(f"AMFI returned HTTP {r.status_code}")
        return r.content
    return call_with_retry(get, on_retry=lambda n, e: logger.warning(f"  [retry {n}] AMFI download: {e}"))


def run_nav_step(env):
    """One NAV run: download, parse, save, log. Returns the file's NAV date, or None when the run failed."""
    run_id = _with_conn(env, mfdb.start_run, STEP_NAV)
    try:
        content = download_nav_file()
        sha = hashlib.sha256(content).hexdigest()
        parsed = amfi_nav.parse(content.decode("utf-8", errors="replace"))
        if len(parsed.rows) < MIN_SCHEMES:
            raise RuntimeError(f"only {len(parsed.rows)} schemes in the file (expected thousands) -- not saved")
        for no, reason, text in parsed.skipped[:20]:
            logger.warning(f"  skipped line {no}: {reason}: {text}")
        if len(parsed.skipped) > 20:
            logger.warning(f"  ... and {len(parsed.skipped) - 20} more skipped line(s)")
        counts = _with_conn(env, mfdb.save_nav_file, parsed, date.today())
        msg = f"{len(parsed.skipped)} line(s) skipped" if parsed.skipped else None
        _with_conn(env, lambda c: mfdb.finish_run(c, run_id, "SUCCESS", msg, file_bytes=len(content), file_sha256=sha, **counts))
        logger.info(f"  AMFI NAV file of {fmt_date(counts['latest_nav_date'])}: {counts['amcs']} fund houses, "
                    f"{counts['schemes']:,} schemes ({counts['new_schemes']:,} new, {counts['inactive_schemes']:,} inactive), "
                    f"{counts['nav_rows']:,} NAV rows added.")
        return counts["latest_nav_date"]
    except Exception as e:  # noqa: BLE001 -- logged and recorded; the scheduler retries
        logger.error(f"  [FAILED] AMFI NAV run: {e}")
        try:
            _with_conn(env, lambda c: mfdb.finish_run(c, run_id, "FAILED", str(e)))
        except Exception as e2:  # noqa: BLE001
            logger.error(f"  could not record the failure: {e2}")
        return None


PORTFOLIO_FROM_DAY = 11          # SEBI: fund houses publish the month's portfolio by the 10th of the next month


def previous_month_end(today):
    return today.replace(day=1) - timedelta(days=1)


def run_portfolio_step(env, month_end, only_missing=True):
    """Scheme portfolios of month_end for every fund house in portfolio_sources (only those not loaded yet when
    only_missing). Logged as an MF_PORTFOLIO run with a coverage line."""
    missing = _with_conn(env, mfdb.missing_portfolio_tables)
    if missing:   # checked before any download: nothing is fetched until the backend has created the tables
        logger.error(f"  [SKIPPED] scheme portfolios: table(s) {', '.join(missing)} not found -- start the tmt backend "
                     f"once so Liquibase 035 creates them, then run again.")
        return
    sources = portfolio_sources.SOURCES
    if only_missing:
        loaded = _with_conn(env, mfdb.portfolio_amcs_loaded, month_end)
        sources = [s for s in sources if s["amc"] not in loaded]
        if not sources:
            return
    run_id = _with_conn(env, mfdb.start_run, mfdb.STEP_PORTFOLIO)
    logger.info(f"  Scheme portfolios of {month_end:%b-%Y}: {len(sources)} fund house(s) to load.")
    try:
        summary = portfolio_loader.load_month(lambda: get_connection(env), month_end, sources)
        loaded = _with_conn(env, mfdb.portfolio_amcs_loaded, month_end)
        failed = [a for a, v in summary.items() if v["error"] and a not in loaded]
        msg = (f"{month_end:%Y-%m}: {len(loaded)} of {len(portfolio_sources.SOURCES)} fund houses loaded"
               + (f"; pending / failed: {', '.join(failed)}" if failed else ""))
        holdings = sum(v["holdings"] for v in summary.values())
        _with_conn(env, lambda c: mfdb.finish_run(c, run_id, "SUCCESS" if not failed else "PARTIAL", msg,
                                                  latest_nav_date=month_end, nav_rows=holdings))
        logger.info(f"  {msg}")
    except Exception as e:  # noqa: BLE001
        logger.error(f"  [FAILED] scheme portfolios: {e}")
        _with_conn(env, lambda c: mfdb.finish_run(c, run_id, "FAILED", str(e), latest_nav_date=month_end))


HISTORY_CHUNK_DAYS = 7
HISTORY_PAUSE_SECONDS = 3


def backfill_history(env, days):
    """
    One-off NAV history back-fill (for 1M / 3M / 1Y returns and the NAV chart): AMFI's history download in 7-day
    chunks, oldest first, NAVs only (schemes come from the daily file). Each chunk is one transaction and logged as a
    run; a restarted back-fill skips chunks already loaded (power cut / shutdown safe).
    """
    end = date.today()
    start = end - timedelta(days=days)
    done = _with_conn(env, mfdb.done_history_chunks)
    chunks, d = [], start
    while d <= end:
        to = min(end, d + timedelta(days=HISTORY_CHUNK_DAYS - 1))
        chunks.append((d, to))
        d = to + timedelta(days=1)
    todo = [c for c in chunks if (c[0].isoformat(), c[1].isoformat()) not in done]
    logger.info(f"NAV history back-fill {fmt_date(start)} -> {fmt_date(end)}: {len(chunks)} chunks of {HISTORY_CHUNK_DAYS} days, "
                f"{len(chunks) - len(todo)} already loaded, {len(todo)} to load.")
    total = 0
    for i, (frm, to) in enumerate(todo, start=1):
        wait_while_other_jobs_run(env)
        run_id = _with_conn(env, mfdb.start_run, mfdb.STEP_HISTORY)
        url = amfi_nav.AMFI_NAV_HISTORY_URL.format(frm=frm.strftime("%d-%b-%Y"), to=to.strftime("%d-%b-%Y"))
        try:
            def get():
                r = browser_get(url, timeout=(15, 300))
                if r.status_code != 200:
                    raise RuntimeError(f"AMFI returned HTTP {r.status_code}")
                return r.content
            content = call_with_retry(get, on_retry=lambda n, e: logger.warning(f"  [retry {n}] {e}"))
            parsed = amfi_nav.parse(content.decode("utf-8", errors="replace"), history=True)
            added = _with_conn(env, mfdb.save_nav_history, parsed.rows)
            total += added
            _with_conn(env, lambda c: mfdb.finish_run(c, run_id, "SUCCESS", f"from={frm.isoformat()}",
                                                      latest_nav_date=to, nav_rows=added, file_bytes=len(content)))
            logger.info(f"  [{i}/{len(todo)}] {fmt_date(frm)} - {fmt_date(to)}: {len(parsed.rows):,} NAVs read, {added:,} added.")
        except Exception as e:  # noqa: BLE001 -- the chunk is retried by the next back-fill run
            logger.error(f"  [{i}/{len(todo)}] {fmt_date(frm)} - {fmt_date(to)} FAILED: {e} -- re-run the back-fill to retry it.")
            try:
                _with_conn(env, lambda c: mfdb.finish_run(c, run_id, "FAILED", f"from={frm.isoformat()} {e}", latest_nav_date=to))
            except Exception:  # noqa: BLE001
                pass
        time.sleep(HISTORY_PAUSE_SECONDS)
    logger.info(f"NAV history back-fill finished: {total:,} NAV rows added.")


def run():
    parser = argparse.ArgumentParser(description="Mutual fund scheduler (AMFI NAVs)")
    parser.add_argument("--once", action="store_true", help="One NAV run now, then exit.")
    parser.add_argument("--portfolios", default=None, metavar="YYYY-MM",
                        help="One-off: load every fund house's scheme portfolio of that month (reloads changed files), then exit.")
    parser.add_argument("--backfill-days", type=int, default=None,
                        help="One-off: load AMFI NAV history of the last N days (resumable), then exit.")
    args = parser.parse_args()

    with start_run_logging("mf_scheduler_main"):
        try:
            env = load_and_validate_env()
        except EnvValidationError as e:
            logger.error(f"[FAILED] {e}")
            sys.exit(1)
        try:
            n = _with_conn(env, mfdb.close_abandoned_runs)
            if n:
                logger.warning(f"[RECOVERED] {n} run(s) left RUNNING by a stopped process marked ABANDONED; redoing.")
        except DbConnectionError as e:
            logger.warning(f"Could not check for abandoned runs at startup: {e}")

        if args.portfolios:
            y, m = (int(x) for x in args.portfolios.split("-"))
            month_end = (date(y + (m == 12), m % 12 + 1, 1) - timedelta(days=1))
            wait_while_other_jobs_run(env)
            run_portfolio_step(env, month_end, only_missing=False)
            return

        if args.backfill_days:
            backfill_history(env, args.backfill_days)
            return

        if args.once:
            wait_while_other_jobs_run(env)
            run_nav_step(env)
            return

        logger.info(f"MF scheduler starting -- AMFI NAVs every evening {WINDOW_START[0]:02d}:{WINDOW_START[1]:02d}-"
                    f"{WINDOW_END_HOUR:02d}:00 IST, retried every {RETRY_SECONDS // 60} min until the day's NAVs are in. "
                    f"Ctrl+C to stop.")
        try:
            while True:
                now = datetime.now(IST)
                if not in_window(now):
                    secs = seconds_until_window(now)
                    logger.info(f"  Next MF run at {fmt_datetime(now + timedelta(seconds=secs))} -- sleeping {round(secs / 3600, 1)} h.")
                    time.sleep(secs)
                    continue
                expected = expected_nav_date(now)
                try:
                    stored = _with_conn(env, mfdb.stored_nav_date)
                except DbConnectionError as e:
                    logger.warning(f"  DB not reachable ({e}) -- retrying in {RETRY_SECONDS // 60} min.")
                    time.sleep(RETRY_SECONDS)
                    continue
                if stored is not None and stored >= expected:
                    secs = seconds_until_window(now)
                    logger.info(f"  NAVs of {fmt_date(expected)} already loaded -- next run in {round(secs / 3600, 1)} h.")
                    time.sleep(secs)
                    continue
                wait_while_other_jobs_run(env)
                logger.info("=" * 60)
                logger.info(f"  MF scheduler -- AMFI NAV run for {fmt_date(expected)} ({fmt_datetime(datetime.now(IST))})")
                logger.info("=" * 60)
                got = run_nav_step(env)
                today = datetime.now(IST).date()
                if today.day >= PORTFOLIO_FROM_DAY:      # last month's portfolios, until every fund house is in
                    run_portfolio_step(env, previous_month_end(today))
                if got is not None and got >= expected:
                    continue  # loaded; the next loop iteration sleeps until tomorrow's window
                logger.info(f"  NAVs of {fmt_date(expected)} not complete yet (file has {fmt_date(got) if got else 'nothing new'}) "
                            f"-- retrying in {RETRY_SECONDS // 60} min.")
                time.sleep(RETRY_SECONDS)
        except KeyboardInterrupt:
            logger.info("\nShutting down.")


if __name__ == "__main__":
    run()
