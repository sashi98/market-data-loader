# stock_universe_update_listener.py
#
# Stock Universe enrichment listener. Polls stock_universe_metadata for
# a moment when ALL FIVE exchange uploads (nse_data, nse_sme_data,
# bse_data, bse_sme_data, nse_fno_data) are simultaneously status
# 'success', then enriches every ISIN in stock_universe -- sector,
# industry, market cap, P/E, P/B, EPS, ROE, debt-to-equity, dividend
# yield, date of listing, and (best-effort) earnings date.
#
# Run manually for testing/development:
#   python stock_universe_update_listener.py --once --limit 10
#
# Real, ongoing usage (no args):
#   python stock_universe_update_listener.py
#
# --once: run a single poll cycle and exit, instead of looping forever.
# --limit N: cap how many ISINs get enriched in one batch. TESTING
#            ONLY -- never pass this for a real run, since it also
#            records a run in stock_universe_enrichment_run that only
#            covers the limited subset, not the full stock_universe.
#
# Architecturally follows indicators_listener.py's pattern (poll loop,
# threading, its own DB connection per thread) -- but deliberately
# DIFFERS from it in two ways that matter given the very different
# scale involved:
#
#   - indicators_listener.py spawns ONE THREAD PER INDICATOR, because
#     there are only ~5 indicators ever. This listener can be enriching
#     thousands of ISINs -- one thread per ISIN would hammer external
#     APIs with thousands of simultaneous connections and almost
#     certainly get rate-limited or blocked outright. Uses a small,
#     bounded worker pool instead (WORKER_THREAD_COUNT), each pulling
#     work off a shared queue with a delay between requests.
#
#   - indicators_listener.py stops an indicator entirely on its first
#     failure (each day's RSI computation depends on the previous
#     day's, so a gap breaks everything after it). Here, every ISIN is
#     fully independent -- one bad/delisted ticker skips and continues,
#     never aborts the batch.
#
# REDESIGNED: NSE and BSE are now resolved and written COMPLETELY
# INDEPENDENTLY, per explicit instruction -- there is NO merging, NO
# "common fields shared across the isin" concept anymore. For a
# dual-listed company (isin X, listed on both NSE and BSE):
#
#   X, NSE row <- populated ONLY from (NSE official API + yfinance NSE
#                 ticker), written to WHERE isin=X AND exchange='NSE'
#                 (or 'NSE SME') ONLY.
#
#   X, BSE row <- populated ONLY from (BSE official API + yfinance BSE
#                 ticker), written to WHERE isin=X AND exchange='BSE'
#                 (or 'BSE SME') ONLY.
#
# Neither side ever influences the other's row, and neither side is a
# "fallback" for the other -- both are always attempted independently
# whenever their own identifiers exist. TradingView is a last-resort
# gap-fill tier, but NSE-SIDE ONLY -- see the note below on why the BSE
# side no longer uses it at all.
#
# TRADINGVIEW REMOVED FROM THE BSE SIDE ENTIRELY, per two independently
# confirmed real cases of it returning BADLY WRONG data specifically for
# BSE symbol lookups: ATLPP (a confirmed debt instrument) got a market
# cap ~20x its real figure before the IShow exclusion caught that
# category; JISLDVREQS (a real DVR share class) got a market cap ~55x
# its real figure (cross-checked against an independent source),
# despite the NSE side's OWN yfinance-sourced market cap for the SAME
# company landing close to correct. Two confirmed bad matches on the
# SAME side is a pattern, not a fluke -- plausibly because BSE
# identifies securities primarily by NUMERIC security code, not text
# symbol, so TradingView's bare text-name query is more prone to
# ambiguous/wrong matches on the BSE side than on NSE's. NSE-side
# TradingView is unaffected by this -- independently verified correct
# for ACCPL against real screenshots earlier this session.
#
# Work is still queued ONE ITEM PER ISIN (not per row) purely because
# fetch_isin_exchange_map conveniently returns both sides' identifiers
# in a single query -- but from that point on, the NSE side and BSE
# side of the SAME queue item are resolved and written as two entirely
# separate operations, each producing its own [OK]/[NO DATA] outcome.
#
# Within EACH side, priority between that side's own two sources is
# "official API wins, yfinance only fills in what the official API
# didn't provide" -- matching the already-confirmed higher precision of
# NSE's own official data over yfinance's approximations for
# sector/industry/date_of_listing/P-E, and applying the same logic
# symmetrically to BSE's own official API.
#
# Worth knowing: this means up to 2 requests on the BSE side (official
# API, yfinance -- no TradingView, see above) and up to 3 on the NSE
# side (official API, yfinance, TradingView-if-needed), so up to 5
# total per dual-listed isin -- MORE total API calls than the earlier
# ISIN-merged design, in exchange for each row reflecting only its own
# exchange's real data, with zero risk of one exchange's value leaking
# onto the other's row.
#
# ONE narrow, deliberate exception to "no merging": market_capitalization,
# debt_to_equity_ratio, dividend_yield, and earnings_date are genuinely
# company-wide facts, not listing-specific ones (a company reports
# earnings once, has one balance sheet, declares one dividend) --
# unlike sector/industry/date_of_listing/index_list, which ARE correctly
# kept independent. BSE's own official API never provides these 4
# fields at all (confirmed across every real sample checked), and
# yfinance's BSE ticker is frequently empty too (confirmed live for
# RELIANCE's own BSE listing). So after the NSE side resolves, its
# values for JUST these 4 fields are used to fill in the BSE side's
# gaps for the SAME 4 fields -- one-directional (NSE fills BSE, never
# the reverse), and only ever filling a gap, never overriding a value
# the BSE side's own sources already produced.

import argparse
import select
import sys
import time
import threading
from datetime import datetime, timezone
from pathlib import Path
from queue import Queue, Empty

sys.path.insert(0, str(Path(__file__).resolve().parent))

from nse import NSE
from bse import BSE

from core.env_validator import load_and_validate_env, EnvValidationError
from core.db_client import get_connection, DbConnectionError
from core.logging_setup import start_run_logging
from core.stock_universe.persistence import (
    fetch_latest_metadata_status, is_batch_ready, get_max_metadata_id,
    fetch_last_processed_cursor, fetch_isin_exchange_map,
    update_stock_fundamentals, normalize_field_value, record_enrichment_run,
    record_enrichment_audit_rows, set_maintenance_running, StockUniversePersistenceError,
)
from core.stock_universe.yfinance_client import fetch_fundamentals, RATE_LIMIT_DELAY_SECONDS
from core.stock_universe.tradingview_client import fetch_fundamentals_tradingview, TRADINGVIEW_COVERABLE_FIELDS
from core.stock_universe.nse_client import fetch_fundamentals_nse, NseExcluded
from core.stock_universe.bse_client import fetch_fundamentals_bse, BseExcluded

# This is a much slower-moving signal than indicators_listener's bhav_copy
# completion check -- these five uploads happen at most a few times a
# day, not continuously. Used as a SAFETY-NET fallback timeout only now,
# not the primary trigger -- see LISTEN_CHANNEL below and run()'s own
# comments for why. If a NOTIFY is ever missed (e.g. this listener's
# LISTEN connection happened to be down/reconnecting at the exact
# moment Java notified -- NOTIFY delivery is NOT persisted, so a missed
# one is gone forever), this bounds how long it takes to notice anyway.
POLL_INTERVAL_SECONDS = 300

# Postgres pub/sub channel -- StockUniverseServiceHandler#saveOrUpdate()
# (Java side) issues NOTIFY on this exact channel name right after EVERY
# successful CSV upload's metadata row commits (all 5 of nse_data/
# nse_sme_data/bse_data/bse_sme_data/nse_fno_data, not just detecting
# "this is the last one" on that side). This listener LISTENs on it and
# wakes up near-instantly instead of waiting for its next
# POLL_INTERVAL_SECONDS timeout -- see run()'s own comments for the
# full mechanics. The channel name must match EXACTLY on both sides;
# nothing enforces that at compile time, so if this ever changes here,
# it must change in StockUniverseServiceHandler.java too.
LISTEN_CHANNEL = "stock_universe_ready"

# Deliberately small -- see this file's own header comment for why this
# is NOT one-thread-per-isin.
WORKER_THREAD_COUNT = 4

# Folder for the nse package's own cookie/session files -- one shared
# location is fine; the package manages its own file naming internally.
NSE_DOWNLOAD_FOLDER = Path(__file__).resolve().parent / "nse_downloads"

# Same idea, separate folder -- the bse package manages its own cookie
# files independently of the nse package's, even though both packages
# share the same author/design philosophy.
BSE_DOWNLOAD_FOLDER = Path(__file__).resolve().parent / "bse_downloads"

# The one narrow exception to "NSE and BSE never influence each other" --
# see this file's header comment for the full reasoning. Confirmed real
# via bse_client.py's own header comment (none of these 4 fields appear
# anywhere in BSE's official API response, across every real sample
# checked) and a live test (yfinance returned nothing at all for
# RELIANCE's own 500325.BO).
NSE_SHARED_FALLBACK_FIELDS = ("market_capitalization", "debt_to_equity_ratio", "dividend_yield", "earnings_date")


def _record_audit(results, results_lock, isin_number, exchange, outcome, reason):
    """
    Records a single non-success outcome (EXCLUDED, NO_DATA, FAILED, or
    FIELD_DROPPED) for one (isin, exchange) side, appended to
    results["audit"] under the same lock used for the enriched/failed/
    excluded counters. Persisted to stock_universe_enrichment_audit at
    the end of the batch (see record_enrichment_audit_rows in
    persistence.py) -- ONLY non-success rows are kept here, by explicit
    design decision -- a full run's thousands of successful sides
    already have their real home in stock_universe itself; this table
    exists purely to make "which ISINs failed or were excluded, and
    why" queryable without grepping logs.

    FIELD_DROPPED is the one outcome here that does NOT mean the side
    failed overall -- it means the side still enriched successfully
    (results["enriched"] still increments), but one specific field
    fetched from a source was unsafe to write (non-finite, or out of
    its column's DECIMAL range -- see persistence.py's
    _sanitize_numeric_fields) and was skipped rather than dropping
    every other good field in the same UPDATE alongside it.
    """
    with results_lock:
        results["audit"].append({
            "isin_number": isin_number,
            "exchange": exchange,
            "outcome": outcome,
            "reason": reason,
        })


def _resolve_nse_side(nse_session, isin_number, nse_symbol, nse_exchange, results, results_lock):
    """
    Resolves and returns the fields for ONE isin's NSE-family row, from
    (NSE official API + yfinance NSE ticker) ONLY -- completely
    independent of whatever happens on the BSE side. Official API wins
    for any field both sources provide.

    Returns (fields, excluded, source_notes) -- excluded=True means
    NSE's own isDelisted flag says this row should not be written at
    all; fields will be {} in that case. Sleeps
    RATE_LIMIT_DELAY_SECONDS after each request it makes.
    """
    fields = {}
    source_notes = []

    try:
        nse_fields = fetch_fundamentals_nse(nse_session, nse_symbol, nse_exchange)
        if nse_fields:
            fields.update(nse_fields)
            source_notes.append(f"NSE:{nse_symbol}")
    except NseExcluded as e:
        print(f"  [EXCLUDED] {isin_number} NSE-side (NSE={nse_symbol}) -- {e}")
        with results_lock:
            results["excluded"] += 1
        _record_audit(results, results_lock, isin_number, nse_exchange, "EXCLUDED", str(e))
        time.sleep(RATE_LIMIT_DELAY_SECONDS)
        return {}, True, []
    time.sleep(RATE_LIMIT_DELAY_SECONDS)

    yf_ticker = f"{nse_symbol}.NS"
    try:
        yf_fields = fetch_fundamentals(yf_ticker)
    except Exception as e:
        # CONFIRMED REAL via production: fetch_fundamentals's own
        # docstring says it deliberately raises everything and expects
        # THE CALLER to catch per-stock -- this call was the one place
        # that contract was never actually honored. yfinance's own
        # library has well-known internal bugs where .info eagerly
        # computes optional sub-structures (earnings estimates, officer
        # lists, etc) and raises a bare TypeError ('NoneType has no
        # len()', 'NoneType is not iterable') when Yahoo's JSON for
        # THIS specific ticker is missing one -- confirmed live on two
        # unambiguously liquid, well-covered names (Coal India, Aadhar
        # Housing Finance), which is the opposite of the usual
        # "obscure ticker, thin data" failure pattern -- MORE analyst
        # coverage data for yfinance to eagerly parse and choke on, not
        # less. Before this fix, letting it propagate meant losing
        # whatever the NSE official API tier above had ALREADY fetched
        # too, for a completely unrelated source's bug. Now: log and
        # keep going with fields as they stand -- same principle as the
        # BSE-side's own wrapper around ITS official-API tier.
        print(f"  [yfinance tier failed] {isin_number} NSE-side (yf={yf_ticker}) -- {e}")
        yf_fields = {}
    if yf_fields:
        for k, v in yf_fields.items():
            fields.setdefault(k, v)
        source_notes.append(f"yf:{yf_ticker}")
    time.sleep(RATE_LIMIT_DELAY_SECONDS)

    # CONFIRMED REAL gap this fixes: REXPIPES (NSE SME) got 6 fields
    # from official+yfinance combined and TradingView was never even
    # tried, despite being missing eps/return_on_equity/
    # debt_to_equity_ratio/dividend_yield -- all fields TradingView
    # actually covers. Checking against TRADINGVIEW_COVERABLE_FIELDS
    # (not just "if not fields") means TradingView is tried whenever
    # there's ANY gap it could plausibly fill, not only when the whole
    # result is empty. Still only fills gaps -- never overrides a value
    # official API or yfinance already produced.
    still_missing = TRADINGVIEW_COVERABLE_FIELDS - fields.keys()
    if still_missing:
        try:
            tv_fields = fetch_fundamentals_tradingview(nse_symbol, "NSE")
        except Exception as e:
            # Same isolation principle as the yfinance wrapper just
            # above -- a TradingView-side crash must not cost the
            # official-API/yfinance fields already gathered.
            print(f"  [TradingView tier failed] {isin_number} NSE-side (NSE={nse_symbol}) -- {e}")
            tv_fields = None
        if tv_fields:
            filled = []
            for k, v in tv_fields.items():
                if k not in fields:
                    fields[k] = v
                    filled.append(k)
            if filled:
                source_notes.append(f"TradingView:NSE:{nse_symbol}(gap-fill:{','.join(filled)})")
        time.sleep(RATE_LIMIT_DELAY_SECONDS)

    return fields, False, source_notes


def _resolve_bse_side(bse_session, isin_number, bse_symbol, bse_exchange, bse_security_code, results, results_lock):
    """
    Resolves and returns the fields for ONE isin's BSE-family row, from
    (BSE official API + yfinance BSE ticker) ONLY -- completely
    independent of whatever happens on the NSE side. Official API wins
    for any field both sources provide.

    NO TRADINGVIEW TIER HERE, deliberately -- removed after TWO
    independently confirmed cases of it returning badly wrong data for
    BSE symbol lookups specifically (ATLPP's market cap off by ~20x,
    JISLDVREQS's off by ~55x against an independent source) -- see this
    file's own header comment for the full reasoning. Whatever the BSE
    official API and yfinance's BSE ticker don't cover simply stays
    missing on this side, rather than risk populating it with
    confidently wrong data.

    Returns (fields, excluded, source_notes) -- excluded=True means
    BSE's own IShow flag says this security isn't a real equity
    (confirmed to correlate with debt instruments -- see bse_client.py's
    own header comment); fields will be {} in that case, and yfinance
    is never attempted, matching NSE-side's isDelisted handling exactly.
    Sleeps RATE_LIMIT_DELAY_SECONDS after each request it makes.
    """
    fields = {}
    source_notes = []

    try:
        bse_fields, bse_index_value = fetch_fundamentals_bse(bse_session, bse_security_code)
        if bse_fields:
            fields.update(bse_fields)
            source_notes.append(f"BSE:{bse_security_code}")
        if bse_index_value:
            fields.setdefault("index_list", [bse_index_value])
    except BseExcluded as e:
        print(f"  [EXCLUDED] {isin_number} BSE-side (BSE={bse_security_code}) -- {e}")
        with results_lock:
            results["excluded"] += 1
        _record_audit(results, results_lock, isin_number, bse_exchange, "EXCLUDED", str(e))
        time.sleep(RATE_LIMIT_DELAY_SECONDS)
        return {}, True, []
    except Exception as e:
        print(f"  [BSE tier failed] {isin_number} BSE-side (BSE={bse_security_code}) -- {e}")
    time.sleep(RATE_LIMIT_DELAY_SECONDS)

    yf_ticker = f"{bse_security_code}.BO"
    try:
        yf_fields = fetch_fundamentals(yf_ticker)
    except Exception as e:
        # Same fix, same reasoning, as _resolve_nse_side's own yfinance
        # wrapper -- this call had the identical gap: the only existing
        # try/except in this function wraps fetch_fundamentals_bse
        # (the OFFICIAL API tier), not this yfinance call that follows
        # it, so a yfinance-internal crash (e.g. INE883D01023/KBS India
        # -- "Expecting value: line 1 column 1", an empty-body JSON
        # parse failure inside yfinance itself) was propagating
        # uncaught, discarding whatever the BSE official API tier had
        # already fetched. Confirmed real via a live production run.
        print(f"  [yfinance tier failed] {isin_number} BSE-side (yf={yf_ticker}) -- {e}")
        yf_fields = {}
    if yf_fields:
        for k, v in yf_fields.items():
            fields.setdefault(k, v)
        source_notes.append(f"yf:{yf_ticker}")
    time.sleep(RATE_LIMIT_DELAY_SECONDS)

    return fields, False, source_notes


def _worker(env_values, work_queue, results_lock, results):
    """
    Pulls (isin_number, nse_symbol, nse_exchange, bse_symbol, bse_exchange,
    bse_security_code) items off the shared queue until it's empty. Opens
    its OWN db connection AND its own NSE() and BSE() sessions -- neither
    psycopg2 connections nor the nse/bse packages' own session/cookies are
    meant to be shared across threads, same reasoning as
    indicators_listener.py's per-thread connections. Both sessions are
    opened ONCE per worker thread and reused across every isin that
    thread processes (not recreated per isin), to avoid repeated
    re-authentication overhead.

    Each queue item produces UP TO TWO independent outcomes -- one for
    the NSE side (if nse_symbol exists), one for the BSE side (if
    bse_security_code exists) -- each with its own [OK]/[NO DATA] log
    line and its own contribution to results["enriched"]/["failed"].
    See this file's header comment for the full reasoning behind why
    the two sides are never merged.
    """
    try:
        conn = get_connection(env_values)
    except DbConnectionError as e:
        print(f"  [worker] Failed to open a connection: {e}")
        return

    NSE_DOWNLOAD_FOLDER.mkdir(exist_ok=True)
    BSE_DOWNLOAD_FOLDER.mkdir(exist_ok=True)

    try:
        with NSE(str(NSE_DOWNLOAD_FOLDER)) as nse_session, BSE(str(BSE_DOWNLOAD_FOLDER)) as bse_session:
            while True:
                try:
                    isin_number, nse_symbol, nse_exchange, bse_symbol, bse_exchange, bse_security_code = work_queue.get_nowait()
                except Empty:
                    break

                # NSE side -- fully independent of the BSE side below,
                # except that its resolved fields are captured here
                # (nse_side_fields_for_sharing) so the BSE side can pull
                # NSE_SHARED_FALLBACK_FIELDS from them afterward -- see
                # this file's header comment for why those 4 fields
                # specifically are a deliberate exception.
                nse_side_fields_for_sharing = {}
                if nse_symbol:
                    try:
                        nse_side_fields, excluded, source_notes = _resolve_nse_side(
                            nse_session, isin_number, nse_symbol, nse_exchange, results, results_lock
                        )
                        if excluded:
                            pass  # already logged and counted inside _resolve_nse_side
                        elif not nse_side_fields:
                            print(f"  [NO DATA] {isin_number} NSE-side (NSE={nse_symbol})")
                            with results_lock:
                                results["failed"] += 1
                            _record_audit(results, results_lock, isin_number, nse_exchange, "NO_DATA",
                                          "No fields returned from NSE official API or yfinance")
                        else:
                            nse_side_fields = {k: normalize_field_value(v) for k, v in nse_side_fields.items()}
                            dropped = update_stock_fundamentals(conn, isin_number, nse_exchange, nse_side_fields)
                            conn.commit()

                            dropped_columns = {column for column, _, _ in dropped}
                            written_fields = {k: v for k, v in nse_side_fields.items() if k not in dropped_columns}
                            nse_side_fields_for_sharing = written_fields

                            for column, value, reason in dropped:
                                print(f"  [FIELD DROPPED] {isin_number} NSE-side -- {column}={value!r} not written: {reason}")
                                _record_audit(results, results_lock, isin_number, nse_exchange, "FIELD_DROPPED",
                                              f"{column}={value!r}: {reason}")

                            if not written_fields:
                                print(f"  [NO DATA] {isin_number} NSE-side (NSE={nse_symbol}) -- "
                                      f"every fetched field was dropped by numeric sanitization")
                                with results_lock:
                                    results["failed"] += 1
                                _record_audit(results, results_lock, isin_number, nse_exchange, "NO_DATA",
                                              "All fetched fields were dropped by numeric sanitization -- nothing written")
                            else:
                                dropped_note = f", {len(dropped)} dropped" if dropped else ""
                                print(f"  [OK] {isin_number} NSE-side -> {' + '.join(source_notes)} -- "
                                      f"{len(written_fields)} field(s) written{dropped_note}: {written_fields}")
                                with results_lock:
                                    results["enriched"] += 1
                    except StockUniversePersistenceError as e:
                        conn.rollback()
                        print(f"  [FAILED] {isin_number} NSE-side -- DB write failed: {e}")
                        with results_lock:
                            results["failed"] += 1
                        _record_audit(results, results_lock, isin_number, nse_exchange, "FAILED", f"DB write failed: {e}")
                    except Exception as e:
                        conn.rollback()
                        print(f"  [FAILED] {isin_number} NSE-side -- error: {e}")
                        with results_lock:
                            results["failed"] += 1
                        _record_audit(results, results_lock, isin_number, nse_exchange, "FAILED", str(e))

                # BSE side -- independent of the NSE side above, EXCEPT
                # for the narrow NSE_SHARED_FALLBACK_FIELDS backfill
                # (see this file's header comment) applied just before
                # writing.
                if bse_security_code:
                    try:
                        bse_side_fields, excluded, source_notes = _resolve_bse_side(
                            bse_session, isin_number, bse_symbol, bse_exchange, bse_security_code, results, results_lock
                        )

                        if excluded:
                            pass  # already logged and counted inside _resolve_bse_side
                        else:
                            shared_from_nse = []
                            for field_name in NSE_SHARED_FALLBACK_FIELDS:
                                if field_name not in bse_side_fields and field_name in nse_side_fields_for_sharing:
                                    bse_side_fields[field_name] = nse_side_fields_for_sharing[field_name]
                                    shared_from_nse.append(field_name)
                            if shared_from_nse:
                                source_notes.append(f"NSE-shared:{','.join(shared_from_nse)}")

                            if not bse_side_fields:
                                print(f"  [NO DATA] {isin_number} BSE-side (BSE={bse_security_code})")
                                with results_lock:
                                    results["failed"] += 1
                                _record_audit(results, results_lock, isin_number, bse_exchange, "NO_DATA",
                                              "No fields returned from BSE official API or yfinance")
                            else:
                                bse_side_fields = {k: normalize_field_value(v) for k, v in bse_side_fields.items()}
                                dropped = update_stock_fundamentals(conn, isin_number, bse_exchange, bse_side_fields)
                                conn.commit()

                                dropped_columns = {column for column, _, _ in dropped}
                                written_fields = {k: v for k, v in bse_side_fields.items() if k not in dropped_columns}

                                for column, value, reason in dropped:
                                    print(f"  [FIELD DROPPED] {isin_number} BSE-side -- {column}={value!r} not written: {reason}")
                                    _record_audit(results, results_lock, isin_number, bse_exchange, "FIELD_DROPPED",
                                                  f"{column}={value!r}: {reason}")

                                if not written_fields:
                                    print(f"  [NO DATA] {isin_number} BSE-side (BSE={bse_security_code}) -- "
                                          f"every fetched field was dropped by numeric sanitization")
                                    with results_lock:
                                        results["failed"] += 1
                                    _record_audit(results, results_lock, isin_number, bse_exchange, "NO_DATA",
                                                  "All fetched fields were dropped by numeric sanitization -- nothing written")
                                else:
                                    dropped_note = f", {len(dropped)} dropped" if dropped else ""
                                    print(f"  [OK] {isin_number} BSE-side -> {' + '.join(source_notes)} -- "
                                          f"{len(written_fields)} field(s) written{dropped_note}: {written_fields}")
                                    with results_lock:
                                        results["enriched"] += 1
                    except StockUniversePersistenceError as e:
                        conn.rollback()
                        print(f"  [FAILED] {isin_number} BSE-side -- DB write failed: {e}")
                        with results_lock:
                            results["failed"] += 1
                        _record_audit(results, results_lock, isin_number, bse_exchange, "FAILED", f"DB write failed: {e}")
                    except Exception as e:
                        conn.rollback()
                        print(f"  [FAILED] {isin_number} BSE-side -- error: {e}")
                        with results_lock:
                            results["failed"] += 1
                        _record_audit(results, results_lock, isin_number, bse_exchange, "FAILED", str(e))
    finally:
        conn.close()


def run_enrichment_batch(env_values, max_metadata_id, limit=None):
    """
    Runs one full enrichment pass over every ISIN currently in
    stock_universe, using a small bounded pool of worker threads, then
    records the outcome as one new row in stock_universe_enrichment_run.

    limit, if given, caps how many ISINs are processed -- for testing
    only. A real run never passes this; run() only sets it when --limit
    is given on the command line.

    For REAL runs only (limit is None): sets
    maintenance_status.is_running=True at the very start, guaranteed to
    be cleared back to False in a finally block no matter how this
    function exits (early return, exception, anything) -- a Spring
    Security filter on the Java side checks this on every request to
    show a global maintenance banner. If this were ever left stuck at
    True, the WHOLE APP stays locked for every user until someone
    notices and fixes it manually, so the finally guarantee here is
    genuinely critical, not just tidy cleanup. --limit test runs
    deliberately skip this entirely, matching the same reasoning as
    skipping record_enrichment_run below -- a developer's quick test
    shouldn't lock out the whole app for every user.
    """
    started_at = datetime.now(timezone.utc)

    if limit is None:
        try:
            conn = get_connection(env_values)
            set_maintenance_running(conn, True)
            conn.close()
        except (DbConnectionError, StockUniversePersistenceError) as e:
            print(f"  [FAILED] Could not set maintenance status -- aborting batch rather than running without the banner active: {e}")
            return

    try:
        _run_enrichment_batch_body(env_values, max_metadata_id, limit, started_at)
    finally:
        if limit is None:
            try:
                conn = get_connection(env_values)
                set_maintenance_running(conn, False)
                conn.close()
            except (DbConnectionError, StockUniversePersistenceError) as e:
                print(f"  [FAILED] Could not clear maintenance status -- MANUAL INTERVENTION NEEDED: "
                      f"the app may be stuck showing the maintenance banner to every user until this is fixed. {e}")


def _run_enrichment_batch_body(env_values, max_metadata_id, limit, started_at):
    """
    The actual batch logic, split out from run_enrichment_batch() purely
    so that function's try/finally around the maintenance-status flag
    can wrap ALL of this cleanly, regardless of which of the several
    early-return paths below gets hit. started_at is passed in from the
    caller (captured before the maintenance flag was even set) rather
    than recomputed here, so it reflects the true start of the whole
    batch, not just this inner function's own start.
    """
    try:
        conn = get_connection(env_values)
        isin_map = fetch_isin_exchange_map(conn)
        conn.close()
    except (DbConnectionError, StockUniversePersistenceError) as e:
        print(f"  [FAILED] Could not fetch isin/exchange map: {e}")
        return

    isins = list(isin_map.items())
    if limit is not None:
        print(f"  --limit {limit} given -- truncating {len(isins)} ISINs down to the first {limit} for this test run.")
        isins = isins[:limit]

    total = len(isins)
    # Worst case 2 requests on the BSE side (official API, yfinance --
    # no TradingView, see this file's own header comment for why) and
    # up to 3 on the NSE side (official API, yfinance,
    # TradingView-if-needed) -- an upper-bound estimate, not exact.
    estimated_minutes = round((total * 5 * RATE_LIMIT_DELAY_SECONDS) / WORKER_THREAD_COUNT / 60, 1)
    print(f"  Enriching {total} ISINs using {WORKER_THREAD_COUNT} worker threads "
          f"({RATE_LIMIT_DELAY_SECONDS}s delay per request per thread, ~{estimated_minutes} min worst-case estimate)...")

    work_queue = Queue()
    for isin_number, entry in isins:
        work_queue.put((isin_number, entry["nse_symbol"], entry["nse_exchange"], entry["bse_symbol"], entry["bse_exchange"], entry["bse_security_code"]))

    results = {"enriched": 0, "failed": 0, "excluded": 0, "audit": []}
    results_lock = threading.Lock()

    threads = [
        threading.Thread(target=_worker, args=(env_values, work_queue, results_lock, results), name=f"enrich-worker-{i}")
        for i in range(WORKER_THREAD_COUNT)
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    completed_at = datetime.now(timezone.utc)
    elapsed_minutes = round((completed_at - started_at).total_seconds() / 60, 1)

    if results["failed"] == 0:
        status = "SUCCESS"
    elif results["enriched"] > 0:
        status = "PARTIAL"
    else:
        status = "FAILED"

    # Counts are per (isin, exchange) SIDE now, not per isin -- a
    # dual-listed company contributes up to 2 to enriched/failed/
    # excluded combined, one for each side, matching the redesign where
    # NSE and BSE are resolved and counted completely independently.
    #
    # Success rate is of ATTEMPTED sides only (enriched vs failed) --
    # excluded sides were never attempted at all (isDelisted/IShow==0
    # skipped them before any API call), so folding them into the
    # denominator would understate how well the actual attempts went.
    attempted_sides = results["enriched"] + results["failed"]
    success_rate = round(100 * results["enriched"] / attempted_sides, 1) if attempted_sides else 0.0

    def _print_summary(persisted):
        tag = "" if persisted else " (NOT PERSISTED -- --limit test run)"
        print("  " + "-" * 58)
        print(f"  Batch complete -- status={status}{tag}")
        print(f"    ISINs processed:  {total}")
        print(f"    Started:          {started_at.isoformat()}")
        print(f"    Completed:        {completed_at.isoformat()}")
        print(f"    Duration:         {elapsed_minutes} min")
        print(f"    Sides enriched:   {results['enriched']}")
        print(f"    Sides excluded:   {results['excluded']} (NSE isDelisted + BSE IShow==0)")
        print(f"    Sides failed:     {results['failed']}")
        print(f"    Success rate:     {success_rate}% (of {attempted_sides} attempted sides, excludes excluded)")
        if results["audit"]:
            print(f"    Non-success detail ({len(results['audit'])} row(s)) -- also written to "
                  f"stock_universe_enrichment_audit{'' if persisted else ', but NOT this time (--limit test run)'}:")
            for row in results["audit"]:
                print(f"      [{row['outcome']}] {row['isin_number']} ({row['exchange']}) -- {row['reason']}")
        print("  " + "-" * 58)

    if limit is not None:
        print(f"  --limit was set -- NOT recording this run in stock_universe_enrichment_run, "
              f"so the persistent cursor stays untouched (a limited test run is not a real complete batch).")
        _print_summary(persisted=False)
        return

    try:
        conn = get_connection(env_values)
        run_id = record_enrichment_run(conn, max_metadata_id, total, results["enriched"], results["failed"], status, started_at)
        if results["audit"]:
            record_enrichment_audit_rows(conn, run_id, results["audit"])
        conn.close()
    except (DbConnectionError, StockUniversePersistenceError) as e:
        print(f"  [FAILED] Batch finished but could not record the outcome: {e}")
        return

    _print_summary(persisted=True)


def poll_once(env_values, limit=None, last_idle_state=None):
    """
    Runs one poll cycle. last_idle_state, if given, is a small mutable
    dict shared across the whole polling loop (see run()) used ONLY to
    avoid re-printing the exact same "nothing to do" message on every
    single cycle when the underlying state hasn't changed. Without
    this, a slow-moving signal like this (uploads happen at most a few
    times a day, polled every POLL_INTERVAL_SECONDS) would flood the
    log with an identical line forever. The banner + idle message
    print once when a given idle state is first seen, then stay
    silent while it remains unchanged; anything that actually changes
    (metadata becomes ready, a new batch appears, an error occurs)
    always prints, regardless of last_idle_state.
    """
    try:
        conn = get_connection(env_values)
        metadata_status = fetch_latest_metadata_status(conn)
        cursor = fetch_last_processed_cursor(conn)
        conn.close()
    except (DbConnectionError, StockUniversePersistenceError) as e:
        print("=" * 60)
        print("  Stock Universe enrichment listener -- poll cycle starting")
        print("=" * 60)
        print(f"  [FAILED] Could not check metadata/cursor: {e}")
        if last_idle_state is not None:
            last_idle_state["key"] = None  # force a fresh banner next time an idle state recurs
        return

    if not is_batch_ready(metadata_status):
        idle_key = ("not_ready",)
        if last_idle_state is not None and last_idle_state.get("key") == idle_key:
            return
        print("=" * 60)
        print("  Stock Universe enrichment listener -- poll cycle starting")
        print("=" * 60)
        print("  Not all 5 exchange uploads are currently SUCCESS -- nothing to do this cycle.")
        print("  (Suppressing this message on subsequent cycles until the state changes.)")
        if last_idle_state is not None:
            last_idle_state["key"] = idle_key
        return

    max_metadata_id = get_max_metadata_id(metadata_status)
    if max_metadata_id <= cursor:
        idle_key = ("no_new_batch", max_metadata_id)
        if last_idle_state is not None and last_idle_state.get("key") == idle_key:
            return
        print("=" * 60)
        print("  Stock Universe enrichment listener -- poll cycle starting")
        print("=" * 60)
        print(f"  Already enriched through metadata id {cursor} -- no new batch since then.")
        print("  (Suppressing this message on subsequent cycles until a new batch appears.)")
        if last_idle_state is not None:
            last_idle_state["key"] = idle_key
        return

    # Something is actually happening -- always print, and reset the
    # idle-state cache so the next idle period logs its banner fresh
    # rather than staying silent based on a now-stale key.
    if last_idle_state is not None:
        last_idle_state["key"] = None

    print("=" * 60)
    print("  Stock Universe enrichment listener -- poll cycle starting")
    print("=" * 60)
    print(f"  New complete batch detected (metadata id {max_metadata_id} > cursor {cursor}) -- starting enrichment.")
    run_enrichment_batch(env_values, max_metadata_id, limit=limit)

    print("  Poll cycle complete.")
    print("=" * 60)


def _open_listen_connection(env_values):
    """
    Opens a DEDICATED connection whose only job is LISTEN <channel> --
    separate from every other connection this listener opens (poll_once
    opens/closes its own short-lived one per cycle, each worker thread
    opens its own for the actual enrichment writes). This one stays
    open for the whole run, idle, just waiting on the socket.

    autocommit=True is REQUIRED here -- LISTEN's effect only persists
    for the lifetime of the session and does not need (and should not
    be wrapped in) an explicit transaction; get_connection()'s default
    autocommit=False would otherwise leave this connection sitting in
    an open transaction indefinitely, which is not what LISTEN needs
    and is generally bad practice for a long-lived idle connection.

    Returns None (never raises) on failure -- a missing LISTEN
    connection should degrade this listener back to pure
    POLL_INTERVAL_SECONDS polling, not crash the whole process; see
    run()'s own handling of a None return here.
    """
    try:
        conn = get_connection(env_values)
    except DbConnectionError as e:
        print(f"[WARNING] Could not open the LISTEN connection ({e}) -- "
              f"falling back to plain {POLL_INTERVAL_SECONDS}s polling for now.")
        return None

    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute(f"LISTEN {LISTEN_CHANNEL};")
    return conn


def _wait_for_notify_or_timeout(listen_conn, timeout_seconds):
    """
    Blocks until EITHER a NOTIFY arrives on listen_conn (which must
    already have run LISTEN <channel> -- see _open_listen_connection)
    OR timeout_seconds elapses, whichever comes first. Uses select() on
    the connection's own socket, which means this call uses ~zero CPU
    while waiting -- not a disguised sleep-and-poll loop.

    The actual CONTENT of any notification is never inspected or
    trusted -- poll_once() re-validates is_batch_ready()/cursor from
    scratch every time it's called regardless of why. This function's
    only job is "wake up sooner than timeout_seconds if something
    happened," not to decide whether that something matters.

    Returns True if woken by select() timing out cleanly (caller can
    treat this the same either way -- see above), and drains
    listen_conn.notifies either way so it doesn't grow unbounded across
    many cycles.
    """
    ready, _, _ = select.select([listen_conn], [], [], timeout_seconds)
    if ready:
        listen_conn.poll()
        listen_conn.notifies.clear()
    return True


def run():
    """Standard entry point -- also callable via main.py, matching every other loader under loaders/."""
    parser = argparse.ArgumentParser(description="Stock Universe enrichment listener")
    parser.add_argument("--once", action="store_true",
                         help="Run a single poll cycle and exit, instead of looping forever. Useful for a first test run.")
    parser.add_argument("--limit", type=int, default=None,
                         help="Cap how many ISINs are enriched in one batch. Testing only -- never use this for a real run.")
    args = parser.parse_args()

    with start_run_logging("stock_universe_update_listener"):
        try:
            env_values = load_and_validate_env()
        except EnvValidationError as e:
            print(f"[FAILED] {e}")
            sys.exit(1)

        if args.once:
            print("Stock Universe enrichment listener -- running a single poll cycle (--once), then exiting.")
            poll_once(env_values, limit=args.limit)
            return

        print(f"Stock Universe enrichment listener starting -- LISTEN/NOTIFY on '{LISTEN_CHANNEL}' for near-instant "
              f"wakeup, {POLL_INTERVAL_SECONDS}s periodic fallback in case a NOTIFY is ever missed. Ctrl+C to stop.")
        last_idle_state = {}
        listen_conn = _open_listen_connection(env_values)

        try:
            while True:
                poll_once(env_values, limit=args.limit, last_idle_state=last_idle_state)

                if listen_conn is not None:
                    try:
                        _wait_for_notify_or_timeout(listen_conn, POLL_INTERVAL_SECONDS)
                    except Exception as e:
                        # The LISTEN connection itself died (DB restart,
                        # network blip, etc) -- fall back to a plain
                        # sleep for THIS cycle and try to reopen a fresh
                        # LISTEN connection for next time, rather than
                        # either crashing the whole listener or silently
                        # running with a broken connection forever.
                        print(f"  [WARNING] LISTEN connection failed ({e}) -- "
                              f"sleeping {POLL_INTERVAL_SECONDS}s and reconnecting.")
                        try:
                            listen_conn.close()
                        except Exception:
                            pass
                        time.sleep(POLL_INTERVAL_SECONDS)
                        listen_conn = _open_listen_connection(env_values)
                else:
                    time.sleep(POLL_INTERVAL_SECONDS)
        except KeyboardInterrupt:
            print("\nShutting down -- any in-flight worker thread will finish its current isin's "
                  "update before exiting (each isin is its own commit, safe to interrupt between isins).")
        finally:
            if listen_conn is not None:
                listen_conn.close()


if __name__ == "__main__":
    run()
