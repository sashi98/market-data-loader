# core/retry.py
#
# Generic retry-with-backoff helper for the network-calling tiers in
# core/stock_universe/*_client.py.
#
# Added 2026-08-19 after a real production batch (5434 ISINs, 7821
# attempted sides) surfaced a small number of hard NO_DATA/FAILED
# outcomes traced back to transient network-layer errors -- specifically
# `curl: (16)` ("A problem was detected in the HTTP2 framing layer"),
# a libcurl-level dropped/reset connection, surfacing from the
# underlying nse/bse-package/yfinance/tradingview_screener HTTP clients
# (all of which use curl_cffi or similar under the hood to get past
# NSE/BSE/Yahoo/TradingView's own bot-detection). Confirmed via a full
# source review of every network tier (nse_client.py, bse_client.py,
# yfinance_client.py, tradingview_client.py) and the orchestration in
# stock_universe_update_listener.py: NONE of them retried anything --
# every external call was attempted exactly once. A single transient
# blip therefore permanently failed that (isin, exchange) side for the
# whole run, even though the overwhelming majority of sides (7818/7821,
# 99.96%) succeeded on their one and only attempt -- consistent with
# rare transient noise, not a systemic problem with any of these
# stocks' data.
#
# Deliberately generic -- NOT tied to any one library's specific
# exception type. curl_cffi, requests, the nse package, and the bse
# package can each raise different exception classes for what is
# functionally the same transient condition (a dropped connection,
# a timeout, a mid-response reset), and trying to enumerate every one
# of them here would be a losing game of whack-a-mole that silently
# stops working the moment any of these libraries changes its internal
# exception types. Retries on ANY exception instead, bounded by
# MAX_ATTEMPTS, with a short fixed delay between attempts.
#
# Matches this codebase's existing per-tier isolation philosophy
# exactly: a call that still fails after every retry attempt raises
# the same way an unretried call always did, so every existing
# try/except at the call site (NseExcluded/BseExcluded passthrough,
# broad Exception -> {} degradation, etc.) keeps working completely
# unchanged. This module ONLY adds retries in front of the call; it
# never changes what a caller ultimately sees on final failure.

import socket
import threading
import time

# ── TMT-MDL-US-0006 (21-Nov-2026): wait out an internet outage instead of giving up ─────────────────────
# On 21-Nov-2026 a power cut took the internet down at ~09:54 during a stock universe enrichment run: every
# call failed its 3 quick attempts in ~4 s and 116 stocks were written off as [NO DATA]. Now a failure that
# looks like lost connectivity (DNS / unreachable / refused / connect timeout) is checked against a real
# probe; if the internet is really down, every worker pauses here until it is back and then retries the SAME
# call -- nothing is skipped and the waiting does not use up the 3 attempts. A failure while the internet is
# fine (404, empty reply, a slow site) is counted as before, so one bad site never pauses the run.

# Hosts tried by the probe; the internet counts as up when ANY of them answers on 443.
PROBE_HOSTS = ("www.nseindia.com", "query1.finance.yahoo.com", "www.google.com")
PROBE_TIMEOUT_SECONDS = 5
PROBE_INTERVAL_SECONDS = 30        # how often a waiting run checks again
WAIT_LOG_EVERY_SECONDS = 300       # "still waiting" line every 5 min
WAIT_WARN_AFTER_SECONDS = 3600     # louder line once the outage passes an hour (the wait itself never ends)

_CONNECTIVITY_MARKERS = (
    "could not resolve host", "nameresolutionerror", "name or service not known",
    "temporary failure in name resolution", "getaddrinfo failed", "nodename nor servname",
    "network is unreachable", "no route to host", "connection refused",
    "failed to establish a new connection", "connecttimeout", "connection timed out",
    "curl: (6)", "curl: (7)", "curl: (28)", "dns server returned general failure",
)

_wait_lock = threading.Lock()


def looks_like_connectivity_error(exc):
    """True when the exception (or anything it wraps) reads like lost connectivity rather than bad data."""
    seen, e = set(), exc
    while e is not None and id(e) not in seen:
        seen.add(id(e))
        text = f"{type(e).__name__} {e}".lower()
        if any(m in text for m in _CONNECTIVITY_MARKERS):
            return True
        e = e.__cause__ or e.__context__
    return False


def internet_available():
    """One quick probe: can we resolve and open a TCP connection to any PROBE_HOSTS on 443?"""
    for host in PROBE_HOSTS:
        try:
            with socket.create_connection((host, 443), timeout=PROBE_TIMEOUT_SECONDS):
                return True
        except OSError:
            continue
    return False


def wait_until_online(log=print):
    """
    Blocks until internet_available(). Returns the seconds spent waiting (0 when it was up). Only one thread
    probes and logs; the other workers queue on the lock and return as soon as it is back.
    """
    with _wait_lock:
        if internet_available():
            return 0
        started = time.time()
        log(f"  [INTERNET DOWN] since {time.strftime('%H:%M:%S')} -- pausing; checking every "
            f"{PROBE_INTERVAL_SECONDS}s, the run resumes from here once it is back.")
        last_log, warned = started, False
        while True:
            time.sleep(PROBE_INTERVAL_SECONDS)
            if internet_available():
                waited = time.time() - started
                log(f"  [INTERNET BACK] after {int(waited // 60)} min {int(waited % 60)} s -- resuming.")
                return waited
            now = time.time()
            if not warned and now - started >= WAIT_WARN_AFTER_SECONDS:
                warned = True
                log(f"  [WARNING] internet still down after {int((now - started) // 60)} min -- still waiting "
                    f"(maintenance stays on; the bhav copy run waits for this enrichment).")
                last_log = now
            elif now - last_log >= WAIT_LOG_EVERY_SECONDS:
                log(f"  [INTERNET DOWN] still waiting ({int((now - started) // 60)} min so far) ...")
                last_log = now


# 3 total attempts (1 initial + 2 retries). Not configurable via env --
# this is a narrow, deliberately small safety margin against transient
# noise, not a general-purpose resilience knob; if 3 attempts with a
# short delay aren't enough, the underlying issue is very likely NOT
# transient (e.g. a genuinely dead symbol, a real API outage), and
# retrying further would just slow the batch down without helping.
MAX_ATTEMPTS = 3

# Deliberately short and fixed, not exponential -- RATE_LIMIT_DELAY_SECONDS
# (yfinance_client.py) already spaces out requests from the SAME worker
# thread; this is specifically for "this one call hit a transient
# network blip, try it again in a couple seconds," not a rate-limit
# backoff strategy of its own.
RETRY_DELAY_SECONDS = 2


def call_with_retry(fn, *args, on_retry=None, **kwargs):
    """
    Calls fn(*args, **kwargs), retrying up to MAX_ATTEMPTS total attempts
    on ANY exception, sleeping RETRY_DELAY_SECONDS between attempts.

    Re-raises the LAST exception if every attempt fails -- callers keep
    their existing try/except handling exactly as before; this only adds
    retries in front of the call, it never changes what an exhausted
    caller ultimately sees.

    on_retry, if given, is called as on_retry(attempt, exception) after
    each FAILED attempt (before sleeping, and not called at all on the
    final exhausted attempt's own failure since there's nothing left to
    retry into) -- used by callers to print a "[retry N/MAX]" line, so
    operators can tell "failed once, retried, succeeded" (fine, expected
    occasionally) apart from "failed on every attempt" (worth watching)
    from the log alone, rather than these two very different outcomes
    looking identical.
    """
    last_exception = None
    attempt = 1
    while attempt <= MAX_ATTEMPTS:
        try:
            return fn(*args, **kwargs)
        except Exception as e:
            last_exception = e
            # TMT-MDL-US-0006: lost internet -> wait until it is back, then retry this same call without
            # counting the attempt. internet_available() first, so a site-specific failure is not a pause.
            if looks_like_connectivity_error(e) and not internet_available():
                wait_until_online()
                continue
            if attempt < MAX_ATTEMPTS:
                if on_retry:
                    on_retry(attempt, e)
                time.sleep(RETRY_DELAY_SECONDS)
            attempt += 1
    raise last_exception
