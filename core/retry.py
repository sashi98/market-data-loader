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

import time

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
    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            return fn(*args, **kwargs)
        except Exception as e:
            last_exception = e
            if attempt < MAX_ATTEMPTS:
                if on_retry:
                    on_retry(attempt, e)
                time.sleep(RETRY_DELAY_SECONDS)
    raise last_exception
