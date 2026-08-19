# core/stock_universe/yfinance_client.py
#
# Wraps yfinance -- builds the correct ticker string for a stock_universe
# row, fetches its .info dict, and maps whatever fields are present onto
# stock_universe's actual column names. Every unit-conversion/mapping
# decision below is documented with exactly how confident it is -- some
# of these were confirmed against a real sample this session, some are
# reasonable assumptions never independently verified. Treat the
# "UNCONFIRMED" ones as a checklist for whoever eventually watches this
# run for real, not as settled fact.
#
# RETRY ADDED 2026-08-19: the ticker.info fetch below now goes through
# core.retry.call_with_retry first, so a transient network blip (e.g.
# `curl: (16)`, an HTTP2 framing-layer error observed in a real
# production run -- see core/retry.py's own header comment for the full
# incident) gets a couple of extra attempts before this function's own
# exception finally propagates to the caller. This does NOT change what
# happens once every attempt is exhausted -- fetch_fundamentals's own
# docstring/contract (raises, never swallows) is unchanged; callers
# (stock_universe_update_listener.py's _resolve_nse_side/_resolve_bse_side)
# already wrap this call in their own try/except.

from datetime import datetime, timezone

import yfinance as yf

from core.retry import call_with_retry, MAX_ATTEMPTS

# Deliberately conservative -- yfinance/Yahoo Finance is known to
# rate-limit or outright block aggressive scraping. This delay is PER
# WORKER THREAD, applied after every single request (success or
# failure) -- see stock_universe_update_listener.py's WORKER_THREAD_COUNT
# for the other half of the rate-limiting strategy (a small bounded pool,
# not one thread per stock).
RATE_LIMIT_DELAY_SECONDS = 1.5


def build_ticker(symbol, exchange, security_code):
    """
    NSE and NSE SME both -> SYMBOL.NS -- yfinance doesn't need to know
    or care about the SME/mainboard distinction for NSE; the text
    symbol works reliably either way.

    BSE and BSE SME both -> SECURITY_CODE.BO -- BSE's own numeric scrip
    code, NOT the text symbol. The text symbol was confirmed unreliable
    for BSE on yfinance during this listener's own design -- this is
    exactly why security_code was added to stock_universe (and to the
    BSE/BSE SME CSV parsers) specifically to support this function.

    Returns None if a BSE-family stock has no security_code at all --
    shouldn't happen given the parsers always set it for active BSE
    securities, but handled defensively here rather than assumed, so a
    caller can skip cleanly instead of building a nonsense ticker like
    "None.BO".
    """
    exch = (exchange or "").upper()
    if exch.startswith("BSE"):
        if not security_code:
            return None
        return f"{security_code}.BO"
    return f"{symbol}.NS"


def fetch_fundamentals(ticker_symbol):
    """
    Fetches yfinance's raw .info dict for one ticker and maps it onto
    stock_universe's real column names. Returns an empty dict (never
    None) if the ticker has no usable data at all, so callers can treat
    "found nothing" and "found some fields" the same way -- just check
    truthiness.

    Raises whatever yfinance/network exception occurs, AFTER retrying a
    couple of times first (see core/retry.py and this file's own header
    comment) -- this function does NOT catch and swallow errors once
    retries are exhausted. Callers (the worker threads in
    stock_universe_update_listener.py) are responsible for catching
    per-stock and continuing, matching the explicit design decision that
    one bad ticker must never abort the whole batch.
    """
    ticker = yf.Ticker(ticker_symbol)
    info = call_with_retry(
        lambda: ticker.info,
        on_retry=lambda attempt, e: print(
            f"    [retry {attempt}/{MAX_ATTEMPTS}] yfinance {ticker_symbol} -- {e}"
        ),
    )

    # yfinance sometimes returns a near-empty dict for a delisted or
    # simply wrong ticker, rather than raising an exception -- treat the
    # absence of ANY real price signal as "no data" rather than writing
    # a mostly-null row that looks like a successful, if sparse, update.
    if not info or (info.get("regularMarketPrice") is None and info.get("currentPrice") is None):
        return {}

    fields = {}

    if info.get("sector"):
        # Checked for truthiness, not just "is not None" -- yfinance
        # sometimes returns an empty string for sector/industry rather
        # than omitting the key entirely (confirmed via a real test
        # run: PREMCO and BNAGROCHEM both came back with sector="",
        # industry=""). An "is not None" check alone would have written
        # that empty string into the column as if it were real data.
        fields["sector"] = info["sector"]

    if info.get("industry"):
        fields["industry"] = info["industry"]

    if info.get("marketCap") is not None:
        fields["market_capitalization"] = info["marketCap"]

    if info.get("trailingPE") is not None:
        fields["price_to_earnings_ratio"] = info["trailingPE"]

    if info.get("priceToBook") is not None:
        fields["price_to_book_ratio"] = info["priceToBook"]

    if info.get("trailingEps") is not None:
        fields["eps"] = info["trailingEps"]

    if info.get("returnOnEquity") is not None:
        # x100 applied here -- corroborated (not independently confirmed
        # against a published figure, but observed to produce plausible
        # values in a real test run: 3M India showed 28.9% ROE, a
        # believable figure for a company known for high margins;
        # 21STCENMGM showed -57.2%, plausible for a struggling company).
        # Unlike dividendYield above, this one held up under real data
        # rather than being overturned by it.
        fields["return_on_equity"] = info["returnOnEquity"] * 100

    if info.get("debtToEquity") is not None:
        # Stored as-is, no multiplication -- corroborated by a real test
        # run (360ONE showed 161.977, 20MICRONS 32.043, 3M India 9.6,
        # all plausible D/E figures already). Still never independently
        # confirmed against a published figure for a specific company.
        fields["debt_to_equity_ratio"] = info["debtToEquity"]

    if info.get("dividendYield") is not None:
        # REVERSED, confirmed wrong via a real live test run --
        # previously documented here as "confirmed" based on a sample
        # JSON showing dividendYield as a fraction (0.0041 for 0.41%),
        # and multiplied by 100 accordingly. Real output overturned
        # that: 360ONE, 20MICRONS, 3M India, and 63MOONS all showed
        # implausible triple-digit "dividend yields" (162.0, 61.0, 46.0,
        # 17.0) with that multiplication applied. Dividing those same
        # raw values by 100 instead gives immediately plausible numbers
        # (1.62%, 0.61%, 0.46%, 0.17%) -- so yfinance's LIVE API is
        # apparently already returning this as a plain percentage
        # number, not a fraction, whatever that original sample showed.
        # Stored as-is now, matching debt_to_equity_ratio's own
        # never-multiplied treatment below.
        fields["dividend_yield"] = info["dividendYield"]

    first_trade_millis = info.get("firstTradeDateMilliseconds")
    if first_trade_millis is not None:
        # FIXED, confirmed via a raw .info dump for RELIANCE
        # (stock-py-services' company_info_controller.py, logging
        # ticker.info directly). The key this project assumed --
        # "firstTradeDateEpochUtc" -- does NOT exist anywhere in the
        # live response at all, which is exactly why this field silently
        # never populated across every test run and the full production
        # run. The real key is "firstTradeDateMilliseconds", and it's in
        # MILLISECONDS, not seconds -- dividing by 1000 before converting.
        # Still the SAME underlying caveat as before, though: this is
        # "the first date Yahoo has price data for this ticker," not
        # necessarily the real listing/IPO date. The planned
        # verification (checking this against Zomato's real, publicly
        # documented 23-Jul-2021 listing date) was never completed --
        # worth doing now that the mapping itself is at least correct.
        fields["date_of_listing"] = datetime.fromtimestamp(first_trade_millis / 1000, tz=timezone.utc).date()

    earnings_timestamp = info.get("earningsTimestamp")
    if earnings_timestamp is not None:
        # FIXED/ADDED, confirmed via the same raw .info dump --
        # "earningsTimestamp" (seconds, standard epoch, NOT milliseconds
        # like firstTradeDateMilliseconds above) is genuinely present for
        # RELIANCE, alongside earningsTimestampStart/End and
        # isEarningsDateEstimate. Previously documented here as "no
        # confirmed field was ever found" -- that was wrong, based on
        # checking a hypothetical/text sample rather than a real live
        # dict. Converts to a plausible date (~Jul 2026 for RELIANCE),
        # consistent with an upcoming/estimated earnings date.
        fields["earnings_date"] = datetime.fromtimestamp(earnings_timestamp, tz=timezone.utc).date()

    return fields
