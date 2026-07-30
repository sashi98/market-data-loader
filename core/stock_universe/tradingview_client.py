# core/stock_universe/tradingview_client.py
#
# Third-tier fallback for stock_universe enrichment -- used only when
# BOTH the NSE and BSE yfinance attempts (yfinance_client.
# fetch_fundamentals_for_isin) returned nothing for an isin. Wraps the
# tradingview-screener package, which queries TradingView's own screener
# API directly (no HTML scraping, no bot-detection issues like NSE's
# Akamai-blocked quote-equity endpoint).
#
# Field names below are confirmed against real sources -- most against
# tradingview-screener's own official documentation
# (https://shner-elmo.github.io/TradingView-Screener/fields/stocks.html),
# and price_book_fq against an actual code example from a related
# library (tradingview-scraper) after the original guess for that field
# (price_to_book_ratio) was tested live against ACCPL and came back
# missing -- 7 of 8 expected fields worked, that one didn't. ONE field
# remains genuinely unconfirmed: return_on_equity_fq (a plausible guess
# following the same _fq suffix pattern as debt_to_equity/price_book_fq,
# but never independently verified -- it DID come back for ACCPL's live
# test, 27.6%, a plausible value, so treat it as corroborated by that
# one result rather than fully confirmed by documentation). If it's
# ever wrong for some other symbol, that field will simply never
# populate -- it will not raise an error, since the field-extraction
# loop below only reads whatever columns actually come back.
#
# CONFIRMED WORKING via a real live test against ACCPL (an NSE SME
# stock yfinance had zero data for) -- returned sector, industry,
# market cap, P/E, EPS, debt-to-equity, and ROE, all matching real
# published values (NSE's own site, TradingView's own screener UI).

from tradingview_screener import stocks, col

# Maps TradingView's own field name -> our stock_universe column name.
FIELD_MAP = {
    "sector": "sector",
    "industry": "industry",
    "market_cap_basic": "market_capitalization",
    "price_earnings_ttm": "price_to_earnings_ratio",
    "earnings_per_share_diluted_ttm": "eps",
    "debt_to_equity": "debt_to_equity_ratio",
    # "Dividend Yield Forward" per TradingView's own field label -- a
    # real semantic difference from yfinance's trailing dividendYield,
    # not just a naming quirk. Both end up in the same dividend_yield
    # column regardless of which tier populated it -- worth remembering
    # if the two ever look inconsistent for the same stock.
    "dividend_yield_recent": "dividend_yield",
    # UNCONFIRMED field name -- see this file's header comment.
    "return_on_equity_fq": "return_on_equity",
    # CONFIRMED via an actual code example from a related library
    # (tradingview-scraper's own README) after "price_to_book_ratio"
    # turned out wrong on a real live test -- ACCPL returned 7 of 8
    # expected fields with that guess, missing exactly this one.
    "price_book_fq": "price_to_book_ratio",
}

# The set of stock_universe columns TradingView can EVER fill, derived
# directly from FIELD_MAP above -- used by callers (
# stock_universe_update_listener.py's _resolve_nse_side/_resolve_bse_side)
# to decide whether TradingView is worth attempting even when official
# API + yfinance already returned SOME data, not just when the result
# is completely empty. CONFIRMED REAL gap this fixes: REXPIPES (NSE SME)
# got 6 fields from official+yfinance combined and TradingView was never
# even tried, despite being missing eps/return_on_equity/
# debt_to_equity_ratio/dividend_yield -- all fields TradingView actually
# covers. date_of_listing/earnings_date/index_list are deliberately NOT
# in this set -- TradingView never covers those regardless of how the
# gap-check is written, so there's no point treating a missing
# date_of_listing as a reason to spend a TradingView request.
TRADINGVIEW_COVERABLE_FIELDS = frozenset(FIELD_MAP.values())


def fetch_fundamentals_tradingview(symbol, exchange):
    """
    Queries TradingView's screener API for ONE specific symbol.

    symbol should be whichever text symbol is available for this isin
    (nse_symbol preferred, bse_symbol otherwise -- see
    stock_universe_update_listener.py's _worker for how the caller
    decides which to pass). exchange should be the STOCK_UNIVERSE
    exchange value ("NSE", "NSE SME", "BSE", "BSE SME") -- collapsed
    here to plain "NSE"/"BSE" for TradingView's own exchange filter,
    matching yfinance_client.build_ticker's identical NSE-family/
    BSE-family collapsing logic.

    Returns an empty dict (never None) if nothing came back, matching
    yfinance_client.fetch_fundamentals's own contract -- callers can
    treat "found nothing" the same way regardless of which tier answered.

    Raises whatever tradingview_screener/network exception occurs --
    same design as yfinance_client.fetch_fundamentals: callers catch and
    log per-stock, this function does not swallow errors itself.
    """
    tv_exchange = "BSE" if (exchange or "").upper().startswith("BSE") else "NSE"

    tv_columns = list(FIELD_MAP.keys())
    # Confirmed real bug on first live test: bare Query() defaults to
    # scanning ONLY the 'america' market (per the library's own
    # documented example JSON payload, 'markets': ['america']) --
    # filtering by exchange == 'NSE' alone against that default scope
    # matches nothing at all, regardless of symbol, since NSE was never
    # even in the scanned market to begin with. stocks('india') scopes
    # the query to the right market first, matching the library's own
    # documented pattern (stocks('italy'), etc.) for exactly this reason.
    count, df = (
        stocks("india")
        .select(*tv_columns)
        .where(
            col("name") == symbol,
            col("exchange") == tv_exchange,
        )
        .limit(1)
        .get_scanner_data()
    )

    if count == 0 or df.empty:
        return {}

    row = df.iloc[0]
    fields = {}
    for tv_field, our_column in FIELD_MAP.items():
        if tv_field not in row:
            continue
        value = row[tv_field]
        # NaN check -- pandas represents a missing numeric field as
        # NaN, not None. Same pattern as company_info_controller.py's
        # own safe() helper.
        if value is not None and value == value:
            fields[our_column] = value

    return fields
