# core/stock_universe/nse_client.py
#
# NSE's own official (unofficial-but-legitimate, session/cookie managed)
# API, via the `nse` Python package (PyPI: nse,
# github.com/BennyThadikaran/NseIndiaApi). NOW THE PRIMARY source for
# NSE-family stocks' sector, industry, market cap, P/E, and
# date_of_listing -- confirmed via a live test against RELIANCE to
# return EXACT matches (listingDate, basicIndustry, P/E) against a
# manually-verified NSE webpage screenshot. This is meaningfully more
# authoritative for these specific fields than yfinance/TradingView,
# which is why NSE is tried FIRST for any isin with an nse_symbol.
#
# NSE structurally does NOT expose P/B, EPS, ROE, debt-to-equity,
# dividend yield, or earnings date anywhere in this response (confirmed
# by inspecting the full raw structure, not just absence for one
# stock) -- yfinance/TradingView still run afterward for every isin
# specifically to cover those, not as a "maybe NSE missed something"
# fallback.
#
# NSE-only -- there is no equivalent official BSE package found or used
# here. BSE-only isins (no nse_symbol) skip this tier entirely.
#
# Confirmed real via equityMetaInfo: NSE also exposes authoritative
# isDelisted/isETFSec/isDebtSec flags -- checked here to SKIP enrichment
# entirely for stocks that shouldn't be enriched at all. This is a real,
# new exclusion capability -- the name-keyword exclusion in
# persistence.py only ever caught mutual funds/ETFs by NAME pattern;
# delisted stocks were never excluded at all before this.
#
# CONFIRMED REAL BUG, caught via a live check on ADANIENT (a major,
# unambiguously real, actively-traded NIFTY 50 equity): equityMetaInfo's
# isDebtSec/isETFSec flags do NOT mean "is this symbol itself a debt
# security/ETF" -- they appear to mean "does this ISSUER also have a
# debt series (or similar) listed separately," which is an extremely
# common, completely normal situation for a large conglomerate (a
# company can have real equity AND separately-listed corporate bonds
# outstanding at the same time). Treating that as a reason to exclude
# the EQUITY symbol entirely was simply the wrong question to ask.
# Both flags removed from the exclusion check below -- only
# isDelisted is kept, since "is this specific symbol currently
# delisted" is a much less ambiguous, genuinely binary concept, without
# the same issuer-vs-series scoping confusion. A more robust future
# approach, if ETF/debt exclusion is still wanted, would likely be
# checking activeSeries directly for a real equity series code (EQ,
# BE, SM, ST, etc.) as a POSITIVE signal, rather than trusting these
# negative flags -- not yet implemented, since this fix was urgent and
# that redesign needs its own verification first.
#
# CONFIRMED REAL BUG, caught via a live test against ACCPL (NSE SME):
# getDetailedScripData defaults to series='EQ', which 404s for SME
# stocks (ACCPL's real series is 'ST', confirmed via a manual NSE
# screenshot). fetch_fundamentals_nse now tries a short list of series
# codes for exchanges containing "SME", instead of assuming 'EQ'
# universally. Also confirmed real via that same test: an uncaught
# generic exception here previously would have escaped all the way up
# to the WORKER's outer exception handler in
# stock_universe_update_listener.py, marking the entire isin FAILED
# outright -- never even attempting yfinance or TradingView. Every
# function below now deliberately swallows ordinary failures and
# returns {} instead of raising, so a bad NSE call can never prevent
# the other two tiers from being tried. NseExcluded is the ONLY
# exception this module still lets escape, since it's a deliberate
# signal based on successfully-retrieved data, not a failure.

from datetime import datetime

# Tried in order for exchanges containing "SME" -- 'ST' is confirmed
# correct for at least one real stock (ACCPL); 'SM'/'SZ' are the other
# documented SME-family series values in the nse package's own type
# hints, included since ACCPL alone doesn't confirm which applies to
# every SME stock universally.
SME_SERIES_CANDIDATES = ["ST", "SM", "SZ"]


class NseExcluded(Exception):
    """
    Raised (not returned) when equityMetaInfo's own isDelisted flag says
    this isin should be skipped entirely. Callers should NOT persist
    anything or fall through to yfinance/TradingView when this is
    raised; str(exception) is the short reason ("delisted"), suitable
    for logging directly.

    Only isDelisted triggers this now -- isETFSec/isDebtSec were removed
    after a confirmed real false positive on ADANIENT. See this
    module's header comment.

    This is the ONLY exception fetch_fundamentals_nse lets escape --
    everything else (network errors, 404s from a wrong series code,
    NSE being temporarily unavailable, etc.) is caught internally and
    degrades to returning {} instead, so a bad NSE call can never
    prevent yfinance/TradingView from still being tried for this isin.
    """
    pass


def _get_exclusion_flags(nse_session, symbol):
    """
    Returns the exclusion reason string ("delisted") or None. Returns
    None (not excluded) if equityMetaInfo itself fails for any reason --
    an inability to CHECK the flag is not the same as confirming the
    stock is fine to exclude, but it also shouldn't block enrichment
    entirely; yfinance/TradingView can still proceed.

    ONLY checks isDelisted now -- isETFSec/isDebtSec were removed after
    a confirmed real false positive on ADANIENT (a major NIFTY 50
    equity, wrongly excluded as "debt"). See this module's header
    comment for why those two flags don't mean what their names suggest.
    """
    try:
        meta = nse_session.equityMetaInfo(symbol)
    except Exception:
        return None

    if str(meta.get("isDelisted", "false")).lower() == "true":
        return "delisted"
    return None


def _get_detailed_scrip_data(nse_session, symbol, exchange):
    """
    Tries getDetailedScripData with the right series code(s) for this
    exchange. Returns the raw dict on the first successful call, or
    None if every attempt failed (never raises -- see this module's
    header comment for why).
    """
    series_candidates = SME_SERIES_CANDIDATES if "SME" in (exchange or "").upper() else ["EQ"]

    for series in series_candidates:
        try:
            return nse_session.getDetailedScripData(symbol, series=series)
        except Exception:
            continue

    return None


def fetch_fundamentals_nse(nse_session, symbol, exchange=None):
    """
    Calls equityMetaInfo (for exclusion flags) then getDetailedScripData
    (for the actual fields) for one NSE symbol, using an already-open
    NSE() session -- see stock_universe_update_listener.py's _worker,
    which opens ONE session per worker thread and reuses it across
    every isin that thread processes, rather than re-authenticating
    per call.

    exchange (the stock_universe exchange value, e.g. "NSE SME")
    determines which series code(s) getDetailedScripData is tried with
    -- see SME_SERIES_CANDIDATES above.

    Returns {} (never None) for anything that isn't a deliberate
    exclusion -- no data found, wrong series exhausted every candidate,
    a network error, anything. See this module's header comment for
    why ordinary failures are swallowed here rather than raised.

    Raises NseExcluded ONLY if equityMetaInfo's own flags say this isin
    should never be enriched from ANY source, not just skipped here.
    """
    exclusion_reason = _get_exclusion_flags(nse_session, symbol)
    if exclusion_reason:
        raise NseExcluded(exclusion_reason)

    data = _get_detailed_scrip_data(nse_session, symbol, exchange)
    if not data:
        return {}

    responses = data.get("equityResponse")
    if not responses:
        return {}

    scrip = responses[0]
    sec_info = scrip.get("secInfo") or {}
    trade_info = scrip.get("tradeInfo") or {}

    fields = {}

    sector = sec_info.get("sector")
    if sector:
        fields["sector"] = sector

    # industryInfo confirmed more specific than basicIndustry in the
    # real RELIANCE test ("Petroleum Products" vs "Refineries &
    # Marketing") -- preferring the more granular one, falling back to
    # the broader one only if industryInfo is absent.
    industry = sec_info.get("industryInfo") or sec_info.get("basicIndustry")
    if industry:
        fields["industry"] = industry

    pe = sec_info.get("pdSymbolPe")
    if pe:
        try:
            fields["price_to_earnings_ratio"] = float(pe)
        except (TypeError, ValueError):
            pass

    market_cap = trade_info.get("totalMarketCap")
    if market_cap:
        try:
            fields["market_capitalization"] = float(market_cap)
        except (TypeError, ValueError):
            pass

    listing_date_raw = sec_info.get("listingDate")
    if listing_date_raw:
        # Confirmed real format via a live test: "29-Nov-1995 00:00:00".
        try:
            fields["date_of_listing"] = datetime.strptime(
                listing_date_raw.strip(), "%d-%b-%Y %H:%M:%S"
            ).date()
        except ValueError:
            pass

    index_list = sec_info.get("indexList")
    if index_list:
        # Confirmed real via a live test for RELIANCE -- 30 real index
        # memberships returned (NIFTY 50, NIFTY ENERGY, etc). Stored as
        # a plain Python list here; persistence.py's
        # update_stock_fundamentals_by_isin is responsible for wrapping
        # it correctly for the JSON column on write.
        fields["index_list"] = index_list

    return fields
