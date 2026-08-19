# core/stock_universe/bse_client.py
#
# BSE's own official (unofficial-but-legitimate, session/cookie managed,
# rate-limited) API, via the `bse` Python package (PyPI: bse,
# github.com/BennyThadikaran/BseIndiaApi) -- same author as the `nse`
# package already integrated, same design philosophy (with-statement
# session management, built-in rate limiting).
#
# Unlike NSE's own API, BSE's equityMetaInfo() is genuinely richer in
# one important way: it directly provides PE, PB, EPS, and ROE --
# confirmed via a real sample response (ETERNAL/Zomato, scrip 543320).
# Those four fields were previously ONLY available from yfinance/
# TradingView for BSE-listed companies; this gives an authoritative,
# official-source alternative for BSE specifically, not just a mirror
# of NSE's own integration.
#
# CONFIRMED GAP, not assumed parity with NSE: the real sample response
# has no listing-date field at all, and Index is a single string (e.g.
# "BSE SENSEX"), not a rich multi-membership list like NSE's own
# indexList (30 entries for RELIANCE). Wrapped as a one-item list here
# for column-format consistency (index_list is always "a list of index
# names," regardless of how rich the source is), but this is
# meaningfully thinner than NSE's own data for the same concept.
#
# No confirmed exclusion-flag equivalent to NSE's isDelisted was found
# in the real sample response (no Status/isDelisted-style field) --
# this module does NOT raise any exclusion exception, unlike
# nse_client.py's NseExcluded. If BSE's API is later found to expose
# one, this should be added the same way, not before that's confirmed.

# CONFIRMED REAL BUG, caught via a live raw-response comparison for
# RELIANCE (security_code 500325): equityMetaInfo's plain "PE"/"EPS"
# fields are STANDALONE figures (parent company only, excluding
# subsidiaries) -- a separate "Con"-prefixed set (ConPE/ConEPS/ConPB/
# ConROE) holds CONSOLIDATED figures instead. For RELIANCE specifically:
# standalone PE=45.66 vs consolidated ConPE=20.31 -- NSE's OWN official
# P/E (20.43, independently confirmed earlier) matches BSE's
# CONSOLIDATED figure almost exactly, not standalone, which is more than
# DOUBLE the real comparable figure. yfinance's own P/E (~24) and EPS
# (55.21) are also far closer to BSE's consolidated basis than standalone.
#
# Fixed accordingly: price_to_earnings_ratio and eps now use the Con*
# fields. price_to_book_ratio and return_on_equity deliberately STILL
# use the standalone PB/ROE fields -- the same live response had
# ConPB/ConROE both null for RELIANCE, so switching those two to
# consolidated would trade a working standalone value for a guaranteed
# gap on some companies, without the same kind of independent
# confirmation P/E and EPS got. Revisit if a case is found where
# standalone PB/ROE themselves prove wrong the same concrete way P/E did.

# CONFIRMED REAL BUG, caught via a live cross-check against NSE's own
# data for RELIANCE: BSE's plain "Sector"/"Industry" fields are a
# DIFFERENT, coarser classification scheme than NSE's own
# sector/industry taxonomy (BSE Sector="Energy" vs NSE
# sector="Oil Gas & Consumable Fuels" for the SAME company) -- while
# "IndustryNew"/"IGroup" match NSE's sector/industry almost exactly
# (IndustryNew="Oil, Gas & Consumable Fuels", IGroup="Petroleum
# Products", both confirmed against real NSE output for RELIANCE).
# Fixed accordingly: sector now reads IndustryNew, industry now reads
# IGroup -- this keeps a dual-listed company's NSE row and BSE row
# reporting the SAME classification taxonomy, rather than two
# different schemes that happen to share column names.

# CONFIRMED REAL BUG, caught via live tests against BSE debt
# instruments (ANNVRPP/KRISHPP/ATLPP, the same 89xxxx-range codes
# confirmed as genuine NCDs earlier this session): equityMetaInfo
# returns the literal STRING "0.0" for PE and EPS on instruments where
# those concepts don't apply at all -- a bond has no earnings-per-share
# concept, so BSE's system defaults to "0.0" as a placeholder rather
# than omitting the field. "0.0" is a non-empty string, so it passed
# the old "if pe:" truthiness check and got stored as if it were a real
# value -- no real company's actual EPS is ever exactly 0.0. Fixed by
# checking the parsed FLOAT isn't exactly zero before accepting it, for
# all four ratio fields (PE, PB, EPS, ROE), not just the two confirmed
# by name -- the same placeholder pattern could plausibly appear on any
# of them for the same underlying reason.

# CONFIRMED REAL exclusion flag, found and verified across 5 real data
# points: IShow=="1" for every real equity checked (RELIANCE, ADANIENT,
# ETERNAL); IShow=="0" for every confirmed debt instrument checked
# (ATLPP/890228, KRISHPP/890232 -- both independently verified as
# genuine NCDs via bseindia.com's own "Debt" tag and all-blank
# Sector/IndustryNew/IGroup fields). This is the BSE equivalent of
# NSE's isDelisted flag -- an EARLIER, cleaner signal than the PE=0.0
# placeholder pattern above, since it catches the whole category before
# ever reaching yfinance/TradingView, rather than discovering pollution
# after the fact (CONFIRMED REAL: TradingView returned a market cap for
# ATLPP nearly 20x its actual, confirmed-via-bseindia.com figure --
# actively wrong data, not just absent).
#
# RETRY ADDED 2026-08-19: the equityMetaInfo call below now goes through
# core.retry.call_with_retry first, so a transient network blip (e.g.
# `curl: (16)`, an HTTP2 framing-layer error observed in a real
# production run -- see core/retry.py's own header comment for the full
# incident) gets a couple of extra attempts before this module's own
# exception finally propagates to the caller (stock_universe_update_
# listener.py's _resolve_bse_side, which already has a broad
# except Exception around this call). This does NOT change what happens
# once every attempt is exhausted -- a still-failing call raises exactly
# the same way it always did.

from core.retry import call_with_retry, MAX_ATTEMPTS


class BseExcluded(Exception):
    """
    Raised (not returned) when equityMetaInfo's own IShow flag says this
    security should be skipped entirely -- confirmed to correlate with
    debt instruments (NCDs), not real equities. Callers should NOT
    persist anything or fall through to yfinance/TradingView when this
    is raised, matching nse_client.NseExcluded's exact same contract.
    str(exception) is a short reason ("not-equity"), suitable for
    logging directly.
    """
    pass


def fetch_fundamentals_bse(bse_session, security_code):
    """
    Calls equityMetaInfo(security_code) for one BSE scrip, using an
    already-open BSE() session -- see stock_universe_update_listener.py's
    _worker, which opens ONE session per worker thread and reuses it
    across every isin that thread processes, rather than
    re-authenticating per call (same pattern as the NSE session).

    Returns (common_fields, index_value) -- common_fields is a plain
    dict of company-level facts (never None, {} if nothing usable came
    back); index_value is the raw BSE Index string (e.g. "BSE SENSEX"),
    or None if absent. Splitting the return this way (rather than
    returning one merged dict like nse_client does) is deliberate --
    unlike NSE's fetch_fundamentals_nse, this function does not know
    which exchange the caller wants the index scoped to, so it hands
    back the raw value and lets stock_universe_update_listener.py's
    _worker decide the scope (same reasoning as yfinance's date_of_listing
    needing the caller to know whether the primary or fallback ticker
    answered).

    Raises BseExcluded if equityMetaInfo's own IShow flag says this
    security is not a real equity (confirmed to correlate with debt
    instruments -- see this file's own header comment). Callers should
    treat this exactly like nse_client.NseExcluded.

    Raises whatever bse-package/network exception occurs otherwise, AFTER
    retrying a couple of times first (see core/retry.py and this file's
    own header comment) -- same contract as nse_client.fetch_fundamentals_nse:
    callers catch and log per-isin, this function does not swallow errors
    itself once retries are exhausted.
    """
    meta = call_with_retry(
        bse_session.equityMetaInfo, security_code,
        on_retry=lambda attempt, e: print(
            f"    [retry {attempt}/{MAX_ATTEMPTS}] BSE equityMetaInfo({security_code}) -- {e}"
        ),
    )
    if not meta:
        return {}, None

    if str(meta.get("IShow", "1")) == "0":
        raise BseExcluded("not-equity")

    fields = {}

    # CONFIRMED via a live cross-check against NSE's own data for
    # RELIANCE (see this file's header comment) -- IndustryNew matches
    # NSE's sector taxonomy, NOT BSE's own plain "Sector" field, which
    # is a different, coarser classification scheme.
    sector = meta.get("IndustryNew")
    if sector:
        fields["sector"] = sector

    # CONFIRMED via the same cross-check -- IGroup matches NSE's
    # industry taxonomy, NOT BSE's own plain "Industry"/"ISubGroup"
    # fields (which are themselves identical to each other, just a
    # DIFFERENT, more granular scheme than IGroup/NSE's own industry).
    industry = meta.get("IGroup")
    if industry:
        fields["industry"] = industry

    pe = meta.get("ConPE")
    if pe:
        try:
            pe_value = float(pe)
            if pe_value != 0:
                fields["price_to_earnings_ratio"] = pe_value
        except (TypeError, ValueError):
            pass

    # CONFIRMED NEW -- not available from NSE's own API at all. Only
    # previously covered by yfinance/TradingView for BSE-listed stocks.
    # Deliberately standalone, NOT ConPB -- see this module's header
    # comment for why (ConPB was null for RELIANCE in the real response
    # this decision was based on, with no independent confirmation
    # standalone PB is itself wrong the way standalone PE/EPS were).
    pb = meta.get("PB")
    if pb:
        try:
            pb_value = float(pb)
            if pb_value != 0:
                fields["price_to_book_ratio"] = pb_value
        except (TypeError, ValueError):
            pass

    eps = meta.get("ConEPS")
    if eps:
        try:
            eps_value = float(eps)
            if eps_value != 0:
                fields["eps"] = eps_value
        except (TypeError, ValueError):
            pass

    # Deliberately standalone, NOT ConROE -- same reasoning as PB above.
    roe = meta.get("ROE")
    if roe:
        try:
            roe_value = float(roe)
            if roe_value != 0:
                fields["return_on_equity"] = roe_value
        except (TypeError, ValueError):
            pass

    index_value = meta.get("Index")
    if index_value in (None, "", "-"):
        index_value = None

    return fields, index_value
