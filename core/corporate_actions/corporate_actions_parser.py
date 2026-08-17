# core/corporate_actions/corporate_actions_parser.py
#
# Normalizes NSE and BSE corporate-actions API responses into row dicts
# ready for corporate_actions_persistence.py, filtered to SPLIT and
# BONUS only (the two action types that mechanically change share
# count/price without changing company value -- the documented root
# cause of the RSI-divergence bug in docs/current-issues.txt; see
# 013.01.00's changelog comment). Every other action type (dividends,
# rights, mergers, etc.) is silently excluded here, not persisted at all
# -- out of scope, not an error.
#
# Both exchanges report the action's ratio as FREE TEXT (NSE's `subject`
# field, e.g. "Bonus 1:1" or "Face Value Split (Sub-Division) - From Rs
# 10/- Per Share To Rs 2/- Per Share") rather than structured numeric
# fields -- this module regex-matches known phrasings to extract the
# ratio. A row whose subject text matches a SPLIT/BONUS keyword but
# whose ratio text doesn't match a known numeric pattern is NOT silently
# guessed at or dropped -- it comes back with face_value_old/new left
# None so the caller can surface it as "unparsed, needs manual review"
# rather than either crashing the whole batch or quietly losing the row.

import re
from datetime import datetime
from decimal import Decimal, InvalidOperation

ACTION_TYPE_SPLIT = "SPLIT"
ACTION_TYPE_BONUS = "BONUS"

_BONUS_KEYWORDS = ("bonus",)
_SPLIT_KEYWORDS = ("split", "sub-division", "sub division", "subdivision")

_BONUS_RATIO_RE = re.compile(r"bonus\D*(\d+)\s*:\s*(\d+)", re.IGNORECASE)
# "r[se]" (not just "rs") on purpose -- NSE uses the grammatically-correct
# singular "Re 1/-" when a split lands exactly on Re. 1 face value (e.g.
# "From Rs 10/- Per Share To Re 1/- Per Share"), vs. plural "Rs" for every
# other value. A plain "rs"-only pattern silently fails to parse any split
# whose new (or old) face value is exactly 1, leaving face_value_old/new
# None and the action stuck unparsed/NSE_ONLY forever (found via TEMBO,
# isin INE869Y01010, ex_date 2026-08-05).
_SPLIT_FACE_VALUE_RE = re.compile(
    r"from\s+r[se]\.?\s*(\d+(?:\.\d+)?)\s*/?-?\s*.*?to\s+r[se]\.?\s*(\d+(?:\.\d+)?)\s*/?-?",
    re.IGNORECASE | re.DOTALL,
)

# Date formats tried, in order, for each exchange's own ex-date field.
_NSE_DATE_FORMATS = ["%d-%b-%Y", "%d-%m-%Y"]
_BSE_DATE_FORMATS = ["%d %b %Y", "%d-%m-%Y", "%Y%m%d"]


class CorporateActionsParseError(Exception):
    """Raised when a raw action row is missing a required structural
    field (isin, symbol, or ex_date) -- these are hard failures, unlike
    an unrecognized free-text ratio, which is not."""
    pass


def _classify(subject_text):
    text = (subject_text or "").lower()
    if any(k in text for k in _BONUS_KEYWORDS):
        return ACTION_TYPE_BONUS
    if any(k in text for k in _SPLIT_KEYWORDS):
        return ACTION_TYPE_SPLIT
    return None


def _extract_bonus_ratio(subject_text):
    """
    Returns (face_value_old, face_value_new) from a "Bonus N:M" style
    subject -- NSE convention: N:M reads as "N new shares for every M
    held" -- shaped so that face_value_new / face_value_old gives the
    correct price multiplier, same formula
    _factor_from_face_values() uses for splits.

    IMPORTANT, easy to get backwards: a bonus INCREASES share count and
    DECREASES per-share price, same direction as a split's face value
    going down -- so the "old" slot here holds the LARGER number
    (total_shares_after = held + new) and the "new" slot holds the
    SMALLER one (held_shares), even though total_shares_after
    chronologically comes AFTER held_shares. E.g. a 1:1 bonus (1 new
    share per 1 held) must produce factor 0.5 (price halves), which
    requires (face_value_old, face_value_new) = (2, 1), i.e.
    (held + new, held) -- NOT (held, held + new). Returns None if the
    text doesn't match.
    """
    match = _BONUS_RATIO_RE.search(subject_text or "")
    if not match:
        return None
    new_shares = Decimal(match.group(1))
    held_shares = Decimal(match.group(2))
    if held_shares == 0:
        return None
    return held_shares + new_shares, held_shares


def _extract_split_face_values(subject_text):
    """
    Returns (face_value_old, face_value_new) from a "... From Rs X/- ...
    To Rs Y/- ..." style subject, or None if the text doesn't match.
    """
    match = _SPLIT_FACE_VALUE_RE.search(subject_text or "")
    if not match:
        return None
    try:
        face_value_old = Decimal(match.group(1))
        face_value_new = Decimal(match.group(2))
    except InvalidOperation:
        return None
    if face_value_old == 0:
        return None
    return face_value_old, face_value_new


def _parse_date_safe(value, formats):
    if not value:
        return None
    value = value.strip()
    for fmt in formats:
        try:
            return datetime.strptime(value, fmt).date()
        except ValueError:
            continue
    return None


def _parse_rows(raw_rows, exchange, field_map, date_formats):
    """
    Shared row-normalization loop for both exchanges -- field_map maps
    our canonical field names (isin, symbol, subject, ex_date) to the
    list of candidate keys to try against each raw row dict, in order
    (each exchange's response uses different casing/naming).
    """
    def _get(raw, canonical_field):
        for key in field_map[canonical_field]:
            value = raw.get(key)
            if value:
                return str(value).strip()
        return ""

    results = []
    for raw in raw_rows:
        isin = _get(raw, "isin")
        symbol = _get(raw, "symbol")
        subject = _get(raw, "subject")
        ex_date_raw = _get(raw, "ex_date")

        if not isin or not symbol or not ex_date_raw:
            raise CorporateActionsParseError(
                f"{exchange} corporate action row missing isin/symbol/ex_date: {raw}"
            )

        ex_date = _parse_date_safe(ex_date_raw, date_formats)
        if ex_date is None:
            raise CorporateActionsParseError(
                f"Could not parse ex_date '{ex_date_raw}' for {symbol} ({isin}, {exchange})"
            )

        action_type = _classify(subject)
        if action_type is None:
            continue  # out of scope (dividend/rights/merger/etc.), not an error

        face_value_old = face_value_new = None
        if action_type == ACTION_TYPE_BONUS:
            ratio = _extract_bonus_ratio(subject)
            if ratio is not None:
                face_value_old, face_value_new = ratio
        elif action_type == ACTION_TYPE_SPLIT:
            face_values = _extract_split_face_values(subject)
            if face_values is not None:
                face_value_old, face_value_new = face_values

        results.append({
            "isin": isin,
            "symbol": symbol,
            "exchange": exchange,
            "action_type": action_type,
            "ex_date": ex_date,
            "raw_ratio_text": subject.strip(),
            "face_value_old": face_value_old,
            "face_value_new": face_value_new,
        })

    return results


def parse_nse_corporate_actions(raw_rows):
    """
    raw_rows: the list of dicts returned by
    corporate_actions_downloader.download_nse_corporate_actions().

    Returns a list of normalized row dicts: isin, symbol, exchange="NSE",
    action_type (SPLIT/BONUS), ex_date, raw_ratio_text, face_value_old,
    face_value_new. See module docstring for scope/error-handling rules.
    """
    field_map = {
        "isin": ["isin"],
        "symbol": ["symbol"],
        "subject": ["subject"],
        "ex_date": ["exDate", "exdate"],
    }
    return _parse_rows(raw_rows, "NSE", field_map, _NSE_DATE_FORMATS)


def parse_bse_corporate_actions(raw_rows):
    """
    Same contract as parse_nse_corporate_actions(), for BSE's response
    shape -- CONFIRMED against a real live response on 2026-08-10 (field
    names below, not a guess anymore). Real BSE rows look like:
    {"scrip_code": 519105, "short_name": "AVTNPL", "long_name": "AVT
    Natural Products Ltd", "Purpose": "Final Dividend - Rs. - 0.4500",
    "Ex_date": "10 Aug 2026", "exdate": "20260810", "RD_Date": ...}.

    Notably, BSE's response has NO isin field at all -- only scrip_code,
    BSE's own numeric identifier. raw_rows passed in here are expected
    to already have gone through
    corporate_actions_persistence.resolve_bse_scrip_codes() first, which
    injects a resolved "isin" key (via stock_universe.SECURITY_CODE) --
    that's why "isin" is still in the candidate list below even though
    BSE itself never sends it.
    """
    field_map = {
        "isin": ["ISIN_CODE", "Isin", "isin"],
        "symbol": ["SCRIP_NAME", "Scrip_Name", "symbol", "short_name"],
        "subject": ["PURPOSE", "Purpose", "purpose"],
        "ex_date": ["Ex_date", "EXDATE", "ex_date"],
    }
    return _parse_rows(raw_rows, "BSE", field_map, _BSE_DATE_FORMATS)
