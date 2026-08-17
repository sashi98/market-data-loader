# core/corporate_actions/corporate_actions_downloader.py
#
# Downloads NSE + BSE corporate-actions (splits/bonus) data for a
# calendar date range. Unlike bhavcopy_downloader.py's static-host,
# no-cookies-needed files, NSE's corporate-actions endpoint lives behind
# their dynamic web app (nseindia.com/api/...) and requires a warm-up
# GET against the public site first to pick up session cookies before
# the API call returns real JSON instead of a 401/403 -- same reasoning
# every NSE-scraping tool documents. A fresh, short-lived
# requests.Session is used per call rather than a shared one, since this
# loader is a low-frequency, manually-triggered tool (see
# corporate_actions_loader.py), not a hot path.
#
# BSE_CORPORATE_ACTIONS_URL and its query params -- CONFIRMED working
# against a real live response on 2026-08-10 (203 raw rows returned for
# a ~5-week window). What was NOT correct on the first real run was the
# assumed response field names in corporate_actions_parser.py's BSE
# field_map (fixed once the real shape was seen) -- see that module's
# docstring for the confirmed real field names, and
# corporate_actions_persistence.resolve_bse_scrip_codes() for why BSE's
# response needing isin resolved from scrip_code (not sent directly)
# was the actual blocker, not this URL/these params.

import csv
import io

import requests

CONNECT_TIMEOUT_SECONDS = 30
READ_TIMEOUT_SECONDS = 60

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)

NSE_HOME_URL = "https://www.nseindia.com/"
NSE_CORPORATE_ACTIONS_URL = "https://www.nseindia.com/api/corporates-corporateActions"

# CONFIRMED working -- see module docstring.
BSE_CORPORATE_ACTIONS_URL = "https://api.bseindia.com/BseIndiaAPI/api/DefaultData/w"

# CSV-EXPORT endpoints -- CONFIRMED (2026-08-13) to be genuinely
# date-range-aware, unlike BSE_CORPORATE_ACTIONS_URL above (which was
# proven, via two independent live probes, to always return the same
# rolling ~1-month "forthcoming actions" window regardless of
# from_date/to_date -- see corporate_actions_parser.py's module
# docstring history / tests/probe_bse_date_range_READONLY.py). These
# are the actual network requests behind each exchange's own "download
# csv" export button:
#   BSE:  bseindia.com/corporates/corporate_act.aspx -- "Kindly download
#         the csv file to view all records" link.
#   NSE:  the same corporates-corporateActions endpoint used above,
#         with &csv=true -- confirmed via narrow-vs-wide-window probes
#         that row counts and ex-dates actually change with the
#         requested range (unlike the BSE DefaultData/w widget).
# Both exchanges are standardized on their CSV export now (rather than
# NSE staying on the JSON endpoint above and only BSE moving to CSV) --
# one consistent code path/format for both exchanges going forward.
# NSE's JSON functions above are left in place, unused by the CSV
# pipeline, as a working fallback.
NSE_CORPORATE_ACTIONS_CSV_URL = "https://www.nseindia.com/api/corporates-corporateActions"
BSE_CORPORATE_ACTIONS_CSV_URL = "https://api.bseindia.com/BseIndiaAPI/api/CorpactCSVDownload/w"


class CorporateActionsDownloadError(Exception):
    """Any download failure (network, non-200 HTTP, unexpected/unparseable body)."""
    pass


def _build_headers(referer):
    return {
        "User-Agent": USER_AGENT,
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.5",
        "Referer": referer,
    }


def download_nse_corporate_actions(from_date, to_date):
    """
    from_date/to_date: python date objects.

    Returns the parsed JSON body (a list of dicts, one per action) from
    NSE's corporates-corporateActions API, filtered to equities, for
    [from_date, to_date].

    An EMPTY list is a normal, valid result (no actions in this window)
    -- not an error. Raises CorporateActionsDownloadError on any
    network/HTTP failure or an unexpected (non-JSON-list) response body.
    """
    session = requests.Session()
    headers = _build_headers(NSE_HOME_URL)

    try:
        # Warm-up request -- NSE's API rejects requests without valid
        # session cookies picked up from a prior visit to the site itself.
        session.get(NSE_HOME_URL, headers=headers, timeout=(CONNECT_TIMEOUT_SECONDS, READ_TIMEOUT_SECONDS))

        params = {
            "index": "equities",
            "from_date": from_date.strftime("%d-%m-%Y"),
            "to_date": to_date.strftime("%d-%m-%Y"),
        }
        response = session.get(
            NSE_CORPORATE_ACTIONS_URL,
            headers=headers,
            params=params,
            timeout=(CONNECT_TIMEOUT_SECONDS, READ_TIMEOUT_SECONDS),
        )
    except requests.exceptions.RequestException as e:
        raise CorporateActionsDownloadError(
            f"Request failed for NSE corporate actions {from_date} to {to_date}: {e}"
        )
    finally:
        session.close()

    if response.status_code != 200:
        raise CorporateActionsDownloadError(
            f"HTTP {response.status_code} received from NSE corporate actions API for {from_date} to {to_date}."
        )

    try:
        body = response.json()
    except ValueError as e:
        raise CorporateActionsDownloadError(
            f"NSE corporate actions API did not return valid JSON for {from_date} to {to_date}: {e}"
        )

    if not isinstance(body, list):
        raise CorporateActionsDownloadError(
            f"NSE corporate actions API returned an unexpected body shape (expected a JSON list) "
            f"for {from_date} to {to_date}."
        )

    return body


def download_bse_corporate_actions(from_date, to_date):
    """
    Same contract as download_nse_corporate_actions(), for BSE.

    URL/params CONFIRMED working (see module docstring) -- returns a
    bare JSON list of dicts, no "Table" wrapper, in real usage so far.
    """
    headers = _build_headers("https://www.bseindia.com/")
    params = {
        "strCat": "Corp Action",
        "strPrevDate": from_date.strftime("%Y%m%d"),
        "strScrip": "",
        "strSearch": "S",
        "strToDate": to_date.strftime("%Y%m%d"),
        "strType": "C",
    }

    try:
        response = requests.get(
            BSE_CORPORATE_ACTIONS_URL,
            headers=headers,
            params=params,
            timeout=(CONNECT_TIMEOUT_SECONDS, READ_TIMEOUT_SECONDS),
        )
    except requests.exceptions.RequestException as e:
        raise CorporateActionsDownloadError(
            f"Request failed for BSE corporate actions {from_date} to {to_date}: {e}"
        )

    if response.status_code != 200:
        raise CorporateActionsDownloadError(
            f"HTTP {response.status_code} received from BSE corporate actions API for {from_date} to {to_date}."
        )

    try:
        body = response.json()
    except ValueError as e:
        raise CorporateActionsDownloadError(
            f"BSE corporate actions API did not return valid JSON for {from_date} to {to_date}: {e}"
        )

    # BSE's real response shape is unconfirmed -- normalize both a bare
    # list and a {"Table": [...]} wrapper (a shape BSE's api.bseindia.com
    # family commonly uses elsewhere), since guessing wrong here would
    # otherwise silently drop every row rather than raise.
    if isinstance(body, dict) and "Table" in body:
        body = body["Table"]

    if not isinstance(body, list):
        raise CorporateActionsDownloadError(
            f"BSE corporate actions API returned an unexpected body shape for {from_date} to {to_date}."
        )

    return body


def _parse_csv_response_text(response_content, exchange, from_date, to_date):
    """
    Shared CSV-body-to-list-of-dicts helper for both exchanges' CSV
    downloaders below.

    Decodes with utf-8-sig explicitly (NOT response.text/apparent
    encoding) -- both NSE and BSE prefix their CSV export bodies with a
    UTF-8 byte-order-mark, and neither declares charset=utf-8 in their
    Content-Type header, so `requests` falls back to guessing (often
    Latin-1/cp1252), which mangles the BOM into literal 'ï»¿' characters
    glued onto the first column's header (confirmed while first
    testing this endpoint) -- utf-8-sig strips a leading BOM cleanly if
    present and is a no-op if it's absent, safe either way.

    Returns a list of dicts (one per CSV data row, keyed by header),
    via csv.DictReader -- same shape callers already get from the JSON
    download functions above, so parse_nse_corporate_actions()/
    parse_bse_corporate_actions() only need each row reshaped to their
    known field_map keys, not a whole new parsing path.
    """
    text = response_content.decode("utf-8-sig")
    stripped = text.strip()
    if not stripped:
        return []
    if stripped.startswith("<") or stripped.startswith("{"):
        raise CorporateActionsDownloadError(
            f"{exchange} corporate actions CSV endpoint returned a non-CSV body "
            f"for {from_date} to {to_date} (first 200 chars: {stripped[:200]!r})."
        )
    return list(csv.DictReader(io.StringIO(text)))


def download_nse_corporate_actions_csv(from_date, to_date):
    """
    CSV-export counterpart to download_nse_corporate_actions() above --
    see NSE_CORPORATE_ACTIONS_CSV_URL's comment for why this is now the
    standard NSE path. Confirmed via narrow-vs-wide-window probes to
    genuinely respect from_date/to_date (89 rows for a single month vs.
    5793 for the full 2024-01-01..2026-08-14 range, all within the
    requested window).

    from_date/to_date: python date objects.

    Returns a list of dicts, one per CSV row -- raw column names as NSE
    sends them (e.g. "SYMBOL", "PURPOSE", "EX-DATE"), NOT yet reshaped
    to parse_nse_corporate_actions()'s field_map, and with NO isin field
    at all (NSE's CSV export has no ISIN column, unlike its JSON
    endpoint) -- callers must resolve isin themselves, e.g. via
    corporate_actions_persistence.resolve_nse_symbols(), before parsing.

    Raises CorporateActionsDownloadError on any network/HTTP failure or
    an unexpected (non-CSV) response body.
    """
    session = requests.Session()
    headers = _build_headers(NSE_HOME_URL)

    try:
        # Same warm-up requirement as the JSON endpoint -- see module docstring.
        session.get(NSE_HOME_URL, headers=headers, timeout=(CONNECT_TIMEOUT_SECONDS, READ_TIMEOUT_SECONDS))

        params = {
            "index": "equities",
            "from_date": from_date.strftime("%d-%m-%Y"),
            "to_date": to_date.strftime("%d-%m-%Y"),
            "csv": "true",
        }
        response = session.get(
            NSE_CORPORATE_ACTIONS_CSV_URL,
            headers=headers,
            params=params,
            timeout=(CONNECT_TIMEOUT_SECONDS, READ_TIMEOUT_SECONDS),
        )
    except requests.exceptions.RequestException as e:
        raise CorporateActionsDownloadError(
            f"Request failed for NSE corporate actions CSV {from_date} to {to_date}: {e}"
        )
    finally:
        session.close()

    if response.status_code != 200:
        raise CorporateActionsDownloadError(
            f"HTTP {response.status_code} received from NSE corporate actions CSV endpoint "
            f"for {from_date} to {to_date}."
        )

    return _parse_csv_response_text(response.content, "NSE", from_date, to_date)


def download_bse_corporate_actions_csv(from_date, to_date):
    """
    CSV-export counterpart to download_bse_corporate_actions() above --
    THIS is the real BSE historical corporate-actions source; the
    DefaultData/w function above is a "forthcoming actions" widget only
    (see its module-level comment) and should not be used for anything
    date-range-sensitive. Confirmed via narrow-vs-wide-window probes to
    genuinely respect Fdate/TDate (89 rows for Jan 2024 alone vs. 6497
    for the full 2024-01-01..2026-08-14 range, all within the requested
    window).

    from_date/to_date: python date objects.

    Returns a list of dicts, one per CSV row -- raw column names as BSE
    sends them (e.g. "Security Code", "Security Name", "Purpose",
    "Ex Date"), NOT yet reshaped to parse_bse_corporate_actions()'s
    field_map, and with NO isin field (BSE's CSV export reports
    "Security Code", its own numeric scrip code, same as the JSON
    endpoint) -- callers must resolve isin via
    corporate_actions_persistence.resolve_bse_scrip_codes() before
    parsing (that function already expects a "scrip_code" key, so
    reshape "Security Code" -> "scrip_code" first).

    Raises CorporateActionsDownloadError on any network/HTTP failure or
    an unexpected (non-CSV) response body.
    """
    headers = _build_headers("https://www.bseindia.com/corporates/corporate_act.aspx")
    params = {
        "scripcode": " ",
        "Fdate": from_date.strftime("%Y%m%d"),
        "TDate": to_date.strftime("%Y%m%d"),
        "Purposecode": "",
        "strSearch": "S",
        "ddlindustrys": "",
        "ddlcategorys": "E",
        "segment": "0",
    }

    try:
        response = requests.get(
            BSE_CORPORATE_ACTIONS_CSV_URL,
            headers=headers,
            params=params,
            timeout=(CONNECT_TIMEOUT_SECONDS, READ_TIMEOUT_SECONDS),
        )
    except requests.exceptions.RequestException as e:
        raise CorporateActionsDownloadError(
            f"Request failed for BSE corporate actions CSV {from_date} to {to_date}: {e}"
        )

    if response.status_code != 200:
        raise CorporateActionsDownloadError(
            f"HTTP {response.status_code} received from BSE corporate actions CSV endpoint "
            f"for {from_date} to {to_date}."
        )

    return _parse_csv_response_text(response.content, "BSE", from_date, to_date)
