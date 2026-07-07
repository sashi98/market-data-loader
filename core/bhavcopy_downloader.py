# core/bhavcopy_downloader.py
#
# Downloads NSE + BSE BhavCopy files for a single trade date. Mirrors
# BhavCopyDownloadServiceHandler.java exactly for the CURRENT (UDiFF)
# format, plus legacy pre-cutover support for both exchanges:
#   - NSE:  ZIP download, extract the .csv inside (both current + legacy)
#   - BSE:  direct CSV download (current) OR ZIP download, extract the
#           .csv inside (legacy, pre-01-Jan-2024)
#   - HTTP 404 = holiday/weekend/not-yet-published -- treated as a soft
#     "not found" signal, not a hard error
#   - HTTP 200 with an HTML body (confirmed for BSE) is ALSO treated as
#     "not found" -- some exchange error pages return 200 instead of a
#     real 404
#   - File naming: "{EXCHANGE}-BC-{DD-MMM-YYYY}.csv"
#     (e.g. "NSE-BC-19-Jun-2026.csv", "BSE-BC-19-Jun-2026.csv")

import io
import os
import zipfile
from datetime import date

import requests

CONNECT_TIMEOUT_SECONDS = 30
READ_TIMEOUT_SECONDS = 60

# NSE switched to the new UDiFF common bhavcopy format on this date --
# confirmed empirically from real download logs: 29-Dec-2023 and earlier
# 404 on the current URL, 01-Jan-2024 onward downloads successfully on
# it. Anything before this uses NSE's older "historical/EQUITIES" archive
# instead -- same static host (nsearchives.nseindia.com), no session/
# cookies needed, just a different URL + a DIFFERENT CSV COLUMN LAYOUT
# (no Src column, TIMESTAMP instead of TradDt -- handled by
# bhavcopy_parser.py's v1/v2 auto-detection).
#
# NOTE: an earlier version of this constant used 2024-07-08, copied from
# a cutover date in an old JS snippet that turned out to just be a
# conservative threshold, not the actual format-change date -- corrected
# to the real empirical boundary above.
NSE_FORMAT_CUTOVER_DATE = date(2024, 1, 1)

# BSE switched to the same UDiFF format on the same date as NSE (per
# Sashikant's own confirmation) -- separate constant since there's no
# guarantee the two exchanges' cutovers will always coincide, but happens
# to be the same value today.
BSE_FORMAT_CUTOVER_DATE = date(2024, 1, 1)

NSE_URL_PATTERN = "https://nsearchives.nseindia.com/content/cm/BhavCopy_NSE_CM_0_0_0_{YYYYMMDD}_F_0000.csv.zip"
NSE_LEGACY_URL_PATTERN = "https://nsearchives.nseindia.com/content/historical/EQUITIES/{YYYY}/{MMM}/cm{DDMMMYYYY}bhav.csv.zip"
BSE_URL_PATTERN = "https://www.bseindia.com/download/BhavCopy/Equity/BhavCopy_BSE_CM_0_0_0_{YYYYMMDD}_F_0000.CSV"
# Legacy BSE pattern -- confirmed working via browser, but the CSV column
# layout inside the ZIP has NOT yet been seen/confirmed, so
# bhavcopy_parser.py may raise "Unrecognized BhavCopy header format"
# for these until that's added (see backlog).
BSE_LEGACY_URL_PATTERN = "https://www.bseindia.com/download/BhavCopy/Equity/BSE_EQ_BHAVCOPY_{DDMMYYYY}.ZIP"

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)


class BhavCopyNotFoundError(Exception):
    """HTTP 404 -- holiday/weekend/not-yet-published. Not a hard error."""
    pass


class BhavCopyDownloadError(Exception):
    """Any other download failure (network, non-200/404 HTTP, bad ZIP)."""
    pass


def _build_headers(download_url):
    referer = "https://www.nseindia.com/" if "nseindia" in download_url else "https://www.bseindia.com/"
    return {
        "User-Agent": USER_AGENT,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
        "Referer": referer,
    }


def is_nse_legacy_format(trade_date):
    """
    Whether this NSE date falls before the UDiFF format cutover -- a pure
    function of the date, independent of whether the file was actually
    downloaded or reused from disk. Callers should use THIS, not the
    is_legacy_format flag returned by download_bhavcopy(), when checking
    an already-existing (skipped) file -- that flag is only accurate for
    a fresh download, since the skip check happens before the format lookup.
    """
    return trade_date < NSE_FORMAT_CUTOVER_DATE


def is_bse_legacy_format(trade_date):
    """Same as is_nse_legacy_format(), for BSE. See its docstring."""
    return trade_date < BSE_FORMAT_CUTOVER_DATE


def _build_url(exchange, trade_date):
    """
    Returns (url, is_legacy_format).

    Both NSE and BSE: two possible URL patterns depending on trade_date,
    per each exchange's own *_FORMAT_CUTOVER_DATE. All four URLs are on
    static hosts, no session/cookies needed.
    """
    if exchange == "NSE":
        if trade_date >= NSE_FORMAT_CUTOVER_DATE:
            yyyymmdd = trade_date.strftime("%Y%m%d")
            return NSE_URL_PATTERN.replace("{YYYYMMDD}", yyyymmdd), False
        else:
            yyyy = trade_date.strftime("%Y")
            mmm = trade_date.strftime("%b").upper()  # e.g. "DEC"
            ddmmmyyyy = trade_date.strftime("%d") + mmm + yyyy  # e.g. "14DEC2023"
            url = (
                NSE_LEGACY_URL_PATTERN
                .replace("{YYYY}", yyyy)
                .replace("{MMM}", mmm)
                .replace("{DDMMMYYYY}", ddmmmyyyy)
            )
            return url, True
    else:
        if trade_date >= BSE_FORMAT_CUTOVER_DATE:
            yyyymmdd = trade_date.strftime("%Y%m%d")
            return BSE_URL_PATTERN.replace("{YYYYMMDD}", yyyymmdd), False
        else:
            ddmmyyyy = trade_date.strftime("%d%m%Y")  # e.g. "27122023"
            url = BSE_LEGACY_URL_PATTERN.replace("{DDMMYYYY}", ddmmyyyy)
            return url, True


def _build_file_name(exchange, trade_date, extension=".csv"):
    date_str = trade_date.strftime("%d-%b-%Y")  # e.g. 19-Jun-2026
    return f"{exchange}-BC-{date_str}{extension}"


def download_bhavcopy(exchange, trade_date, download_dir):
    """
    exchange:      "NSE" or "BSE"
    trade_date:    python date object
    download_dir:  directory to save into (created if missing)

    Returns (file_path, was_skipped, is_legacy_format) on success.
      was_skipped     -- True if the file already existed on disk
                          (non-empty) and the download was skipped
                          entirely, no network call made.
      is_legacy_format -- True if this came from NSE's pre-cutover
                          "historical/EQUITIES" archive (different CSV
                          column layout -- bhavcopy_parser.py does not
                          yet handle this; always False for BSE and for
                          NSE dates on/after NSE_FORMAT_CUTOVER_DATE).
                          NOTE: when was_skipped=True, this is always
                          False, since the skip check happens before the
                          format lookup -- a previously-downloaded legacy
                          file will still work fine on disk, this flag
                          just isn't recomputed for skipped files.

    Raises BhavCopyNotFoundError on HTTP 404.
    Raises BhavCopyDownloadError on any other failure.
    """
    os.makedirs(download_dir, exist_ok=True)

    file_name = _build_file_name(exchange, trade_date)
    output_path = os.path.join(download_dir, file_name)

    # -- Skip if already downloaded -- a zero-byte file (e.g. from a prior
    # interrupted/failed write) does NOT count as "already downloaded";
    # re-attempt the download in that case rather than trusting a partial file.
    if os.path.exists(output_path) and os.path.getsize(output_path) > 0:
        return output_path, True, False

    download_url, is_legacy_format = _build_url(exchange, trade_date)
    headers = _build_headers(download_url)

    try:
        response = requests.get(
            download_url,
            headers=headers,
            timeout=(CONNECT_TIMEOUT_SECONDS, READ_TIMEOUT_SECONDS),
        )
    except requests.exceptions.RequestException as e:
        raise BhavCopyDownloadError(f"Request failed for {exchange} {trade_date}: {e}")

    if response.status_code == 404:
        raise BhavCopyNotFoundError(
            f"No BhavCopy file found for {exchange} {trade_date} (HTTP 404). "
            f"Likely a holiday/weekend or not yet published. URL: {download_url}"
        )

    if response.status_code != 200:
        raise BhavCopyDownloadError(
            f"HTTP {response.status_code} received from {exchange} for {trade_date}. URL: {download_url}"
        )

    # -- HTML-body check -- BSE (confirmed) sometimes returns its own
    # "page not found" HTML page with a genuine HTTP 200 status instead
    # of a real 404 (seen for 26-29 Dec 2023). Without this check, that
    # HTML gets saved as if it were a valid CSV, and the failure only
    # surfaces confusingly later at parse time ("Unrecognized BhavCopy
    # header format"). Treat this the same as a 404 -- a soft "not
    # available" signal, not a hard error.
    content_start = response.content[:200].lstrip().lower()
    if content_start.startswith(b"<!doctype") or content_start.startswith(b"<html"):
        raise BhavCopyNotFoundError(
            f"No BhavCopy file found for {exchange} {trade_date} (HTTP 200 but HTML page, "
            f"not a CSV/ZIP -- likely the exchange's own \"not found\" page returned with a "
            f"200 status instead of 404). URL: {download_url}"
        )

    if exchange == "NSE" or (exchange == "BSE" and is_legacy_format):
        # NSE (always) and legacy-format BSE (pre-cutover) return a ZIP --
        # extract the .csv inside. Current-format BSE returns a direct CSV.
        try:
            with zipfile.ZipFile(io.BytesIO(response.content)) as zf:
                csv_entries = [name for name in zf.namelist() if name.lower().endswith(".csv")]
                if not csv_entries:
                    raise BhavCopyDownloadError(
                        f"No CSV file found inside {exchange} ZIP archive for {trade_date}. URL: {download_url}"
                    )
                with zf.open(csv_entries[0]) as csv_in, open(output_path, "wb") as csv_out:
                    csv_out.write(csv_in.read())
        except zipfile.BadZipFile as e:
            raise BhavCopyDownloadError(f"Invalid ZIP received for {exchange} {trade_date}: {e}")
    else:
        # Current-format BSE -- direct CSV, write as-is.
        with open(output_path, "wb") as f:
            f.write(response.content)

    return output_path, False, is_legacy_format
