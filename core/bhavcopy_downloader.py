# core/bhavcopy_downloader.py
#
# Downloads NSE + BSE BhavCopy files for a single trade date. Mirrors
# BhavCopyDownloadServiceHandler.java exactly:
#   - NSE:  ZIP download, extract the .csv inside
#   - BSE:  direct CSV download
#   - Same URL patterns, same browser-like headers, same timeouts
#   - HTTP 404 = holiday/weekend/not-yet-published -- treated as a soft
#     "not found" signal, not a hard error
#   - File naming: "{EXCHANGE}-BC-{DD-MMM-YYYY}.csv"
#     (e.g. "NSE-BC-19-Jun-2026.csv", "BSE-BC-19-Jun-2026.csv")

import io
import os
import zipfile

import requests

CONNECT_TIMEOUT_SECONDS = 30
READ_TIMEOUT_SECONDS = 60

NSE_URL_PATTERN = "https://nsearchives.nseindia.com/content/cm/BhavCopy_NSE_CM_0_0_0_{YYYYMMDD}_F_0000.csv.zip"
BSE_URL_PATTERN = "https://www.bseindia.com/download/BhavCopy/Equity/BhavCopy_BSE_CM_0_0_0_{YYYYMMDD}_F_0000.CSV"

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


def _build_url(exchange, trade_date):
    yyyymmdd = trade_date.strftime("%Y%m%d")
    pattern = NSE_URL_PATTERN if exchange == "NSE" else BSE_URL_PATTERN
    return pattern.replace("{YYYYMMDD}", yyyymmdd)


def _build_file_name(exchange, trade_date, extension=".csv"):
    date_str = trade_date.strftime("%d-%b-%Y")  # e.g. 19-Jun-2026
    return f"{exchange}-BC-{date_str}{extension}"


def download_bhavcopy(exchange, trade_date, download_dir):
    """
    exchange:      "NSE" or "BSE"
    trade_date:    python date object
    download_dir:  directory to save into (created if missing)

    Returns the saved file path (str) on success.
    Raises BhavCopyNotFoundError on HTTP 404.
    Raises BhavCopyDownloadError on any other failure.
    """
    download_url = _build_url(exchange, trade_date)
    headers = _build_headers(download_url)

    os.makedirs(download_dir, exist_ok=True)

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

    file_name = _build_file_name(exchange, trade_date)
    output_path = os.path.join(download_dir, file_name)

    if exchange == "NSE":
        # Response body is a ZIP -- extract the .csv inside.
        try:
            with zipfile.ZipFile(io.BytesIO(response.content)) as zf:
                csv_entries = [name for name in zf.namelist() if name.lower().endswith(".csv")]
                if not csv_entries:
                    raise BhavCopyDownloadError(
                        f"No CSV file found inside NSE ZIP archive for {trade_date}. URL: {download_url}"
                    )
                with zf.open(csv_entries[0]) as csv_in, open(output_path, "wb") as csv_out:
                    csv_out.write(csv_in.read())
        except zipfile.BadZipFile as e:
            raise BhavCopyDownloadError(f"Invalid ZIP received for NSE {trade_date}: {e}")
    else:
        # BSE -- direct CSV, write as-is.
        with open(output_path, "wb") as f:
            f.write(response.content)

    return output_path
