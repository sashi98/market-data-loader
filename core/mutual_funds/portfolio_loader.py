# core/mutual_funds/portfolio_loader.py -- TMT-MDL-US-0008
#
# Loads one month of scheme portfolios for every fund house in portfolio_sources: listing (page or API) -> the month's
# file links -> download -> parse (portfolio_parser) -> save (one transaction per file). A file already loaded with the same
# checksum is skipped; a changed file is reloaded. One fund house failing never stops the others. Returns a coverage
# summary for the log and mf_scheduler_run.

import hashlib
import io
import json
import logging
import re
from html import unescape
from urllib.parse import quote, unquote, urljoin

from core.browser_identity import browser_get, browser_post
from core.retry import call_with_retry
from core.mutual_funds import portfolio_parser, portfolio_sources
from core.mutual_funds import persistence as mfdb

logger = logging.getLogger("mf_portfolio_loader")
_NEXT_DATA = re.compile(r'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>', re.S)
_HREF = re.compile(r"""href\s*=\s*["']([^"']+)["']""", re.I)


def find_links(html, base_url, pattern):
    """Absolute URLs of every href in the page whose decoded URL matches the month pattern (de-duplicated, in order)."""
    out = []
    for raw in _HREF.findall(html):
        url = urljoin(base_url, unescape(raw.strip()))
        if pattern.search(unquote(url)) and url not in out:
            out.append(url)
    return out


def _get(url, timeout=(15, 180)):
    def once():
        r = browser_get(url, timeout=timeout)
        if r.status_code != 200:
            raise RuntimeError(f"HTTP {r.status_code} for {url}")
        return r.content
    return call_with_retry(once, on_retry=lambda n, e: logger.warning(f"    [retry {n}] {e}"))


def _dig(obj, path):
    for k in path:
        obj = obj.get(k) if isinstance(obj, dict) else None
        if obj is None:
            return None
    return obj


def find_files(listing, body, month_end, pattern):
    """The month's file URLs from one listing answer (bytes), per listing["parse"]."""
    if listing.get("parse", "hrefs") == "hrefs":
        base = listing.get("base") or listing["url"]
        return find_links(body.decode("utf-8", errors="replace"), base, pattern)
    text = body.decode("utf-8", errors="replace")
    if listing["parse"] == "next_data":
        m = _NEXT_DATA.search(text)
        if not m:
            raise RuntimeError("no __NEXT_DATA__ in the page")
        text = m.group(1)
    data = json.loads(text)
    out = []
    for f in _dig(data, listing["files"]) or []:
        title, url = _dig(f, listing["title"]), _dig(f, listing["url_key"])
        if not (title and url and pattern.search(str(title).strip())):
            continue
        if listing.get("url_prefix") and not str(url).lower().startswith("http"):
            url = listing["url_prefix"] + quote(str(url), safe="/:%")
        if url not in out:
            out.append(url)
    return out


def _read_listing(listing):
    def once():
        if listing.get("method", "GET").upper() == "POST":
            r = browser_post(listing["url"], headers=listing.get("headers"), json=listing.get("json"),
                             data=listing.get("form"), timeout=(15, 60))
        else:
            r = browser_get(listing["url"], headers=listing.get("headers"), timeout=(15, 60))
        if r.status_code != 200:
            raise RuntimeError(f"HTTP {r.status_code} for {listing['url']}")
        return r.content
    return call_with_retry(once, on_retry=lambda n, e: logger.warning(f"    [retry {n}] {e}"))


def fetch_listing(source, month_end):
    """The month's file URLs of one fund house: its listing, then its fallback listing (if any) when the first one
    fails or has no file for the month. Raises when no listing could be read."""
    pattern = portfolio_sources.link_pattern(source, month_end)
    listing = portfolio_sources.fill(source["listing"], month_end)
    error = None
    while listing:
        try:
            links = find_files(listing, _read_listing(listing), month_end, pattern)
            if links or not listing.get("fallback"):
                return links
        except Exception as e:  # noqa: BLE001
            error = e
            if not listing.get("fallback"):
                break
            logger.warning(f"    listing failed ({e}); trying the fallback listing")
        listing = listing.get("fallback")
    if error:
        raise error
    return []


def load_month(conn_factory, month_end, sources=None):
    """
    conn_factory() -> a new DB connection (closed here). Returns {amc: {"files", "loaded", "unchanged", "failed",
    "schemes", "holdings", "error"}}.
    """
    summary = {}
    for src in sources or portfolio_sources.SOURCES:
        amc = src["amc"]
        s = summary.setdefault(amc, {"files": 0, "loaded": 0, "unchanged": 0, "failed": 0, "schemes": 0, "holdings": 0, "error": None})
        try:
            links = fetch_listing(src, month_end)
        except Exception as e:  # noqa: BLE001
            s["error"] = f"listing page: {e}"
            logger.error(f"  {amc}: could not read the file listing -- {e}")
            continue
        s["files"] = len(links)
        if not links:
            s["error"] = "no file for this month in the listing yet"
            logger.info(f"  {amc}: no {month_end:%b-%Y} file in the listing yet.")
            continue
        for url in links:
            try:
                content = _get(url)
                sha = hashlib.sha256(content).hexdigest()
                conn = conn_factory()
                try:
                    if mfdb.portfolio_file_unchanged(conn, amc, month_end, url, sha):
                        s["unchanged"] += 1
                        continue
                    schemes = portfolio_parser.parse_any(content, url)
                    n_sch, n_hold = mfdb.save_portfolio_file(conn, amc, month_end, url, sha, schemes)
                finally:
                    conn.close()
                s["loaded"] += 1
                s["schemes"] += n_sch
                s["holdings"] += n_hold
            except Exception as e:  # noqa: BLE001 -- recorded; the next run retries this file
                s["failed"] += 1
                s["error"] = str(e)[:300]
                logger.error(f"  {amc}: {unquote(url.rsplit('/', 1)[-1])} failed -- {e}")
                try:
                    conn = conn_factory()
                    try:
                        mfdb.record_portfolio_failure(conn, amc, month_end, url, str(e))
                    finally:
                        conn.close()
                except Exception:  # noqa: BLE001
                    pass
        logger.info(f"  {amc}: {s['files']} file(s) -- {s['loaded']} loaded, {s['unchanged']} unchanged, {s['failed']} failed; "
                    f"{s['schemes']} schemes, {s['holdings']} equity holdings.")
    return summary
