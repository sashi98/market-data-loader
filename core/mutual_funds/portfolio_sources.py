# core/mutual_funds/portfolio_sources.py -- TMT-MDL-US-0008
#
# Where each fund house publishes its monthly portfolio, and which files of that listing are the month's. One entry
# per fund house; adding a fund house = adding an entry here (the generic reader in portfolio_parser handles the SEBI
# layout) plus a test against a saved file. `amc` is the fund house name exactly as in AMFI's NAV file (mf_amc).
#
# listing -- how to get the month's file list (every string value is a template, see FIELDS):
#   {"url": ..., "method": "GET" | "POST", "json": {...} | "form": {...}, "parse": "hrefs" | "json_files"}
#   parse "hrefs":      the answer is HTML; every href is a candidate (matched on its URL-decoded URL).
#   parse "json_files": the answer is JSON; `files` is the key path to the list, `title` / `url_key` the key paths of
#                       one file's name and download URL (matched on the title).
#   parse "next_data":  the answer is a Next.js page; its embedded __NEXT_DATA__ JSON is read like "json_files".
#   "headers": extra request headers; "fallback": a second listing tried when the first fails or finds no file;
#   "url_prefix": put before a relative file URL of a JSON listing (spaces etc. are %-encoded).
# A file can be an .xlsx (whatever its name), a .zip of workbooks, or an old .xls -- portfolio_parser.parse_any.
# link -- a regex (str.format template: write regex braces doubled, e.g. \d{{1,2}}) every candidate must match.
# FIELDS: {month}="october", {Month}="October", {mon}="oct", {year}="2026", {yy}="26", {mm}="10", {m}="10" (no
#         leading zero).
#
# Checked from Sashikant's browser on 28-Nov-2026 (Sep- and Oct-2026 files):
#   SBI  -- the portfolios page builds its file table with a script: POST JSON {FundId, PSYear, PSMonth} to
#           /ajaxcall/CMS/GetSchemePortfolioSheets returns that table as HTML (no login, no cookie). One .xlsx for all
#           schemes; its URL ends in "?sfvrsn=..." (CMS version), so the pattern allows a query string.
#   HDFC -- the page carries only the latest month; any month comes from cms.hdfcfund.com's disclosures API
#           (form POST year / type=monthly / month) as JSON. One .xlsx per scheme; some file URLs end in "_0.xlsx",
#           so the match is on the file title. The API answered Python 403 without the browser's Origin / Referer
#           headers (28-Nov-2026), so they are sent; if it still refuses, the page's own data (latest month only) is
#           the fallback.
#   Nippon -- one workbook for all schemes ("NIMF-MONTHLY-PORTFOLIO-31-Oct-26.xls" -- an .xlsx inside despite the
#           name; some months spell the month out, "31-July-26", so the month is matched as "jul[a-z]*"), linked in the downloads page's own HTML.
#   ICICI Prudential -- one .zip a month ("Monthly-Portfolio-Disclosure-October-2026.zip", ~26 MB), listed by the
#           downloads API apimf.icicipruamc.com/nms/v1/downloads/files (POST JSON, category "Monthly Portfolio
#           Disclosures"; no login, but the site's own headers sourceUrl=DOWNLOADS / env=api and Accept:
#           application/json are needed -- without Accept it answers its HTML page (probe 28-Nov-2026,
#           tests/probe_icici_listing_READONLY.py). File URLs are relative to https://www.icicipruamc.com/blob.

SOURCES = [
    {
        "amc": "SBI Mutual Fund",
        "listing": {
            "url": "https://www.sbimf.com/ajaxcall/CMS/GetSchemePortfolioSheets",
            "method": "POST",
            "json": {"FundId": "", "PSYear": "{year}", "PSMonth": "{Month}"},
            "parse": "hrefs",
            "base": "https://www.sbimf.com/portfolios",
        },
        "link": r"all-schemes-monthly-portfolio---as-on-\d{{1,2}}(st|nd|rd|th)?-{month}-{year}\.xlsx(\?.*)?$",
    },
    {
        "amc": "HDFC Mutual Fund",
        "listing": {
            "url": "https://cms.hdfcfund.com/en/hdfc/api/v2/disclosures/monthfortportfolio",
            "method": "POST",
            "form": {"year": "{year}", "type": "monthly", "month": "{m}"},
            "headers": {"Origin": "https://www.hdfcfund.com",
                        "Referer": "https://www.hdfcfund.com/statutory-disclosure/portfolio/monthly-portfolio",
                        "Accept": "application/json, text/plain, */*"},
            "parse": "json_files",
            "files": ["data", "files"],
            "title": ["title"],
            "url_key": ["file", "url"],
            "fallback": {
                "url": "https://www.hdfcfund.com/statutory-disclosure/portfolio/monthly-portfolio",
                "method": "GET",
                "parse": "next_data",
                "files": ["props", "pageProps", "portfolioDataResponse", "data", "files"],
                "title": ["title"],
                "url_key": ["file", "url"],
            },
        },
        "link": r"^Monthly .+ - \d{{1,2}} {Month} {year}\.xlsx$",
    },
    {
        "amc": "Nippon India Mutual Fund",
        "listing": {
            "url": "https://mf.nipponindiaim.com/investor-service/downloads/factsheet-portfolio-and-other-disclosures",
            "method": "GET",
            "parse": "hrefs",
        },
        "link": r"/NIMF-MONTHLY-PORTFOLIO-\d{{1,2}}-{mon}[a-z]*-{yy}\.xlsx?$",
    },
    {
        "amc": "ICICI Prudential Mutual Fund",
        "listing": {
            "url": "https://apimf.icicipruamc.com/nms/v1/downloads/files",
            "method": "POST",
            "headers": {"sourceUrl": "DOWNLOADS", "env": "api", "Accept": "application/json, text/plain, */*",
                        "Content-Type": "application/json", "Origin": "https://www.icicipruamc.com",
                        "Referer": "https://www.icicipruamc.com/"},
            "json": {"categoryId": "26a073d7-08d2-4a95-95fa-f83a4ee51e40", "schemeCategory": "", "userType": "Investor",
                     "fileType": "All", "page": "1", "size": "50", "filter": [], "categoryName": "OTHERS"},
            "parse": "json_files",
            "files": ["success", "data", "files"],
            "title": ["title", "text"],
            "url_key": ["url"],
            "url_prefix": "https://www.icicipruamc.com/blob",
        },
        "link": r"^Monthly Portfolio Disclosure {Month} {year}$",
    },
]


def fields(month_end):
    return {"month": month_end.strftime("%B").lower(), "Month": month_end.strftime("%B"),
            "mon": month_end.strftime("%b").lower(), "year": str(month_end.year), "yy": f"{month_end.year % 100:02d}", "mm": f"{month_end.month:02d}",
            "m": str(month_end.month)}


def fill(template, month_end):
    """Fills every string of a template (str / dict / list) with the month's fields."""
    f = fields(month_end)
    if isinstance(template, str):
        return template.format(**f)
    if isinstance(template, dict):
        return {k: fill(v, month_end) for k, v in template.items()}
    if isinstance(template, list):
        return [fill(v, month_end) for v in template]
    return template


def link_pattern(source, month_end):
    import re
    return re.compile(source["link"].format(**fields(month_end)), re.I)
