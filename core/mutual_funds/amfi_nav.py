# core/mutual_funds/amfi_nav.py -- TMT-MDL-US-0007
#
# Parses AMFI's daily "NAVAll.txt" -- every scheme option of every Indian mutual fund with its latest NAV -- the
# "bhav copy" of mutual funds. Layout (checked 28-Nov-2026):
#
#   Scheme Code;ISIN Div Payout/ ISIN Growth;ISIN Div Reinvestment;Scheme Name;Plan;Option;Net Asset Value;Date
#   <blank>
#   Open Ended Schemes(Equity Scheme - Large Cap Fund)          <- category line
#   <blank>
#   Axis Mutual Fund                                            <- fund house (AMC) line
#   <blank>
#   120465;INF846K01DP8;-;Axis Large Cap Fund - Direct Plan - Growth;Direct Plan;Growth;61.2300;27-Nov-2026
#
# Plan / Option can be blank (then taken from the scheme name); NAV can be 0, "N.A." or years old for wound-up /
# segregated portfolios. Pure functions only -- no DB, no network -- so it is unit-tested (tests/test_amfi_nav.py).

import re
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal, InvalidOperation

AMFI_NAV_URL = "https://portal.amfiindia.com/spages/NAVAll.txt"
# NAV history for a date range (checked 28-Nov-2026 from Sashikant's browser): same layout, other column order --
#   Scheme Code;NAV Name;Plan;Option;ISIN Div Payout/ISIN Growth;ISIN Div Reinvestment;Net Asset Value;Date
# one line per scheme per date; category lines "Open Ended Schemes ( Equity Scheme - Multi Cap Fund )".
AMFI_NAV_HISTORY_URL = "https://portal.amfiindia.com/DownloadNAVHistoryReport_Po.aspx?frmdt={frm}&todt={to}"
HEADER_PREFIX = "Scheme Code;"
STALE_DAYS = 30

_CATEGORY = re.compile(r"^(?P<type>[A-Za-z ]*Schemes?)\s*\((?P<cat>.*)\)\s*$")
_ISIN = re.compile(r"^IN[A-Z0-9]{10}$")


@dataclass
class SchemeRow:
    scheme_code: int
    amc: str
    scheme_name: str
    scheme_type: str
    category: str
    asset_class: str
    plan: str
    option_name: str
    isin_growth: str
    isin_reinvest: str
    nav: Decimal
    nav_date: date


@dataclass
class ParseResult:
    rows: list
    skipped: list          # (line_no, reason, text) for lines that could not be read
    latest_nav_date: date   # most common recent NAV date in the file (the file's "as of" date)


def asset_class_of(category):
    """EQUITY / DEBT / HYBRID / SOLUTION / INDEX_ETF / FOF / OTHER from AMFI's category text."""
    c = (category or "").lower()
    if c.startswith("equity scheme") or "elss" in c:
        return "EQUITY"
    if c.startswith("debt scheme") or c.startswith("income") or "gilt" in c or "liquid" in c:
        return "DEBT"
    if c.startswith("hybrid scheme") or "balanced" in c or "arbitrage" in c:
        return "HYBRID"
    if c.startswith("solution oriented") or "children" in c or "retirement" in c or "life cycle" in c:
        return "SOLUTION"
    if "index" in c or "etf" in c:
        return "INDEX_ETF"
    if "fof" in c or "fund of fund" in c:
        return "FOF"
    return "OTHER"


def plan_of(plan_col, name):
    p = (plan_col or "").strip().lower()
    n = (name or "").lower()
    if "direct" in p or (not p and re.search(r"\bdirect\b", n)):
        return "DIRECT"
    if "regular" in p or (not p and re.search(r"\bregular\b", n)):
        return "REGULAR"
    return None


def option_of(option_col, name):
    o = (option_col or "").strip()
    if o:
        return o[:60]
    n = (name or "").lower()
    if "growth" in n:
        return "Growth"
    if "idcw" in n or "dividend" in n:
        return "IDCW"
    if "bonus" in n:
        return "Bonus"
    return None


def _isin(v):
    v = (v or "").strip().upper()
    return v if _ISIN.match(v) else None


def _nav(v):
    try:
        d = Decimal((v or "").strip())
    except (InvalidOperation, ValueError):
        return None
    return d if d > 0 else None


def _date(v):
    try:
        return datetime.strptime((v or "").strip(), "%d-%b-%Y").date()
    except ValueError:
        return None


# column positions per file layout (field name -> index), from the header line
_DAILY = {"code": 0, "isin_g": 1, "isin_r": 2, "name": 3, "plan": 4, "option": 5, "nav": 6, "date": 7}
_HISTORY = {"code": 0, "name": 1, "plan": 2, "option": 3, "isin_g": 4, "isin_r": 5, "nav": 6, "date": 7}


def _layout(header):
    """Column map from the header: NAVAll.txt has the ISINs before the name, the history file after the option."""
    cols = [c.strip().lower() for c in header.split(";")]
    return _HISTORY if len(cols) > 1 and cols[1].startswith("nav name") else _DAILY


def parse(text, history=False):
    """
    Parses NAVAll.txt (history=False: one row per scheme) or a NAV history file (history=True: one row per scheme per
    date; duplicates are (code, date)). Never raises on a bad line: it is listed in skipped and parsing goes on.
    """
    rows, skipped = [], []
    scheme_type, category, amc = None, None, None
    seen = set()
    cols = _DAILY
    for no, raw in enumerate(text.splitlines(), start=1):
        line = raw.strip().lstrip("\ufeff")
        if not line:
            continue
        if line.startswith(HEADER_PREFIX):
            cols = _layout(line)
            continue
        if ";" not in line:
            m = _CATEGORY.match(line)
            if m:
                scheme_type, category = m.group("type").strip(), m.group("cat").strip()
            else:
                amc = line
            continue
        parts = [p.strip() for p in line.split(";")]
        if len(parts) < 8:
            skipped.append((no, "fewer than 8 fields", line[:200]))
            continue
        code_s, isin_g, isin_r = parts[cols["code"]], parts[cols["isin_g"]], parts[cols["isin_r"]]
        name, plan_col, option_col = parts[cols["name"]], parts[cols["plan"]], parts[cols["option"]]
        nav_s, date_s = parts[cols["nav"]], parts[cols["date"]]
        if not code_s.isdigit():
            skipped.append((no, "scheme code not a number", line[:200]))
            continue
        if amc is None:
            skipped.append((no, "no fund house line before the scheme", line[:200]))
            continue
        code = int(code_s)
        key = (code, date_s) if history else code
        if key in seen:
            skipped.append((no, "duplicate scheme code", line[:200]))
            continue
        seen.add(key)
        rows.append(SchemeRow(
            scheme_code=code, amc=amc, scheme_name=name[:300], scheme_type=scheme_type, category=category,
            asset_class=asset_class_of(category), plan=plan_of(plan_col, name), option_name=option_of(option_col, name),
            isin_growth=_isin(isin_g), isin_reinvest=_isin(isin_r), nav=_nav(nav_s), nav_date=_date(date_s)))
    return ParseResult(rows=rows, skipped=skipped, latest_nav_date=file_date(rows))


def file_date(rows):
    """The file's NAV date: the latest date that at least 10% of the schemes carry (ignores a few odd future dates)."""
    counts = {}
    for r in rows:
        if r.nav_date:
            counts[r.nav_date] = counts.get(r.nav_date, 0) + 1
    if not counts:
        return None
    floor = max(1, len(rows) // 10)
    common = [d for d, n in counts.items() if n >= floor]
    return max(common) if common else max(counts)


def is_active(row, latest_nav_date):
    """Active = has a NAV dated within STALE_DAYS of the file's date."""
    if row.nav is None or row.nav_date is None or latest_nav_date is None:
        return False
    return (latest_nav_date - row.nav_date).days <= STALE_DAYS
