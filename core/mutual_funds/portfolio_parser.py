# core/mutual_funds/portfolio_parser.py -- TMT-MDL-US-0008
#
# Reads a fund house's monthly portfolio disclosure (Excel) into schemes and their EQUITY holdings. SEBI prescribes
# the columns, so one generic reader works across fund houses; it finds, per sheet:
#   - the header row: a cell "ISIN" + a "Quantity" cell + a "Market / Fair Value" cell (+ "% to NAV / AUM / net assets");
#   - the scheme name above it: the cell after "SCHEME NAME", else the first title line (SBI: one sheet per scheme with
#     "SCHEME NAME : ..."; HDFC: one file per scheme, title "HDFC Flexi Cap Fund (An open ended ...)");
#   - data rows whose ISIN is an Indian equity share (INE.... with security code 01 -- bonds are 07 / 08 etc.).
# Market value is converted to Rs lakh from the header's unit (Lakhs / Lacs, Crores, else rupees). The same ISIN twice
# in a scheme (e.g. two listing segments) is summed. Checked on SBI and HDFC Oct-2026 files (28-Nov-2026).
# Nippon (28-Nov-2026): row 1 starts with a scheme code ("RLMF001") before the name, so the title is the first cell
# that reads like a name (has a space); and "% to NAV" is a fraction (0.032 = 3.2%) -- when no % value of the table
# is above 1 AND all % values of the table (holdings + total rows) add up to 0.9..10 (a percent-style table adds up
# to 100+), the scheme's percentages are scaled by 100.

import re
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation

EQUITY_ISIN = re.compile(r"^INE[A-Z0-9]{4}01[A-Z0-9]{3}$")
_SCHEME_LABEL = re.compile(r"^\s*scheme\s*name\s*:?\s*$", re.I)
_SKIP_TITLE = re.compile(r"portfolio (statement )?as on|back to index|mutual fund$|^\d+$|monthly portfolio", re.I)
_TRIM_NAME = re.compile(r"\s*\((an?\s+open[\s-]ended|an?\s+close[\s-]ended|erstwhile|formerly)[^)]*\)?.*$", re.I)


@dataclass
class Holding:
    isin: str
    name: str
    industry: str
    quantity: Decimal
    market_value_lakhs: Decimal
    pct_of_aum: Decimal


@dataclass
class SchemePortfolio:
    scheme_name: str
    scheme_type: str
    sheet: str
    holdings: list = field(default_factory=list)

    @property
    def pct_total(self):
        return sum((h.pct_of_aum or Decimal(0)) for h in self.holdings)


def scheme_type_of(name):
    """ARBITRAGE / INDEX_ETF / HYBRID / ACTIVE from the scheme name (index and arbitrage holdings are not a fund
    manager's view on the stock -- the MF Holding tab and the research prompt keep them apart)."""
    n = (name or "").lower()
    if "arbitrage" in n:
        return "ARBITRAGE"
    if re.search(r"\betf\b|\bindex\b|\bnifty\b|\bsensex\b|\bbse\s*\d|\bfund of fund|\bfof\b|exchange traded", n):
        return "INDEX_ETF"
    if re.search(r"hybrid|balanced|equity savings|multi[\s-]?asset|asset allocation|dynamic asset|retirement|children|"
                 r"conservative|advantage", n):
        return "HYBRID"
    return "ACTIVE"


def clean_scheme_name(raw):
    s = re.sub(r"\s+", " ", str(raw or "")).strip()
    s = _TRIM_NAME.sub("", s).strip(" -:")
    return s[:300]


def _num(v):
    if v is None or v == "":
        return None
    if isinstance(v, (int, float, Decimal)):
        return Decimal(str(v))
    s = str(v).replace(",", "").replace("%", "").strip()
    if s in ("", "-", "NIL", "N/A", "$$", "#"):
        return None
    try:
        return Decimal(s)
    except InvalidOperation:
        return None


def _header_map(cells):
    """Column index per field from a header row, or None when the row is not a holdings header."""
    m = {}
    for i, c in enumerate(cells):
        if not isinstance(c, str):
            continue
        t = re.sub(r"\s+", " ", c).strip().lower()
        if t == "isin" or t.startswith("isin"):
            m.setdefault("isin", i)
        elif "quantity" in t:
            m.setdefault("qty", i)
        elif ("market" in t or "fair value" in t) and "value" in t:
            m.setdefault("mv", i)
            m.setdefault("mv_unit", t)
        elif t.startswith("%") or "% to" in t or "% of" in t or "to net assets" in t or "to nav" in t or "to aum" in t:
            m.setdefault("pct", i)
        elif "name of" in t or "instrument" in t or "issuer" in t or t == "company":
            m.setdefault("name", i)
        elif "industry" in t or "rating" in t or "sector" in t:
            m.setdefault("industry", i)
    return m if {"isin", "qty", "mv"} <= m.keys() else None


def _unit_factor(header_text):
    t = header_text or ""
    if "crore" in t or re.search(r"\bcr\b", t):
        return Decimal(100)
    if "lakh" in t or "lac" in t:
        return Decimal(1)
    return Decimal("0.00001")  # rupees -> lakh


def parse_rows(sheet_name, rows):
    """One sheet's rows (lists of cell values) -> SchemePortfolio, or None when the sheet has no holdings table."""
    title, scheme, hdr, header_at = None, None, None, None
    by_isin = {}
    max_pct, sum_pct = Decimal(0), Decimal(0)
    for idx, row in enumerate(rows):
        cells = list(row)
        if hdr is None:
            texts = [c for c in cells if isinstance(c, str) and c.strip()]
            for j, c in enumerate(cells):
                if isinstance(c, str) and _SCHEME_LABEL.match(c):
                    nxt = [x for x in cells[j + 1:] if isinstance(x, str) and x.strip()]
                    if nxt:
                        scheme = nxt[0]
                if isinstance(c, str) and c.strip().lower().startswith("scheme name") and ":" in c and c.split(":", 1)[1].strip():
                    scheme = c.split(":", 1)[1]
            if title is None:
                named = [x for x in texts if " " in x.strip()]
                if named and not _SKIP_TITLE.search(named[0]) and not _SCHEME_LABEL.match(named[0]):
                    title = named[0]
            m = _header_map(cells)
            if m:
                hdr, header_at = m, idx
            continue
        if "pct" in hdr and hdr["pct"] < len(cells):
            any_pct = _num(cells[hdr["pct"]])
            if any_pct is not None:
                max_pct = max(max_pct, abs(any_pct))
                sum_pct += abs(any_pct)
        isin = cells[hdr["isin"]] if hdr["isin"] < len(cells) else None
        isin = isin.strip().upper() if isinstance(isin, str) else None
        if not isin or not EQUITY_ISIN.match(isin):
            continue
        qty = _num(cells[hdr["qty"]]) if hdr["qty"] < len(cells) else None
        mv = _num(cells[hdr["mv"]]) if hdr["mv"] < len(cells) else None
        pct = _num(cells[hdr["pct"]]) if "pct" in hdr and hdr["pct"] < len(cells) else None
        if qty is None and mv is None:
            continue
        mv_l = (mv * _unit_factor(hdr["mv_unit"])) if mv is not None else None
        name = cells[hdr["name"]] if "name" in hdr and hdr["name"] < len(cells) else None
        ind = cells[hdr["industry"]] if "industry" in hdr and hdr["industry"] < len(cells) else None
        h = by_isin.get(isin)
        if h is None:
            by_isin[isin] = Holding(isin, str(name or "").strip()[:300], str(ind or "").strip()[:150], qty or Decimal(0),
                                    mv_l or Decimal(0), pct or Decimal(0))
        else:
            h.quantity += qty or 0
            h.market_value_lakhs += mv_l or 0
            h.pct_of_aum += pct or 0
    if hdr is None:
        return None
    if by_isin and Decimal(0) < max_pct <= Decimal("1.0001") and Decimal("0.9") <= sum_pct <= Decimal(10):   # fraction-style "% to NAV" (Nippon): 0.032 -> 3.2
        for h in by_isin.values():
            h.pct_of_aum = (h.pct_of_aum * 100).quantize(Decimal("0.0001"))
    name = clean_scheme_name(scheme or title or sheet_name)
    if not name:
        return None
    return SchemePortfolio(name, scheme_type_of(name), sheet_name, list(by_isin.values()))


def parse_xls(content):
    """Every sheet of an old binary .xls (BIFF) -> list of SchemePortfolio (needs xlrd)."""
    import xlrd
    book = xlrd.open_workbook(file_contents=content)
    out = []
    for sh in book.sheets():
        sp = parse_rows(sh.name, (sh.row_values(r) for r in range(sh.nrows)))
        if sp is not None:
            out.append(sp)
    return out


def parse_any(content, name=""):
    """
    One downloaded file (bytes) -> list of SchemePortfolio, whatever the fund house packed it as (TMT-MDL-US-0008):
      - .xlsx (a zip with [Content_Types].xml), even when named .xls (Nippon);
      - a .zip of workbooks (ICICI) -- every .xlsx / .xls / .xlsm inside, nested zips included;
      - an old binary .xls (OLE, "D0 CF 11 E0").
    Scheme names repeated across members are kept once (the first one).
    """
    import io
    import zipfile
    if content[:4] == b"PK\x03\x04":
        with zipfile.ZipFile(io.BytesIO(content)) as z:
            names = z.namelist()
            if "[Content_Types].xml" in names:
                return parse_workbook(io.BytesIO(content))
            out, seen = [], set()
            for n in sorted(names):
                if n.endswith("/") or not re.search(r"\.(xlsx|xlsm|xls|zip)$", n, re.I):
                    continue
                for sp in parse_any(z.read(n), n):
                    if sp.scheme_name not in seen:
                        seen.add(sp.scheme_name)
                        out.append(sp)
            return out
    if content[:4] == b"\xd0\xcf\x11\xe0":
        return parse_xls(content)
    raise ValueError(f"not an Excel workbook or zip: {name or 'download'} starts with {content[:8]!r}")


def parse_workbook(path_or_file):
    """Every sheet of an .xlsx -> list of SchemePortfolio (sheets without a holdings table are skipped)."""
    import openpyxl
    wb = openpyxl.load_workbook(path_or_file, read_only=True, data_only=True)
    out = []
    try:
        for ws in wb.worksheets:
            sp = parse_rows(ws.title, ws.iter_rows(values_only=True))
            if sp is not None:
                out.append(sp)
    finally:
        wb.close()
    return out
