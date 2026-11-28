# tests/test_mf_portfolio.py -- TMT-MDL-US-0008
# Generic portfolio reader (SBI- and HDFC-style layouts), scheme types, unit conversion, month link patterns.
# The SBI Oct-2026 file is used when present (data/mf_samples, not in git). Run: python tests/test_mf_portfolio.py

import sys
from datetime import date
from decimal import Decimal
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from core.mutual_funds import portfolio_parser as pp  # noqa: E402
from core.mutual_funds import portfolio_sources as ps  # noqa: E402
from core.mutual_funds.portfolio_loader import find_links  # noqa: E402

ADANI = "INE423A01024"

SBI_STYLE = [
    [None, "SBI Mutual Fund", "007", "Back to Index"],
    [None, "SCHEME NAME :", "SBI Aggressive Hybrid Fund (Erstwhile known as SBI Equity Hybrid Fund)"],
    [None, "PORTFOLIO STATEMENT AS ON :", "2026-10-31"],
    [None, "Name of the Instrument / Issuer", "ISIN", "Rating / Industry^", "Quantity", "Market value\n(Rs. in Lakhs)", "% to AUM"],
    [None, "EQUITY & EQUITY RELATED"],
    ["100418", "Adani Enterprises Ltd.", ADANI, "Metals & Minerals Trading", 7460874, 192445.78, 2.24],
    ["100001", "Some Bond 7.5% 2030", "INE001A07TM4", "CRISIL AAA", 500, 5100.0, 0.06],
    [None, "Total", None, None, None, 999999, 100],
]

HDFC_STYLE = [
    ["HDFC Flexi Cap Fund (An open ended equity scheme investing across large cap, mid cap & small cap stocks)"] * 3,
    ["Portfolio as on 31-Oct-2026"] * 3,
    ["ISIN", "Coupon (%)", "Name Of the Instrument", "Industry+ /Rating", "Quantity", "Market/ Fair Value (Rs. in Lacs.)", "% to NAV", "Yield"],
    ["EQUITY & EQUITY RELATED"],
    [ADANI, None, "ADANI ENTERPRISES LIMTIED", "Metals & Minerals Trading", "2029136", "52339.53", "0.47"],
    [ADANI, None, "ADANI ENTERPRISES LIMTIED", "Metals & Minerals Trading", "1000", "25.80", "0.01"],
]


NIPPON_STYLE = [
    ["RLMF001", "Nippon India Growth Mid Cap Fund (An open-ended equity scheme predominantly investing in mid cap stocks)", None, None, None, None, None, "Index"],
    [None, "Monthly Portfolio Statement as on October 31,2026"],
    [],
    [None, "ISIN", "Name of the Instrument", "Industry / Rating", "Quantity", "Market/Fair Value\n( Rs. in Lacs)", "% to NAV", "YIELD"],
    [None, None, "Equity & Equity related"],
    ["ADAN02", ADANI, "Adani Enterprises Limited", "Metals & Minerals Trading", 116291, 2999.61, 0.0082],
    ["MCEX02", "INE745G01043", "Multi Commodity Exchange of India Limited", "Capital Markets", 4901000, 156283.09, 0.032],
    [None, None, "Grand Total", None, None, 4883845.5, 1],
]


def test_nippon_style_sheet_code_title_and_fraction_pct():
    sp = pp.parse_rows("GF", NIPPON_STYLE)
    assert sp.scheme_name == "Nippon India Growth Mid Cap Fund", sp.scheme_name
    by = {h.isin: h for h in sp.holdings}
    assert by[ADANI].pct_of_aum == Decimal("0.82") and by["INE745G01043"].pct_of_aum == Decimal("3.2"), by
    assert by[ADANI].quantity == Decimal(116291) and by[ADANI].market_value_lakhs == Decimal("2999.61")


def test_sbi_style_sheet():
    sp = pp.parse_rows("SEHF", SBI_STYLE)
    assert sp.scheme_name == "SBI Aggressive Hybrid Fund", sp.scheme_name
    assert sp.scheme_type == "HYBRID"
    assert [h.isin for h in sp.holdings] == [ADANI]            # the bond (security code 07) is not equity
    h = sp.holdings[0]
    assert h.quantity == Decimal(7460874) and h.market_value_lakhs == Decimal("192445.78") and h.pct_of_aum == Decimal("2.24")


def test_hdfc_style_sheet_sums_duplicate_isin():
    sp = pp.parse_rows("HDFCEQ", HDFC_STYLE)
    assert sp.scheme_name == "HDFC Flexi Cap Fund", sp.scheme_name
    assert sp.scheme_type == "ACTIVE"
    h = sp.holdings[0]
    assert h.quantity == Decimal(2030136) and h.market_value_lakhs == Decimal("52365.33") and h.pct_of_aum == Decimal("0.48")


def test_units_and_types():
    assert pp._unit_factor("market value (rs. in crores)") == 100
    assert pp._unit_factor("market value (rs.)") == Decimal("0.00001")
    assert pp.scheme_type_of("Kotak Arbitrage Fund") == "ARBITRAGE"
    assert pp.scheme_type_of("SBI Nifty 50 ETF") == "INDEX_ETF"
    assert pp.scheme_type_of("UTI Nifty 50 Index Fund") == "INDEX_ETF"
    assert pp.scheme_type_of("SBI Equity Savings Fund") == "HYBRID"
    assert pp.scheme_type_of("Quant Small Cap Fund") == "ACTIVE"
    assert pp.parse_rows("Index", [["Index"], ["Scheme", "Sheet"]]) is None


def test_month_links():
    from core.mutual_funds.portfolio_loader import find_files
    oct26 = date(2026, 10, 31)
    # SBI: the HTML table its ajax call returns (28-Nov-2026), URLs carry "?sfvrsn=..."
    sbi = ps.SOURCES[0]
    listing = ps.fill(sbi["listing"], oct26)
    assert listing["json"] == {"FundId": "", "PSYear": "2026", "PSMonth": "October"}
    html = ('<a href="/docs/default-source/scheme-portfolios/all-schemes-monthly-portfolio---as-on-31st-october-2026.xlsx?sfvrsn=95fe7a7e_2">x</a>'
            '<a href="/docs/default-source/scheme-portfolios/all-schemes-monthly-portfolio---as-on-31st-october-2026.xlsx?sfvrsn=95fe7a7e_2">x</a>'
            '<a href="/docs/default-source/scheme-portfolios/all-schemes-monthly-portfolio---as-on-30th-september-2026.xlsx?sfvrsn=2a79126c_2">y</a>')
    assert find_files(listing, html.encode(), oct26, ps.link_pattern(sbi, oct26)) == [
        "https://www.sbimf.com/docs/default-source/scheme-portfolios/all-schemes-monthly-portfolio---as-on-31st-october-2026.xlsx?sfvrsn=95fe7a7e_2"]
    # HDFC: the disclosures API's JSON (28-Nov-2026); Sep-2026 files can end in "_0.xlsx", the title does not
    hdfc = ps.SOURCES[1]
    sep26 = date(2026, 9, 30)
    listing = ps.fill(hdfc["listing"], sep26)
    assert listing["form"] == {"year": "2026", "type": "monthly", "month": "9"}
    body = ('{"data":{"files":['
            '{"file_id":"1","title":"Monthly HDFC Retirement Fund - Equity Plan - 30 September 2026.xlsx",'
            '"file":{"filename":"a","url":"https://files.hdfcfund.com/s3fs-public/2026-10/Monthly%20HDFC%20Retirement%20Fund%20-%20Equity%20Plan%20-%2030%20September%202026_0.xlsx"}},'
            '{"file_id":"2","title":"Monthly HDFC Value Fund - 31 October 2026.xlsx",'
            '"file":{"filename":"b","url":"https://files.hdfcfund.com/s3fs-public/2026-11/Monthly%20HDFC%20Value%20Fund%20-%2031%20October%202026.xlsx"}}'
            ']},"code":200,"message":"success"}')
    got = find_files(listing, body.encode(), sep26, ps.link_pattern(hdfc, sep26))
    assert len(got) == 1 and got[0].endswith("2030%20September%202026_0.xlsx"), got
    # HDFC fallback: the page's __NEXT_DATA__ (latest month only)
    page = ('<html><script id="__NEXT_DATA__" type="application/json">{"props":{"pageProps":{"portfolioDataResponse":'
            + body.replace('{"data":', '{"data":', 1) + '}}}</script></html>')
    got = find_files(listing["fallback"], page.encode(), oct26, ps.link_pattern(hdfc, oct26))
    assert len(got) == 1 and got[0].endswith("2031%20October%202026.xlsx"), got


def test_nippon_month_link():
    nip = next(s for s in ps.SOURCES if s["amc"] == "Nippon India Mutual Fund")
    base = "https://mf.nipponindiaim.com/InvestorServices/FactsheetsDocuments/"
    html = "".join(f'<a href="{base}{f}">D</a>' for f in [
        "NIMF-MONTHLY-PORTFOLIO-31-Oct-26.xls", "NIMF-FORTNIGHTLY-PORTFOLIO-31-Oct-26.xls", "NIMF-MONTHLY-PORTFOLIO-30-Sep-26.xls",
        "NIMF-MONTHLY-PORTFOLIO-31-July-26.xls", "NIMF-MONTHLY-PORTFOLIO-31-Oct-25.xls"])
    assert find_links(html, nip["listing"]["url"], ps.link_pattern(nip, date(2026, 10, 31))) == [base + "NIMF-MONTHLY-PORTFOLIO-31-Oct-26.xls"]
    assert find_links(html, nip["listing"]["url"], ps.link_pattern(nip, date(2026, 7, 31))) == [base + "NIMF-MONTHLY-PORTFOLIO-31-July-26.xls"]


def test_icici_listing_relative_url():
    from core.mutual_funds.portfolio_loader import find_files
    ic = next(s for s in ps.SOURCES if s["amc"] == "ICICI Prudential Mutual Fund")
    oct26 = date(2026, 10, 31)
    body = ('{"success":{"data":{"files":['
            '{"title":{"code":"x","text":"Monthly Portfolio Disclosure October 2026"},'
            '"url":"/downloads/Files/Monthly Portfolio Disclosures/2026/Oct/Monthly-Portfolio-Disclosure-October-2026.zip"},'
            '{"title":{"code":"y","text":"Monthly Portfolio Disclosure October 2025"},"url":"/downloads/x.zip"}],"isNext":true}}}')
    got = find_files(ps.fill(ic["listing"], oct26), body.encode(), oct26, ps.link_pattern(ic, oct26))
    assert got == ["https://www.icicipruamc.com/blob/downloads/Files/Monthly%20Portfolio%20Disclosures/2026/Oct/"
                   "Monthly-Portfolio-Disclosure-October-2026.zip"], got


def _xlsx_bytes(rows):
    import io
    import openpyxl
    wb = openpyxl.Workbook()
    ws = wb.active
    for r in rows:
        ws.append(r)
    b = io.BytesIO()
    wb.save(b)
    return b.getvalue()


def test_parse_any_xlsx_zip_and_rejects_other():
    import io
    import zipfile
    one = _xlsx_bytes(NIPPON_STYLE)
    assert [s.scheme_name for s in pp.parse_any(one, "NIMF.xls")] == ["Nippon India Growth Mid Cap Fund"]
    z = io.BytesIO()
    with zipfile.ZipFile(z, "w") as zf:
        zf.writestr("Oct-2026/Growth.xlsx", one)
        zf.writestr("Oct-2026/Hybrid.xlsx", _xlsx_bytes(SBI_STYLE))
        zf.writestr("Oct-2026/readme.txt", "x")
    names = sorted(s.scheme_name for s in pp.parse_any(z.getvalue(), "icici.zip"))
    assert names == ["Nippon India Growth Mid Cap Fund", "SBI Aggressive Hybrid Fund"], names
    try:
        pp.parse_any(b"<!DOCTYPE html>", "x.zip")
        assert False, "html must be rejected"
    except ValueError:
        pass


def test_fallback_listing_used_when_first_fails():
    from core.mutual_funds import portfolio_loader as pl
    calls = []

    def fake_read(listing):
        calls.append(listing["url"])
        if listing.get("method") == "POST":
            raise RuntimeError("HTTP 403")
        return ('<script id="__NEXT_DATA__">{"props":{"pageProps":{"portfolioDataResponse":{"data":{"files":['
                '{"title":"Monthly HDFC Value Fund - 31 October 2026.xlsx","file":{"url":"https://f/x.xlsx"}}]}}}}}</script>').encode()
    orig_read, orig_retry = pl._read_listing, pl.call_with_retry
    pl._read_listing = fake_read
    try:
        assert pl.fetch_listing(ps.SOURCES[1], date(2026, 10, 31)) == ["https://f/x.xlsx"]
        assert len(calls) == 2
    finally:
        pl._read_listing, pl.call_with_retry = orig_read, orig_retry


def test_real_sbi_file_when_present():
    f = Path(__file__).resolve().parents[3] / "data" / "mf_samples" / "SBI_2026-10.xlsx"
    if not f.exists():
        print("  (SBI sample file not present -- skipped)")
        return
    schemes = pp.parse_workbook(f)
    holders = {s.scheme_name: h for s in schemes for h in s.holdings if h.isin == ADANI}
    assert len(holders) == 23, len(holders)
    assert holders["SBI Nifty 50 ETF"].quantity == 6586404
    assert round(holders["SBI Aggressive Hybrid Fund"].market_value_lakhs / 100, 2) == Decimal("1924.46")


if __name__ == "__main__":
    fails = 0
    for name, fn in list(globals().items()):
        if name.startswith("test_") and callable(fn):
            try:
                fn()
                print(f"PASS {name}")
            except AssertionError as e:
                fails += 1
                print(f"FAIL {name}: {e}")
    print("all passed" if not fails else f"{fails} failed")
    sys.exit(1 if fails else 0)
