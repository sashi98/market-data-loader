# tests/test_amfi_nav.py -- TMT-MDL-US-0007
# AMFI NAVAll.txt parser and the MF scheduler's window rules. No DB, no network.
# Run: python tests/test_amfi_nav.py (or pytest).

import sys
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from core.mutual_funds import amfi_nav as an  # noqa: E402

SAMPLE = """﻿Scheme Code;ISIN Div Payout/ ISIN Growth;ISIN Div Reinvestment;Scheme Name;Plan;Option;Net Asset Value;Date

Open Ended Schemes(Equity Scheme - Large Cap Fund)

Axis Mutual Fund

120465;INF846K01DP8;-;Axis Large Cap Fund - Direct Plan - Growth;Direct Plan;Growth;61.2300;27-Nov-2026
112277;INF846K01131;INF846K01149;Axis Large Cap Fund - Regular Plan - IDCW;;;18.4400;27-Nov-2026

SBI Mutual Fund

119598;INF200K01QX4;-;SBI Large Cap Fund - DIRECT PLAN - Growth;;;101.5512;27-Nov-2026

Open Ended Schemes(Other Scheme - Other  ETFs)

SBI Mutual Fund

135106;INF200KA1FS1;-;SBI Nifty 50 ETF;;;262.4100;27-Nov-2026

Close Ended Schemes(Income)

Kotak Mahindra Mutual Fund

123689;INF174K01KI5;INF174K01KJ3;Kotak FMP Series 99 - Direct Plan;Direct Plan;Annual Dividend;17.1427;15-Sep-2017
130001;INF174K01ZZ9;-;Kotak Segregated Portfolio 1;;;0.0000;27-Nov-2026
130002;bad line
x130003;-;-;Bad code;;;1;27-Nov-2026
120465;INF846K01DP8;-;duplicate code;;;1;27-Nov-2026
"""


def parsed():
    return an.parse(SAMPLE)


def test_rows_amc_and_category():
    r = parsed()
    codes = [x.scheme_code for x in r.rows]
    assert codes == [120465, 112277, 119598, 135106, 123689, 130001], codes
    axis = r.rows[0]
    assert axis.amc == "Axis Mutual Fund"
    assert axis.scheme_type == "Open Ended Schemes"
    assert axis.category == "Equity Scheme - Large Cap Fund"
    assert axis.asset_class == "EQUITY"
    assert axis.plan == "DIRECT" and axis.option_name == "Growth"
    assert axis.nav == Decimal("61.2300") and axis.nav_date == date(2026, 11, 27)
    assert axis.isin_growth == "INF846K01DP8" and axis.isin_reinvest is None
    assert r.rows[3].amc == "SBI Mutual Fund" and r.rows[3].asset_class == "INDEX_ETF"
    assert r.rows[4].scheme_type == "Close Ended Schemes" and r.rows[4].asset_class == "DEBT"


def test_plan_and_option_from_name_when_blank():
    r = parsed()
    assert r.rows[1].plan == "REGULAR" and r.rows[1].option_name == "IDCW"
    assert r.rows[2].plan == "DIRECT" and r.rows[2].option_name == "Growth"


def test_bad_lines_skipped_not_fatal():
    reasons = [s[1] for s in parsed().skipped]
    assert reasons == ["fewer than 8 fields", "scheme code not a number", "duplicate scheme code"], reasons


def test_zero_nav_and_stale_are_inactive():
    r = parsed()
    assert r.latest_nav_date == date(2026, 11, 27)
    by = {x.scheme_code: x for x in r.rows}
    assert by[130001].nav is None and not an.is_active(by[130001], r.latest_nav_date)
    assert not an.is_active(by[123689], r.latest_nav_date)          # 2017 NAV
    assert an.is_active(by[120465], r.latest_nav_date)


def test_asset_classes():
    assert an.asset_class_of("Hybrid Scheme - Arbitrage Fund") == "HYBRID"
    assert an.asset_class_of("Solution Oriented Scheme - Retirement Fund") == "SOLUTION"
    assert an.asset_class_of("Other Scheme - Index Funds") == "INDEX_ETF"
    assert an.asset_class_of("Other Scheme - FoF Overseas") == "FOF"
    assert an.asset_class_of("Equity Scheme - ELSS") == "EQUITY"
    assert an.asset_class_of("Debt Scheme - Liquid Fund") == "DEBT"


def test_scheduler_window_and_expected_date():
    import mf_scheduler_main as m
    ist = timezone(timedelta(hours=5, minutes=30))
    fri_2245 = datetime(2026, 11, 27, 22, 45, tzinfo=ist)
    assert m.in_window(fri_2245) and m.expected_nav_date(fri_2245) == date(2026, 11, 27)
    sat_0100 = datetime(2026, 11, 28, 1, 0, tzinfo=ist)
    assert m.in_window(sat_0100) and m.expected_nav_date(sat_0100) == date(2026, 11, 27)
    sat_2245 = datetime(2026, 11, 28, 22, 45, tzinfo=ist)
    assert m.expected_nav_date(sat_2245) == date(2026, 11, 27)        # weekend -> Friday's NAVs
    noon = datetime(2026, 11, 28, 12, 0, tzinfo=ist)
    assert not m.in_window(noon) and m.seconds_until_window(noon) == 10.5 * 3600


HISTORY = """Scheme Code;NAV Name;Plan;Option;ISIN Div Payout/ISIN Growth;ISIN Div Reinvestment;Net Asset Value;Date

Open Ended Schemes ( Equity Scheme - Multi Cap Fund )

Aditya Birla Sun Life Mutual Fund
148921;Aditya Birla Sun Life Multi-Cap Fund-Direct Growth;Direct Plan;GROWTH;INF209KB1Y49;;23.58;20-Nov-2026
148921;Aditya Birla Sun Life Multi-Cap Fund-Direct Growth;Direct Plan;GROWTH;INF209KB1Y49;;23.71;21-Nov-2026

Motilal Oswal Mutual Fund
152650;Motilal Oswal Multi Cap Fund Regular Plan Growth;;;INF247L01CO2;;15.6521;20-Nov-2026
"""


def test_history_layout_columns_and_dates():
    r = an.parse(HISTORY, history=True)
    assert [(x.scheme_code, str(x.nav), x.nav_date.day) for x in r.rows] == [
        (148921, "23.58", 20), (148921, "23.71", 21), (152650, "15.6521", 20)], r.rows
    first = r.rows[0]
    assert first.scheme_name.startswith("Aditya Birla") and first.plan == "DIRECT" and first.option_name == "GROWTH"
    assert first.isin_growth == "INF209KB1Y49" and first.category == "Equity Scheme - Multi Cap Fund"
    assert r.rows[2].plan == "REGULAR" and r.rows[2].option_name == "Growth"
    assert r.skipped == []


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
