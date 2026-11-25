# tests/test_financial_results.py -- TMT-US-0030: core/financials/quarterly_results.py
#
# Pure helpers always run. The DB tests run against a throw-away Postgres only when TMT_TEST_PG_DSN is set
# (e.g. "host=/tmp/pg port=55432 dbname=tmt_test user=tmt"); they create their own tables, never touch TMT's DB.
#
# Fixture figures: SWSOLAR's real consolidated results as yfinance returned them on 22-Nov-2026 (Rs, not crore).

import os
from datetime import date, datetime

import pandas as pd
import pytest

from core.financials import quarterly_results as qr


def _income_frame():
    cols = [pd.Timestamp("2026-09-30"), pd.Timestamp("2026-06-30"), pd.Timestamp("2026-03-31"),
            pd.Timestamp("2025-12-31"), pd.Timestamp("2025-06-30")]
    return pd.DataFrame({
        cols[0]: {"Total Revenue": 15275000000, "EBITDA": 703200000, "Pretax Income": 300000000,
                  "Net Income Common Stockholders": 192300000, "Net Income": 192300000, "Diluted EPS": 0.82},
        cols[1]: {"Total Revenue": 15901300000, "EBITDA": 988800000, "Pretax Income": 700000000,
                  "Net Income Common Stockholders": 542200000, "Net Income": 542200000, "Diluted EPS": 2.32},
        cols[2]: {"Total Revenue": 19456100000, "EBITDA": 1986900000, "Pretax Income": 1541100000,
                  "Net Income Common Stockholders": 1345700000, "Net Income": 1345700000, "Diluted EPS": 5.76},
        cols[3]: {"Total Revenue": 20922100000, "EBITDA": 546900000, "Pretax Income": float("nan"),
                  "Net Income Common Stockholders": -27700000, "Net Income": -27700000, "Diluted EPS": -0.12},
        cols[4]: {"Total Revenue": 17616300000, "EBITDA": 1065700000, "Pretax Income": 450000000,
                  "Net Income Common Stockholders": 319700000, "Net Income": 319700000, "Diluted EPS": 1.37},
    })


def _balance_frame():
    return pd.DataFrame({
        pd.Timestamp("2026-09-30"): {"Total Debt": 9433300000, "Cash Cash Equivalents And Short Term Investments": 12245700000,
                                     "Stockholders Equity": 7227200000, "Minority Interest": -37700000},
    })


# ── Pure helpers ─────────────────────────────────────────────────────────────

def test_fiscal_label_indian_quarters():
    assert qr.fiscal_label(date(2025, 12, 31)) == "Q3 FY26"
    assert qr.fiscal_label(date(2026, 3, 31)) == "Q4 FY26"
    assert qr.fiscal_label(date(2026, 6, 30)) == "Q1 FY27"
    assert qr.fiscal_label(date(2026, 9, 30)) == "Q2 FY27"
    assert qr.fiscal_label(date(2026, 8, 15)) is None


def test_due_quarter_end_waits_14_days_after_quarter_end():
    assert qr.due_quarter_end(date(2026, 11, 22)) == date(2026, 9, 30)
    assert qr.due_quarter_end(date(2026, 10, 13)) == date(2026, 6, 30), "13 days after Sep 30: not yet due"
    assert qr.due_quarter_end(date(2026, 10, 14)) == date(2026, 9, 30)
    assert qr.due_quarter_end(date(2027, 1, 5)) == date(2026, 9, 30)


def test_is_due():
    today = date(2026, 11, 22)
    assert qr.is_due(None, today)
    assert qr.is_due(date(2026, 6, 30), today)
    assert not qr.is_due(date(2026, 9, 30), today)


def test_rows_from_frame_crore_newest_first_nan_is_none():
    rows = qr.rows_from_frame(_income_frame(), qr._INCOME_FIELDS)
    assert [r["period_end"] for r in rows] == [date(2026, 9, 30), date(2026, 6, 30), date(2026, 3, 31),
                                               date(2025, 12, 31), date(2025, 6, 30)]
    dec = rows[3]
    assert dec["revenue"] == 2092.21 and dec["pat_attributable"] == -2.77 and dec["eps_diluted"] == -0.12
    assert dec["pbt"] is None, "NaN stays empty"
    assert rows[2]["pat_attributable"] == 134.57
    assert qr.rows_from_frame(pd.DataFrame(), qr._INCOME_FIELDS) == []
    assert qr.rows_from_frame(None, qr._INCOME_FIELDS) == []


def test_yf_ticker():
    assert qr.yf_ticker("SWSOLAR", "NSE", None) == "SWSOLAR.NS"
    assert qr.yf_ticker("ABC", "NSE SME", None) == "ABC.NS"
    assert qr.yf_ticker("ABC", "BSE", "542760") == "542760.BO"
    assert qr.yf_ticker("ABC", "BSE SME", None) is None


# ── DB (throw-away Postgres) ─────────────────────────────────────────────────

def test_is_company_equity_skips_funds_rights_and_partly_paid():
    # TMT-US-0030-011
    assert qr.is_company_equity("INE00M201021") and qr.is_company_equity("INE0PS301014") and qr.is_company_equity("INE002A01018")
    assert not qr.is_company_equity("INF846K01K35"), "mutual fund / ETF"
    assert not qr.is_company_equity("INE754C20028"), "CRESCE-RE1 rights entitlement"
    assert not qr.is_company_equity("IN9155A01020"), "partly paid / DVR line"
    assert not qr.is_company_equity(None) and not qr.is_company_equity("INE00M2010")


def test_no_data_backs_off_30_days_unless_a_new_quarter_becomes_due():
    no_data = (qr.STATUS_NO_DATA, "yfinance 531552.BO: no quarterly statements returned")
    old_failed = (qr.STATUS_FAILED, "yfinance 531552.BO: no quarterly statements returned")   # saved before -011
    network = (qr.STATUS_FAILED, "HTTPSConnectionPool: Read timed out")
    tried = datetime(2026, 11, 23, 2, 0)
    for status, message in (no_data, old_failed):
        assert qr.backed_off(status, message, tried, date(2026, 11, 30)), "1 week later: still waiting"
        assert qr.backed_off(status, message, tried, date(2026, 12, 13)), "3 weeks later: still waiting"
        assert not qr.backed_off(status, message, tried, date(2026, 12, 24)), "30 days later: asked again"
    # Q3 FY27 (Dec-26) becomes due on 14-Jan-2027: a no-data company tried on 10-Jan is asked again on 15-Jan
    assert not qr.backed_off(qr.STATUS_NO_DATA, "x", datetime(2027, 1, 10), date(2027, 1, 15))
    assert not qr.backed_off(*network, tried, date(2026, 11, 30)), "network failures retry next run"
    assert not qr.backed_off(qr.STATUS_NO_DATA, "x", None, date(2026, 11, 30))
    assert qr.is_no_data(qr.STATUS_FAILED, "no yfinance ticker for this listing")


DSN = os.environ.get("TMT_TEST_PG_DSN")
pytestmark_db = pytest.mark.skipif(not DSN, reason="set TMT_TEST_PG_DSN to run the DB tests")

_DDL = """
DROP TABLE IF EXISTS stock_quarterly_result, stock_balance_sheet, stock_financials_refresh, stock_universe;
CREATE TABLE stock_universe (isin_number VARCHAR(12), symbol VARCHAR(50), exchange VARCHAR(20), security_code VARCHAR(20));
CREATE TABLE stock_quarterly_result (id BIGSERIAL PRIMARY KEY, isin_number VARCHAR(12) NOT NULL, symbol VARCHAR(50),
  period_end DATE NOT NULL, fiscal_label VARCHAR(12), revenue NUMERIC(18,2), ebitda NUMERIC(18,2), pbt NUMERIC(18,2),
  pat NUMERIC(18,2), pat_attributable NUMERIC(18,2), eps_diluted NUMERIC(12,2), source VARCHAR(30) NOT NULL,
  source_ticker VARCHAR(40), fetched_at TIMESTAMP NOT NULL, updated_at TIMESTAMP NOT NULL,
  CONSTRAINT uq_stock_quarterly_result_isin_period UNIQUE (isin_number, period_end));
CREATE TABLE stock_balance_sheet (id BIGSERIAL PRIMARY KEY, isin_number VARCHAR(12) NOT NULL, period_end DATE NOT NULL,
  total_debt NUMERIC(18,2), cash_and_st_investments NUMERIC(18,2), stockholders_equity NUMERIC(18,2),
  minority_interest NUMERIC(18,2), source VARCHAR(30) NOT NULL, fetched_at TIMESTAMP NOT NULL, updated_at TIMESTAMP NOT NULL,
  CONSTRAINT uq_stock_balance_sheet_isin_period UNIQUE (isin_number, period_end));
CREATE TABLE stock_financials_refresh (isin_number VARCHAR(12) PRIMARY KEY, last_attempt_at TIMESTAMP NOT NULL,
  last_success_at TIMESTAMP, last_status VARCHAR(20) NOT NULL, last_message VARCHAR(500), trigger_source VARCHAR(20));
INSERT INTO stock_universe VALUES ('INE00M201021','SWSOLAR','NSE',NULL), ('INE00M201021','SWSOLAR','BSE','542760'),
  ('INE0U4701011','DIGITIDE','NSE',NULL), ('INE999Z01011','BSEONLY','BSE','500001');
"""


@pytest.fixture
def conn():
    import psycopg2
    c = psycopg2.connect(DSN)
    with c.cursor() as cur:
        cur.execute(_DDL)
    c.commit()
    yield c
    c.close()


@pytestmark_db
def test_save_inserts_then_never_overwrites_with_null(conn):
    quarters = qr.rows_from_frame(_income_frame(), qr._INCOME_FIELDS)
    balances = qr.rows_from_frame(_balance_frame(), qr._BALANCE_FIELDS)
    assert qr.save(conn, "INE00M201021", "SWSOLAR", "SWSOLAR.NS", quarters, balances) == 5
    conn.commit()
    # a later fetch: only 2 quarters, one value missing -> no new rows, nothing lost
    later = [dict(quarters[0], revenue=None), quarters[1]]
    assert qr.save(conn, "INE00M201021", "SWSOLAR", "SWSOLAR.NS", later, []) == 0
    conn.commit()
    stored = qr.load_stored(conn, "INE00M201021")
    assert len(stored["quarters"]) == 5, "the 3 quarters not in the later fetch are kept"
    assert float(stored["quarters"][0]["revenue"]) == 1527.5, "NULL never overwrites a stored value"
    assert stored["quarters"][0]["fiscalLabel"] == "Q2 FY27"
    assert float(stored["quarters"][3]["patAttributable"]) == -2.77
    assert float(stored["balanceSheet"]["totalDebt"]) == 943.33


@pytestmark_db
def test_refresh_one_records_success_and_failure(conn, monkeypatch):
    monkeypatch.setattr(qr, "fetch_from_yfinance", lambda t: (qr.rows_from_frame(_income_frame(), qr._INCOME_FIELDS), []))
    r = qr.refresh_one(conn, "INE00M201021", "SWSOLAR", "NSE", None, trigger="COPY_PROMPT")
    assert r == {"refreshed": True, "newQuarters": 5, "ticker": "SWSOLAR.NS", "error": None, "status": "OK"}

    def boom(t):
        raise qr.FinancialsError("Yahoo down")
    monkeypatch.setattr(qr, "fetch_from_yfinance", boom)
    r = qr.refresh_one(conn, "INE00M201021", "SWSOLAR", "NSE", None, trigger="COPY_PROMPT")
    assert r["refreshed"] is False and "Yahoo down" in r["error"]
    with conn.cursor() as cur:
        cur.execute("SELECT last_status, last_message, last_success_at IS NOT NULL FROM stock_financials_refresh")
        assert cur.fetchone() == ("FAILED", "Yahoo down", True), "a failure keeps the last success time"
        cur.execute("SELECT count(*) FROM stock_quarterly_result")
        assert cur.fetchone()[0] == 5, "a failed refresh loses nothing"


@pytestmark_db
def test_select_due_one_listing_per_company_nse_first_skips_up_to_date_and_attempted_today(conn):
    today = date(2026, 11, 22)
    due = qr.select_due(conn, today)
    assert [(d[0], d[2]) for d in due] == [("INE00M201021", "NSE"), ("INE0U4701011", "NSE"), ("INE999Z01011", "BSE")]
    quarters = qr.rows_from_frame(_income_frame(), qr._INCOME_FIELDS)        # latest 2026-09-30 = up to date
    qr.save(conn, "INE00M201021", "SWSOLAR", "SWSOLAR.NS", quarters, [])
    qr.record_attempt(conn, "INE0U4701011", "FAILED", "x", "ENRICHMENT", now=datetime(2026, 11, 22, 9, 0))
    conn.commit()
    assert [d[0] for d in qr.select_due(conn, today)] == ["INE999Z01011"]
    assert [d[0] for d in qr.select_due(conn, date(2026, 11, 23))] == ["INE0U4701011", "INE999Z01011"], "retried next day"


@pytestmark_db
def test_refresh_due_one_failure_does_not_stop_the_rest(conn, monkeypatch):
    import psycopg2

    def fetch(ticker):
        if ticker == "DIGITIDE.NS":
            raise qr.FinancialsError("rate limited")
        return qr.rows_from_frame(_income_frame(), qr._INCOME_FIELDS), []
    monkeypatch.setattr(qr, "fetch_from_yfinance", fetch)
    sleeps = []
    summary = qr.refresh_due(lambda: psycopg2.connect(DSN), sleep=sleeps.append, today=date(2026, 11, 22))
    assert summary == {"checked": 3, "refreshed": 2, "newQuarters": 10, "noData": 0, "failed": 1,
                       "skippedNotCompany": 0, "skippedBackedOff": 0}
    assert len(sleeps) == 3, "throttled after every company"


@pytestmark_db
def test_no_data_company_is_backed_off_and_non_company_never_selected(conn, monkeypatch):
    # TMT-US-0030-011
    with conn.cursor() as cur:
        cur.execute("INSERT INTO stock_universe VALUES ('INF846K01K35','BNKETF','NSE',NULL), ('INE754C20028','CRESCE-RE1','BSE','800001')")
    conn.commit()
    monkeypatch.setattr(qr, "fetch_from_yfinance", lambda t: ([], []))
    r = qr.refresh_one(conn, "INE999Z01011", "BSEONLY", "BSE", "500001", trigger="ENRICHMENT")
    assert r["status"] == "NO_DATA"
    qr.record_attempt(conn, "INE999Z01011", "NO_DATA", r["error"], "ENRICHMENT", now=datetime(2026, 11, 25, 9, 0))
    conn.commit()
    due, counts = qr.select_due_with_counts(conn, date(2026, 11, 30))
    assert "INE999Z01011" not in [d[0] for d in due] and counts == {"notCompany": 2, "backedOff": 1}


@pytestmark_db
def test_resolve_listing_prefers_given_exchange_then_nse(conn):
    assert qr.resolve_listing(conn, "swsolar")[2] == "NSE"
    assert qr.resolve_listing(conn, "SWSOLAR", "BSE")[2] == "BSE"
    assert qr.resolve_listing(conn, "NOPE") is None
