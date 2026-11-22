# core/financials/quarterly_results.py
#
# TMT-US-0030 -- reported quarterly results and half-yearly balance sheets per company, so the research prompt
# (tmt's PromptService) can hand the AI REAL figures instead of letting it look them up (TMT-US-0029-008 found the
# AI's own figures wrong and different on every run).
#
# Source: yfinance (Yahoo Finance) -- checked 22-Nov-2026 against SWSOLAR's filings: every quarter matched
# (Q3 FY26 PAT -2.77, Q4 FY26 134.57 Rs cr). It returns only ~5 quarters, sometimes with a gap, and balance sheets
# half-yearly, so:
#   - a refresh NEVER overwrites a stored value with NULL (older quarters stay, TMT builds its own history);
#   - missing quarters stay missing (the prompt shows them empty, the AI lists them under data_gaps).
#
# One module, two callers (same pattern as corporate actions):
#   - stock_universe_update_listener.py, after the weekly enrichment: refresh_due(...) for every company whose latest
#     quarter is due (TMT-US-0030-004);
#   - stock-py-services POST /api/stock/<symbol>/financials/refresh, called by tmt's Copy prompt when the stock's
#     latest quarter is due (TMT-US-0030-003 / -005). stock-py-services writes the DB (Sashikant, 22-Nov-2026).
#
# Keyed by ISIN only: results belong to the company, not to an exchange listing. The NSE listing is preferred as
# the yfinance ticker (SYMBOL.NS), else BSE (SECURITY_CODE.BO) -- same tickers as the enrichment's yfinance tier.

import logging
import time
from datetime import date, datetime, timedelta

log = logging.getLogger(__name__)

SOURCE = "yfinance"
CRORE = 10_000_000
# Results are filed within 45 days of quarter end (60 for March); from this many days after a quarter end the
# quarter counts as "due" -- a company may have filed. Matches tmt's PromptService.RESULTS_DUE_AFTER_DAYS.
DUE_AFTER_DAYS = 14
# Same per-request delay as the enrichment's yfinance tier (core/stock_universe/yfinance_client.py).
RATE_LIMIT_DELAY_SECONDS = 1.5

# yfinance row names, first one present wins.
_INCOME_FIELDS = {
    "revenue": ["Total Revenue", "Operating Revenue"],
    "ebitda": ["EBITDA", "Normalized EBITDA"],
    "pbt": ["Pretax Income"],
    "pat": ["Net Income Including Noncontrolling Interests", "Net Income"],
    "pat_attributable": ["Net Income Common Stockholders", "Net Income"],
    "eps_diluted": ["Diluted EPS", "Basic EPS"],
}
_BALANCE_FIELDS = {
    "total_debt": ["Total Debt"],
    "cash_and_st_investments": ["Cash Cash Equivalents And Short Term Investments", "Cash And Cash Equivalents"],
    "stockholders_equity": ["Stockholders Equity", "Common Stock Equity"],
    "minority_interest": ["Minority Interest"],
}
_NOT_CRORE = {"eps_diluted"}


class FinancialsError(Exception):
    """Raised when results can't be fetched or stored; callers log it and carry on (alert, never block)."""


# ── Pure helpers (unit-tested) ────────────────────────────────────────────────

def fiscal_label(period_end):
    """Indian fiscal quarter of a quarter-end date: 2025-12-31 -> 'Q3 FY26', 2026-03-31 -> 'Q4 FY26'."""
    fy = period_end.year + 1 if period_end.month >= 4 else period_end.year
    q = {6: 1, 9: 2, 12: 3, 3: 4}.get(period_end.month)
    return f"Q{q} FY{fy % 100:02d}" if q else None


def last_quarter_end(on_or_before):
    """The latest calendar quarter end (Mar 31 / Jun 30 / Sep 30 / Dec 31) on or before the given date."""
    d = on_or_before
    for m, day in ((12, 31), (9, 30), (6, 30), (3, 31)):
        qe = date(d.year, m, day)
        if qe <= d:
            return qe
    return date(d.year - 1, 12, 31)


def due_quarter_end(today, due_after_days=DUE_AFTER_DAYS):
    """The latest quarter end whose results may be out by today: the last quarter end at least due_after_days ago."""
    return last_quarter_end(today - timedelta(days=due_after_days))


def is_due(latest_stored_period_end, today, due_after_days=DUE_AFTER_DAYS):
    """True when newer results than the latest stored quarter may have been filed (or nothing is stored yet)."""
    return latest_stored_period_end is None or latest_stored_period_end < due_quarter_end(today, due_after_days)


def _value(df, column, names, divide):
    """First non-NaN value among the row names for this column; Rs -> Rs crore unless divide is False."""
    import pandas as pd
    for name in names:
        if name in df.index:
            v = df.at[name, column]
            if v is not None and not pd.isna(v):
                v = float(v)
                return round(v / CRORE, 2) if divide else round(v, 2)
    return None


def rows_from_frame(df, fields):
    """yfinance statement (index = line items, columns = period end dates) -> [{period_end: date, field: value}], newest first."""
    if df is None or getattr(df, "empty", True):
        return []
    out = []
    for column in df.columns:
        period_end = column.date() if hasattr(column, "date") else date.fromisoformat(str(column)[:10])
        row = {"period_end": period_end}
        for field, names in fields.items():
            row[field] = _value(df, column, names, field not in _NOT_CRORE)
        if any(v is not None for k, v in row.items() if k != "period_end"):
            out.append(row)
    out.sort(key=lambda r: r["period_end"], reverse=True)
    return out


def yf_ticker(symbol, exchange, security_code):
    """SYMBOL.NS for NSE-family listings, SECURITY_CODE.BO for BSE-family (as core/stock_universe/yfinance_client)."""
    if (exchange or "").upper().startswith("BSE"):
        return f"{security_code}.BO" if security_code else None
    return f"{symbol}.NS" if symbol else None


# ── Fetch (network) ───────────────────────────────────────────────────────────

def fetch_from_yfinance(ticker_symbol):
    """(quarters, balance_sheets) for one yfinance ticker, newest first. Raises FinancialsError on failure."""
    import yfinance as yf
    try:
        t = yf.Ticker(ticker_symbol)
        income = t.quarterly_income_stmt
        balance = t.quarterly_balance_sheet
    except Exception as e:  # yfinance raises many kinds -- one failure never stops the caller
        raise FinancialsError(f"yfinance {ticker_symbol}: {e}") from e
    return rows_from_frame(income, _INCOME_FIELDS), rows_from_frame(balance, _BALANCE_FIELDS)


# ── DB ────────────────────────────────────────────────────────────────────────

_UPSERT_QUARTER = """
    INSERT INTO stock_quarterly_result
        (isin_number, symbol, period_end, fiscal_label, revenue, ebitda, pbt, pat, pat_attributable, eps_diluted,
         source, source_ticker, fetched_at, updated_at)
    VALUES (%(isin)s, %(symbol)s, %(period_end)s, %(fiscal_label)s, %(revenue)s, %(ebitda)s, %(pbt)s, %(pat)s,
            %(pat_attributable)s, %(eps_diluted)s, %(source)s, %(ticker)s, %(now)s, %(now)s)
    ON CONFLICT (isin_number, period_end) DO UPDATE SET
        symbol           = COALESCE(EXCLUDED.symbol, stock_quarterly_result.symbol),
        fiscal_label     = COALESCE(EXCLUDED.fiscal_label, stock_quarterly_result.fiscal_label),
        revenue          = COALESCE(EXCLUDED.revenue, stock_quarterly_result.revenue),
        ebitda           = COALESCE(EXCLUDED.ebitda, stock_quarterly_result.ebitda),
        pbt              = COALESCE(EXCLUDED.pbt, stock_quarterly_result.pbt),
        pat              = COALESCE(EXCLUDED.pat, stock_quarterly_result.pat),
        pat_attributable = COALESCE(EXCLUDED.pat_attributable, stock_quarterly_result.pat_attributable),
        eps_diluted      = COALESCE(EXCLUDED.eps_diluted, stock_quarterly_result.eps_diluted),
        source           = EXCLUDED.source,
        source_ticker    = EXCLUDED.source_ticker,
        fetched_at       = EXCLUDED.fetched_at,
        updated_at       = EXCLUDED.updated_at
    RETURNING (xmax = 0) AS inserted
"""

_UPSERT_BALANCE = """
    INSERT INTO stock_balance_sheet
        (isin_number, period_end, total_debt, cash_and_st_investments, stockholders_equity, minority_interest,
         source, fetched_at, updated_at)
    VALUES (%(isin)s, %(period_end)s, %(total_debt)s, %(cash_and_st_investments)s, %(stockholders_equity)s,
            %(minority_interest)s, %(source)s, %(now)s, %(now)s)
    ON CONFLICT (isin_number, period_end) DO UPDATE SET
        total_debt              = COALESCE(EXCLUDED.total_debt, stock_balance_sheet.total_debt),
        cash_and_st_investments = COALESCE(EXCLUDED.cash_and_st_investments, stock_balance_sheet.cash_and_st_investments),
        stockholders_equity     = COALESCE(EXCLUDED.stockholders_equity, stock_balance_sheet.stockholders_equity),
        minority_interest       = COALESCE(EXCLUDED.minority_interest, stock_balance_sheet.minority_interest),
        source                  = EXCLUDED.source,
        fetched_at              = EXCLUDED.fetched_at,
        updated_at              = EXCLUDED.updated_at
"""

_RECORD_ATTEMPT = """
    INSERT INTO stock_financials_refresh (isin_number, last_attempt_at, last_success_at, last_status, last_message, trigger_source)
    VALUES (%(isin)s, %(now)s, CASE WHEN %(ok)s THEN %(now)s END, %(status)s, %(message)s, %(trigger)s)
    ON CONFLICT (isin_number) DO UPDATE SET
        last_attempt_at = EXCLUDED.last_attempt_at,
        last_success_at = COALESCE(EXCLUDED.last_success_at, stock_financials_refresh.last_success_at),
        last_status     = EXCLUDED.last_status,
        last_message    = EXCLUDED.last_message,
        trigger_source  = EXCLUDED.trigger_source
"""


def save(conn, isin, symbol, ticker, quarters, balance_sheets, now=None):
    """Upserts the fetched rows (never a value over with NULL). Returns the number of NEW quarters. Caller commits."""
    now = now or datetime.now()
    new_quarters = 0
    with conn.cursor() as cur:
        for q in quarters:
            cur.execute(_UPSERT_QUARTER, {**q, "isin": isin, "symbol": symbol, "ticker": ticker, "source": SOURCE,
                                          "fiscal_label": fiscal_label(q["period_end"]), "now": now})
            row = cur.fetchone()
            if row and row[0]:
                new_quarters += 1
        for b in balance_sheets:
            cur.execute(_UPSERT_BALANCE, {**b, "isin": isin, "source": SOURCE, "now": now})
    return new_quarters


def record_attempt(conn, isin, status, message, trigger, now=None):
    now = now or datetime.now()
    with conn.cursor() as cur:
        cur.execute(_RECORD_ATTEMPT, {"isin": isin, "now": now, "ok": status == "OK", "status": status,
                                      "message": (message or "")[:500] or None, "trigger": trigger})


def load_stored(conn, isin, quarters=8):
    """The stored results for one company: {quarters: [...newest first], balanceSheet: {...} or None, lastAttemptAt}."""
    with conn.cursor() as cur:
        cur.execute("""SELECT period_end, fiscal_label, revenue, ebitda, pbt, pat, pat_attributable, eps_diluted, source
                         FROM stock_quarterly_result WHERE isin_number = %s
                        ORDER BY period_end DESC LIMIT %s""", (isin, quarters))
        cols = ["periodEnd", "fiscalLabel", "revenue", "ebitda", "pbt", "pat", "patAttributable", "epsDiluted", "source"]
        qs = [dict(zip(cols, r)) for r in cur.fetchall()]
        cur.execute("""SELECT period_end, total_debt, cash_and_st_investments, stockholders_equity, minority_interest
                         FROM stock_balance_sheet WHERE isin_number = %s ORDER BY period_end DESC LIMIT 1""", (isin,))
        b = cur.fetchone()
        cur.execute("SELECT last_attempt_at FROM stock_financials_refresh WHERE isin_number = %s", (isin,))
        a = cur.fetchone()
    bs = None if b is None else dict(zip(["periodEnd", "totalDebt", "cashAndStInvestments", "stockholdersEquity",
                                          "minorityInterest"], b))
    return {"quarters": qs, "balanceSheet": bs, "lastAttemptAt": a[0] if a else None}


def resolve_listing(conn, symbol, exchange=None):
    """(isin, symbol, exchange, security_code) for a symbol: the given exchange's listing, else NSE-family, else any."""
    with conn.cursor() as cur:
        cur.execute("""SELECT isin_number, symbol, exchange, security_code FROM stock_universe
                        WHERE upper(symbol) = upper(%s)
                        ORDER BY CASE WHEN %s IS NOT NULL AND split_part(exchange, ' ', 1) = upper(%s) THEN 0 ELSE 1 END,
                                 CASE WHEN exchange LIKE 'NSE%%' THEN 0 ELSE 1 END
                        LIMIT 1""", (symbol, exchange, exchange))
        return cur.fetchone()


def refresh_one(conn, isin, symbol, exchange, security_code, trigger):
    """
    Fetches and stores one company's results, in its own transaction. Never raises: returns
    {refreshed, newQuarters, ticker, error}. A failure is recorded in stock_financials_refresh.
    """
    ticker = yf_ticker(symbol, exchange, security_code)
    if ticker is None:
        result = {"refreshed": False, "newQuarters": 0, "ticker": None, "error": "no yfinance ticker for this listing"}
    else:
        try:
            quarters, balances = fetch_from_yfinance(ticker)
            if not quarters and not balances:
                raise FinancialsError(f"yfinance {ticker}: no quarterly statements returned")
            new = save(conn, isin, symbol, ticker, quarters, balances)
            conn.commit()
            result = {"refreshed": True, "newQuarters": new, "ticker": ticker, "error": None}
        except Exception as e:
            conn.rollback()
            result = {"refreshed": False, "newQuarters": 0, "ticker": ticker, "error": str(e)}
    try:
        record_attempt(conn, isin, "OK" if result["refreshed"] else "FAILED", result["error"], trigger)
        conn.commit()
    except Exception as e:
        conn.rollback()
        log.warning("Financials refresh %s: could not record the attempt: %s", isin, e)
    return result


# ── Weekly enrichment (TMT-US-0030-004) ───────────────────────────────────────

def select_due(conn, today=None):
    """
    One listing per company (NSE-family preferred) whose latest stored quarter is older than the due quarter end
    (or that has none), skipping companies already attempted today. [(isin, symbol, exchange, security_code)].
    """
    today = today or date.today()
    due = due_quarter_end(today)
    with conn.cursor() as cur:
        cur.execute("""
            WITH listing AS (
                SELECT DISTINCT ON (su.isin_number) su.isin_number, su.symbol, su.exchange, su.security_code
                  FROM stock_universe su
                 ORDER BY su.isin_number, CASE WHEN su.exchange LIKE 'NSE%%' THEN 0 ELSE 1 END, su.exchange
            ), latest AS (
                SELECT isin_number, MAX(period_end) AS latest FROM stock_quarterly_result GROUP BY isin_number
            )
            SELECT l.isin_number, l.symbol, l.exchange, l.security_code
              FROM listing l
              LEFT JOIN latest q ON q.isin_number = l.isin_number
              LEFT JOIN stock_financials_refresh r ON r.isin_number = l.isin_number
             WHERE (q.latest IS NULL OR q.latest < %s)
               AND (r.last_attempt_at IS NULL OR r.last_attempt_at::date < %s)
             ORDER BY l.isin_number
        """, (due, today))
        return cur.fetchall()


def refresh_due(get_conn, limit=None, delay_seconds=RATE_LIMIT_DELAY_SECONDS, sleep=time.sleep, today=None):
    """
    Refreshes every due company, one at a time (Yahoo rate-limits), each in its own transaction, so one failure or
    an interrupted run loses nothing -- whatever is still due is picked up next week or by Copy prompt.
    get_conn() opens a DB connection. Returns {checked, refreshed, newQuarters, failed}.
    """
    conn = get_conn()
    try:
        due = select_due(conn, today)
        if limit is not None:
            due = due[:limit]
        summary = {"checked": len(due), "refreshed": 0, "newQuarters": 0, "failed": 0}
        log.info("  Financial results: %d compan%s due for a refresh.", len(due), "y" if len(due) == 1 else "ies")
        for i, (isin, symbol, exchange, security_code) in enumerate(due, 1):
            r = refresh_one(conn, isin, symbol, exchange, security_code, trigger="ENRICHMENT")
            if r["refreshed"]:
                summary["refreshed"] += 1
                summary["newQuarters"] += r["newQuarters"]
            else:
                summary["failed"] += 1
                log.info("    [results failed] %s %s (%s) -- %s", isin, symbol, r["ticker"], r["error"])
            if i % 100 == 0:
                log.info("    ... %d / %d companies done", i, len(due))
            sleep(delay_seconds)
        log.info("  Financial results: %(checked)d checked, %(refreshed)d refreshed, %(newQuarters)d new quarter(s), "
                 "%(failed)d failed.", summary)
        return summary
    finally:
        conn.close()
