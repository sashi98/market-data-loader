# core/rsi/rsi_incremental.py
#
# Incremental single-date RSI14 update -- Story 2 of the Indicators
# Framework (docs/epics/indicators-framework/indicators-framework-epic-v1.html).
# Given rsi14d_workbook's CURRENT state, computes and upserts exactly ONE
# new trade_date for every isin, rather than the full-history recompute
# rsi14d_loader.py does.
#
# SCOPE, DELIBERATELY NARROW -- this module does NOT do gap-detection or
# multi-day catch-up. Story 3/4's indicators_listener.py already walks
# forward one real trading day at a time and calls run_incremental_update()
# exactly once per date, in order. That's simpler than what the abandoned
# Java RsiDailyUpdateListener tried to do (validate-the-whole-chain-first,
# walk-multiple-days, all inside one listener) -- here, "which date, and
# is it the right next one" is entirely the caller's problem, not this
# module's.
#
# ERROR-HANDLING PHILOSOPHY -- two different severities, deliberately:
#   - A single isin having no prior rsi14d_workbook row, missing
#     close/prev_close for this date, or (should never happen)
#     accumulating more than RSI_PERIOD-1 pre-seed rows: all SOFT skips.
#     Logged, counted, the rest of the date's isins still get processed
#     and written normally. One flaky isin should not block RSI for the
#     entire market.
#   - No bhav_copy data at all for the requested trade_date, or any
#     actual DB error: a REAL failure -- raises RsiPersistenceError.
#     This is what Story 4's catch-up thread is meant to catch and turn
#     into the DEACTIVE + indicators_open_failures path.

import pandas as pd

from core.rsi.rsi_math import RSI_PERIOD, compute_gain_loss, seed, step, compute_rsi14
from core.rsi.rsi_persistence import upsert_rsi14d_workbook, RsiPersistenceError

LATEST_PER_ISIN_SQL = """
    SELECT DISTINCT ON (isin, exchange, series, symbol) isin, exchange, series, symbol, trade_date AS latest_trade_date, avg_gain, avg_loss
      FROM rsi14d_workbook
     ORDER BY isin, exchange, series, symbol, trade_date DESC
"""

PRIOR_HISTORY_SQL = """
    SELECT isin, exchange, series, symbol, gain, loss
      FROM rsi14d_workbook
     WHERE (isin, exchange, series, symbol) IN %(isin_exchange_series_symbol_quads)s
     ORDER BY isin, exchange, series, symbol, trade_date ASC
"""

CLOSES_FOR_DATE_SQL = """
    SELECT bc.isin, bc.exchange, bc.series, bc.symbol, bc.close, bc.prev_close
      FROM bhav_copy bc
     WHERE bc.trade_date = %(trade_date)s
       AND EXISTS (
           SELECT 1 FROM security_series ss
            WHERE ss.exchange = bc.exchange
              AND bc.series LIKE ss.series_code_pattern
              AND ss.category = 'EQUITY'
       )
"""


def _fetch_closes_for_date(conn, trade_date):
    try:
        df = pd.read_sql(CLOSES_FOR_DATE_SQL, conn, params={"trade_date": trade_date})
    except Exception as e:
        raise RsiPersistenceError(f"Failed to fetch bhav_copy closes for {trade_date}: {e}")

    if df.empty:
        raise RsiPersistenceError(
            f"No bhav_copy data at all for trade_date={trade_date} within the RSI series scope -- "
            f"cannot compute an incremental update for a date with zero data."
        )
    return df


def _fetch_latest_per_isin(conn):
    try:
        df = pd.read_sql(LATEST_PER_ISIN_SQL, conn)
    except Exception as e:
        raise RsiPersistenceError(f"Failed to fetch latest rsi14d_workbook row per isin: {e}")
    return df.set_index(["isin", "exchange", "series", "symbol"])


def _fetch_prior_history(conn, isin_exchange_series_symbol_quads):
    """
    isin_exchange_series_symbol_quads: list of (isin, exchange, series,
    symbol) tuples still pre-seed. Returns dict (isin, exchange, series,
    symbol) -> (gains, losses), oldest-to-newest.
    """
    if not isin_exchange_series_symbol_quads:
        return {}
    try:
        df = pd.read_sql(
            PRIOR_HISTORY_SQL, conn,
            params={"isin_exchange_series_symbol_quads": tuple(isin_exchange_series_symbol_quads)},
        )
    except Exception as e:
        raise RsiPersistenceError(f"Failed to fetch prior pre-seed history: {e}")

    history = {quad: ([], []) for quad in isin_exchange_series_symbol_quads}
    for (isin, exchange, series, symbol), group in df.groupby(["isin", "exchange", "series", "symbol"], sort=False):
        history[(isin, exchange, series, symbol)] = (list(group["gain"]), list(group["loss"]))
    return history

def run_incremental_update(conn, trade_date):
    closes_df = _fetch_closes_for_date(conn, trade_date)
    latest_df = _fetch_latest_per_isin(conn)

    still_pre_seed_quads = [
        (row.isin, row.exchange, row.series, row.symbol) for row in closes_df.itertuples(index=False)
        if (row.isin, row.exchange, row.series, row.symbol) in latest_df.index
           and pd.isna(latest_df.loc[(row.isin, row.exchange, row.series, row.symbol), "avg_gain"])
    ]
    prior_history = _fetch_prior_history(conn, still_pre_seed_quads)

    counts = {
        "updated": 0, "seeded": 0, "still_pre_seed": 0,
        "skipped_no_workbook": 0, "skipped_missing_price": 0,
        "skipped_invariant_violation": 0,
    }
    result_rows = []

    for row in closes_df.itertuples(index=False):
        isin, exchange, series, symbol, close, prev_close = (
            row.isin, row.exchange, row.series, row.symbol, row.close, row.prev_close
        )
        key = (isin, exchange, series, symbol)

        if close is None or pd.isna(close) or prev_close is None or pd.isna(prev_close):
            counts["skipped_missing_price"] += 1
            continue

        if key not in latest_df.index:
            counts["skipped_no_workbook"] += 1
            continue

        latest_avg_gain = latest_df.loc[key, "avg_gain"]
        latest_avg_loss = latest_df.loc[key, "avg_loss"]

        if not pd.isna(latest_avg_gain):
            gain, loss = compute_gain_loss(close, prev_close)
            avg_gain, avg_loss = step(latest_avg_gain, latest_avg_loss, gain, loss)
            rsi14 = compute_rsi14(avg_gain, avg_loss)
            counts["updated"] += 1
        else:
            prior_gains, prior_losses = prior_history.get(key, ([], []))
            prior_count = len(prior_gains)

            if prior_count > RSI_PERIOD - 1:
                counts["skipped_invariant_violation"] += 1
                continue

            gain, loss = compute_gain_loss(close, prev_close)

            if prior_count == RSI_PERIOD - 1:
                avg_gain, avg_loss = seed(prior_gains + [gain], prior_losses + [loss])
                rsi14 = compute_rsi14(avg_gain, avg_loss)
                counts["seeded"] += 1
            else:
                avg_gain, avg_loss, rsi14 = None, None, None
                counts["still_pre_seed"] += 1

        result_rows.append({
            "isin": isin, "exchange": exchange, "series": series, "symbol": symbol, "trade_date": trade_date,
            "gain": gain, "loss": loss,
            "avg_gain": avg_gain, "avg_loss": avg_loss, "rsi14": rsi14,
        })

    result_df = pd.DataFrame(result_rows, columns=[
        "isin", "exchange", "series", "symbol", "trade_date", "gain", "loss", "avg_gain", "avg_loss", "rsi14"
    ])
    written = upsert_rsi14d_workbook(conn, result_df) if not result_df.empty else 0

    counts["written"] = written
    return counts


def get_current_max_date(conn):
    """
    Returns rsi14d_workbook's current MAX(trade_date) across all isins,
    or None if the table has no rows at all (Part 1's bulk backfill has
    never been run -- the incremental path has nothing to build on top
    of yet).

    Used ONLY for auto-bootstrapping indicators_workbook_metadata's
    cursor the first time rsi14d runs under the Indicators Framework --
    its IWM row starts out with latest_trade_date NULL (see
    core/indicators/dispatch.py's bootstrap contract and
    indicators_listener.py).
    """
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT MAX(trade_date) FROM rsi14d_workbook")
            row = cur.fetchone()
    except Exception as e:
        raise RsiPersistenceError(f"Failed to fetch rsi14d_workbook's current max date: {e}")
    return row[0] if row else None

