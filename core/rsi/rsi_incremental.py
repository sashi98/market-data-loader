# core/rsi/rsi_incremental.py
#
# Incremental single-date RSI14 update -- Story 2 of the Indicators
# Framework (docs/epics/indicators-framework/indicators-framework-epic-v1.html).
# Given rsi14d_workbook's CURRENT state, computes and upserts exactly ONE
# new trade_date for every isin, rather than the full-history recompute
# rsi14d_loader.py does.
#
# SCOPE -- this module does NOT do multi-day catch-up or decide "which
# date is next". Story 3/4's indicators_listener.py already walks
# forward one real trading day at a time (per bhav_copy_metadata's own
# SUCCESS history) and calls run_incremental_update() exactly once per
# date, in order -- "which date, and is it the right next one" is
# entirely the caller's problem, not this module's.
#
# CONTINUITY FIX (see core/rsi/rsi_continuity.py) -- this module DOES
# now do per-isin GAP detection, which is different from the listener's
# date sequencing above: the listener guarantees it calls this function
# once for every real trading date in order, but a single isin can still
# be absent from a given date's eligible rows (suspension, or simply no
# EQUITY-eligible row that day) while the market as a whole trades
# normally. When such an isin's next eligible row finally appears, this
# module compares its last known eligible trade_date against the shared
# trading calendar and reseeds (restarts the Wilder average from the
# last 14 eligible closes) instead of stepping across the gap as if it
# were one ordinary day -- the same mechanism that was silently
# corrupting MEIL's RSI. An ordinary weekend/holiday is never a gap:
# the comparison is in real trading SESSIONS elapsed, not calendar days.
#
# ERROR-HANDLING PHILOSOPHY -- two different severities, deliberately:
#   - A single isin having no prior rsi14d_workbook row, missing
#     close/prev_close for this date, an unresolvable gap-reseed (fewer
#     than RSI_PERIOD eligible rows available even after a gap), or
#     (should never happen) accumulating more than RSI_PERIOD-1 pre-seed
#     rows: all SOFT skips. Logged, counted, the rest of the date's
#     isins still get processed and written normally. One flaky isin
#     should not block RSI for the entire market.
#   - No bhav_copy data at all for the requested trade_date, or any
#     actual DB error: a REAL failure -- raises RsiPersistenceError.
#     This is what Story 4's catch-up thread is meant to catch and turn
#     into the DEACTIVE + indicators_open_failures path.

import pandas as pd

from core.rsi.rsi_math import RSI_PERIOD, compute_gain_loss, seed, step, compute_rsi14
from core.rsi.rsi_persistence import upsert_rsi14d_workbook, RsiPersistenceError
from core.rsi.rsi_continuity import (
    ELIGIBLE_SERIES_EXISTS_SQL, MIN_LIQUIDITY_FILTER_SQL, TIEBREAK_RANK_SQL,
    fetch_trading_calendar, build_calendar_index, is_gap,
    fetch_recent_eligible_closes, RsiContinuityError,
)
from core.corporate_actions.adjustment import ADJUSTMENT_FACTOR_JOIN_SQL

LATEST_PER_ISIN_SQL = """
    SELECT DISTINCT ON (isin, exchange) isin, exchange, series, symbol, trade_date AS latest_trade_date, avg_gain, avg_loss
      FROM rsi14d_workbook
     ORDER BY isin, exchange, trade_date DESC
"""

PRIOR_HISTORY_SQL = """
    SELECT isin, exchange, gain, loss
      FROM rsi14d_workbook
     WHERE (isin, exchange) IN %(isin_exchange_pairs)s
     ORDER BY isin, exchange, trade_date ASC
"""

# close/prev_close are multiplied by the isin's cumulative corporate-
# actions adjustment factor as of trade_date (see
# core/corporate_actions/adjustment.py) -- COALESCE to 1 for isins with
# no MATCHED action, which is the overwhelming majority and a pure
# no-op multiply for them. bhav_copy itself is never modified; this is
# a read-time adjustment only. Eligibility filter + same-day tiebreak
# (core/rsi/rsi_continuity.py) guarantee exactly one row per
# (isin, exchange) for this trade_date -- e.g. a BL block-deal row or a
# BSE '#'-remark-flag duplicate never reaches the walk.
CLOSES_FOR_DATE_SQL = f"""
    WITH ranked AS (
        SELECT bc.isin, bc.exchange, bc.series, bc.symbol,
               bc.close * COALESCE(adj.factor, 1) AS close,
               bc.prev_close * COALESCE(adj.factor, 1) AS prev_close,
               {TIEBREAK_RANK_SQL}
          FROM bhav_copy bc
          {ADJUSTMENT_FACTOR_JOIN_SQL}
         WHERE bc.trade_date = %(trade_date)s
           AND {ELIGIBLE_SERIES_EXISTS_SQL}
           AND {MIN_LIQUIDITY_FILTER_SQL}
    )
    SELECT isin, exchange, series, symbol, close, prev_close
      FROM ranked
     WHERE continuity_rank = 1
"""


def _fetch_closes_for_date(conn, trade_date):
    try:
        df = pd.read_sql(CLOSES_FOR_DATE_SQL, conn, params={"trade_date": trade_date})
    except Exception as e:
        raise RsiPersistenceError(f"Failed to fetch bhav_copy closes for {trade_date}: {e}")

    if df.empty:
        raise RsiPersistenceError(
            f"No bhav_copy data at all for trade_date={trade_date} within the RSI continuity scope -- "
            f"cannot compute an incremental update for a date with zero data."
        )
    return df


def _fetch_latest_per_isin(conn):
    try:
        df = pd.read_sql(LATEST_PER_ISIN_SQL, conn)
    except Exception as e:
        raise RsiPersistenceError(f"Failed to fetch latest rsi14d_workbook row per isin: {e}")
    return df.set_index(["isin", "exchange"])


def _fetch_prior_history(conn, isin_exchange_pairs):
    """
    isin_exchange_pairs: list of (isin, exchange) tuples still pre-seed.
    Returns dict (isin, exchange) -> (gains, losses), oldest-to-newest.

    KNOWN SIMPLIFICATION: this accumulates every prior row regardless of
    whether a gap occurred mid-accumulation (an isin that hasn't reached
    its first full seed yet is a much rarer case than a gap on an
    already-seeded isin, the case the reseed path below handles). Not
    gap-aware during initial accumulation.
    """
    if not isin_exchange_pairs:
        return {}
    try:
        df = pd.read_sql(
            PRIOR_HISTORY_SQL, conn,
            params={"isin_exchange_pairs": tuple(isin_exchange_pairs)},
        )
    except Exception as e:
        raise RsiPersistenceError(f"Failed to fetch prior pre-seed history: {e}")

    history = {pair: ([], []) for pair in isin_exchange_pairs}
    for (isin, exchange), group in df.groupby(["isin", "exchange"], sort=False):
        history[(isin, exchange)] = (list(group["gain"]), list(group["loss"]))
    return history


def _reseed_from_gap(conn, isin, exchange, trade_date):
    """
    Restarts the Wilder average from scratch using the last RSI_PERIOD
    eligible closes up to and including trade_date, instead of stepping
    across a detected gap. Returns (gain, loss, avg_gain, avg_loss,
    rsi14) for trade_date, or None if fewer than RSI_PERIOD eligible
    rows are available yet (falls back to pre-seed treatment by the caller).
    """
    window_df = fetch_recent_eligible_closes(conn, isin, exchange, trade_date, RSI_PERIOD)
    if len(window_df) < RSI_PERIOD:
        return None

    gains, losses = [], []
    for row in window_df.itertuples(index=False):
        g, l = compute_gain_loss(row.close, row.prev_close)
        if g is None:
            return None  # a null prev_close inside the window -- can't seed cleanly, stay pre-seed
        gains.append(g)
        losses.append(l)

    avg_gain, avg_loss = seed(gains, losses)
    rsi14 = compute_rsi14(avg_gain, avg_loss)
    return gains[-1], losses[-1], avg_gain, avg_loss, rsi14


def run_incremental_update(conn, trade_date):
    closes_df = _fetch_closes_for_date(conn, trade_date)
    latest_df = _fetch_latest_per_isin(conn)

    try:
        calendar_dates = fetch_trading_calendar(conn, end_date=trade_date)
        calendar_index = build_calendar_index(calendar_dates)
    except RsiContinuityError as e:
        raise RsiPersistenceError(f"Failed to build trading calendar for gap detection: {e}")

    still_pre_seed_pairs = [
        (row.isin, row.exchange) for row in closes_df.itertuples(index=False)
        if (row.isin, row.exchange) in latest_df.index
           and pd.isna(latest_df.loc[(row.isin, row.exchange), "avg_gain"])
    ]
    prior_history = _fetch_prior_history(conn, still_pre_seed_pairs)

    counts = {
        "updated": 0, "seeded": 0, "still_pre_seed": 0, "reseeded_after_gap": 0,
        "skipped_no_workbook": 0, "skipped_missing_price": 0,
        "skipped_invariant_violation": 0, "skipped_reseed_insufficient_history": 0,
    }
    result_rows = []

    for row in closes_df.itertuples(index=False):
        isin, exchange, series, symbol, close, prev_close = (
            row.isin, row.exchange, row.series, row.symbol, row.close, row.prev_close
        )
        key = (isin, exchange)

        if close is None or pd.isna(close) or prev_close is None or pd.isna(prev_close):
            counts["skipped_missing_price"] += 1
            continue

        if key not in latest_df.index:
            counts["skipped_no_workbook"] += 1
            continue

        latest_avg_gain = latest_df.loc[key, "avg_gain"]
        latest_avg_loss = latest_df.loc[key, "avg_loss"]
        latest_trade_date = latest_df.loc[key, "latest_trade_date"]

        if not pd.isna(latest_avg_gain):
            if is_gap(calendar_index, latest_trade_date, trade_date):
                reseeded = _reseed_from_gap(conn, isin, exchange, trade_date)
                if reseeded is None:
                    counts["skipped_reseed_insufficient_history"] += 1
                    continue
                gain, loss, avg_gain, avg_loss, rsi14 = reseeded
                counts["reseeded_after_gap"] += 1
            else:
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

