# core/rsi/rsi_calculator.py
#
# Pure RSI14 math -- Wilder's smoothing, hand-implemented (NOT
# pandas_ta.rma(), which uses ewm(adjust=False) starting from the very
# first data point rather than a simple-mean seed after `period` rows.
# Verified against a hand-checked 20-day table: pandas_ta.rma() diverges
# by up to ~9 RSI points in the weeks after the seed, which would not
# match what TradingView or a broker app shows. This module reproduces
# TradingView's exact convention instead.
#
# Gain/loss are derived from bhav_copy's own PREV_CLOSE column (as
# published by NSE/BSE), NOT from close.diff() against the previous row
# in our own dataframe. This matters: close.diff() only knows about rows
# WE happen to have -- if bhav_copy is missing a trading day for an isin,
# diff() silently bridges the gap using whichever row is before it,
# producing a wrong gain/loss for that day. prev_close comes straight
# from the exchange and is correct regardless of gaps in our own
# ingestion. It does NOT fix unadjusted corporate actions (splits/bonus)
# -- prev_close is just as raw/unadjusted as close itself, same known
# limitation as the rest of bhav_copy.
#
# Algorithm (period=14, hardcoded -- see rsi14d_workbook's own naming):
#   Rows with a null prev_close (typically only an isin's very first
#     ever listed day, if that falls inside our fetched window): gain/
#     loss/avg_gain/avg_loss/rsi14 all stay NULL for that row.
#   Seed (first `period` CONSECUTIVE rows with a valid gain/loss):
#     avg_gain = simple mean of those `period` gains
#     avg_loss = simple mean of those `period` losses
#   Every row after: avg_gain[i] = (avg_gain[i-1]*(period-1) + gain[i]) / period
#                     avg_loss[i] = (avg_loss[i-1]*(period-1) + loss[i]) / period
#                     rsi14 = 100 - 100/(1 + avg_gain/avg_loss)
#
# Inherently sequential (each row depends on the previous row's
# avg_gain/avg_loss) -- no vectorized pandas built-in reproduces this
# exactly, hence the explicit loop below.
#
# CONTINUITY FIX (see core/rsi/rsi_continuity.py) -- the walk is no
# longer one unbroken sequence per group. calendar_index (built from
# bhav_copy_metadata's own real-trading-session history) is consulted
# between every consecutive pair of rows; a gap of GAP_THRESHOLD_SESSIONS
# or more real sessions with no eligible row for this isin (a genuine
# suspension, not an ordinary weekend/holiday) restarts the Wilder
# average from scratch at that point instead of stepping across it --
# stepping would compute a Wilder gain/loss over a multi-week economic
# gap as if it were one ordinary trading day, which is the exact
# mechanism behind the MEIL RSI-corruption bug.

import pandas as pd

from core.rsi.rsi_math import RSI_PERIOD, seed, step, compute_rsi14
from core.rsi.rsi_continuity import is_gap


def compute_rsi14_for_isin(df_isin, calendar_index=None):
    """
    REPOINTED 2026-08-24 (security_id migration) -- name kept as-is to
    avoid churn on the 3 historical one-off repair/diagnostic scripts
    under tests/ that still call it directly against raw bhav_copy
    (tests/repair_availfc_rsi_WRITE.py, tests/repair_split_face_values_
    WRITE.py, tests/diagnose_availfc_rsi_manual.py). The function
    itself doesn't care what the grouping column represents -- df_isin
    just needs close/prev_close/trade_date, plus isin/exchange/series/
    symbol for output.

    security_id is echoed into the output DataFrame if the caller
    supplied one (the new bulk/incremental path always does, via
    bhav_copy_adjusted); otherwise falls back to isin as security_id --
    correct by construction for any of the old isin-scoped callers,
    since a security that never had an isin-lineage bridge has
    security_id == isin anyway.
    """
    df_isin = df_isin.reset_index(drop=True)
    close = df_isin["close"]
    prev_close = df_isin["prev_close"]
    trade_dates = df_isin["trade_date"]

    delta = close - prev_close
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)

    n = len(df_isin)
    avg_gain = pd.Series([float("nan")] * n)
    avg_loss = pd.Series([float("nan")] * n)

    valid_mask = gain.notna() & loss.notna()

    # Chunk starts: the first valid row, plus every row that follows a
    # real trading-session gap from the previous valid row. Each chunk
    # gets its own independent seed()+step() walk -- no bridging across
    # a gap boundary. With calendar_index=None (or no gaps found), this
    # degenerates to exactly the original single-chunk behavior.
    chunk_starts = []
    prev_idx = None
    for i in range(n):
        if not valid_mask.iloc[i]:
            continue
        if prev_idx is None:
            chunk_starts.append(i)
        elif calendar_index is not None and is_gap(calendar_index, trade_dates.iloc[prev_idx], trade_dates.iloc[i]):
            chunk_starts.append(i)
        prev_idx = i

    for chunk_num, start in enumerate(chunk_starts):
        end = chunk_starts[chunk_num + 1] if chunk_num + 1 < len(chunk_starts) else n
        seed_pos = start + RSI_PERIOD - 1
        if seed_pos >= end:
            continue  # fewer than RSI_PERIOD rows in this chunk -- stays pre-seed (NaN) until the next chunk, if any
        avg_gain.iloc[seed_pos], avg_loss.iloc[seed_pos] = seed(
            list(gain.iloc[start:seed_pos + 1]),
            list(loss.iloc[start:seed_pos + 1]),
        )
        for i in range(seed_pos + 1, end):
            avg_gain.iloc[i], avg_loss.iloc[i] = step(
                avg_gain.iloc[i - 1], avg_loss.iloc[i - 1], gain.iloc[i], loss.iloc[i]
            )

    rsi14 = pd.Series([
        compute_rsi14(
            None if pd.isna(ag) else ag,
            None if pd.isna(al) else al,
        )
        for ag, al in zip(avg_gain, avg_loss)
    ])

    return pd.DataFrame({
        "security_id": df_isin["security_id"] if "security_id" in df_isin.columns else df_isin["isin"],
        "isin": df_isin["isin"],
        "exchange": df_isin["exchange"],
        "series": df_isin["series"],
        "symbol": df_isin["symbol"],
        "trade_date": df_isin["trade_date"],
        "gain": gain,
        "loss": loss,
        "avg_gain": avg_gain,
        "avg_loss": avg_loss,
        "rsi14": rsi14,
    })


def compute_rsi14_all(df, calendar_index=None):
    """
    df: DataFrame across ALL securities, already one clean, continuous
    row per (security_id, exchange, trade_date) -- from
    rsi_persistence.fetch_bhav_copy_closes() reading bhav_copy_adjusted
    -- columns [security_id, isin, exchange, series, symbol,
    trade_date, close, prev_close].

    REPOINTED 2026-08-24 (security_id migration) -- grouped by
    (security_id, exchange) now, not (isin, exchange). A stock's RSI
    walk stays continuous across BOTH a series relabel (e.g. EQ -> BE
    -> EQ, the original MEIL fix) AND an isin change (e.g. MWL's
    SME-to-mainboard migration, bundled with a split on the same
    ex_date) instead of fragmenting into a dead old-isin series plus a
    freshly-reseeded new-isin series. ISIN/SERIES/SYMBOL are carried
    through as the row's own source values (audit trail only, same
    role they already played after 014.02.00) -- not part of the
    grouping key.

    Falls back to grouping by (isin, exchange) if the input lacks a
    security_id column, for backward compatibility with old-style
    isin-scoped callers (see compute_rsi14_for_isin's docstring).

    calendar_index: optional dict from core.rsi.rsi_continuity.
    build_calendar_index(), threaded through to each security's walk
    for gap detection. None disables gap-reseed (equivalent to the old
    always-bridge behavior) -- callers should always pass a real index
    in production; None exists mainly for isolated unit testing.
    """
    group_keys = ["security_id", "exchange"] if "security_id" in df.columns else ["isin", "exchange"]
    results = []
    for _key, group in df.groupby(group_keys, sort=False):
        group_sorted = group.sort_values("trade_date")
        results.append(compute_rsi14_for_isin(group_sorted, calendar_index=calendar_index))
    return pd.concat(results, ignore_index=True)
