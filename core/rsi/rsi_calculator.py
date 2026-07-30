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

import pandas as pd

from core.rsi.rsi_math import RSI_PERIOD, seed, step, compute_rsi14


def compute_rsi14_for_isin(df_isin):
    df_isin = df_isin.reset_index(drop=True)
    close = df_isin["close"]
    prev_close = df_isin["prev_close"]

    delta = close - prev_close
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)

    n = len(df_isin)
    avg_gain = pd.Series([float("nan")] * n)
    avg_loss = pd.Series([float("nan")] * n)

    valid_mask = gain.notna() & loss.notna()
    first_valid_idx = valid_mask.idxmax() if valid_mask.any() else None

    if first_valid_idx is not None:
        seed_pos = first_valid_idx + RSI_PERIOD - 1
        if seed_pos < n:
            avg_gain.iloc[seed_pos], avg_loss.iloc[seed_pos] = seed(
                list(gain.iloc[first_valid_idx:seed_pos + 1]),
                list(loss.iloc[first_valid_idx:seed_pos + 1]),
            )
            for i in range(seed_pos + 1, n):
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


def compute_rsi14_all(df):
    """
    df: DataFrame across ALL isins, columns [isin, exchange, series,
    symbol, trade_date, close, prev_close].

    Grouped by (isin, exchange, series, symbol) -- a stock trading under
    multiple series on the same exchange (e.g. EQ vs BE) is a genuinely
    distinct price series with its own independent RSI walk, same
    reasoning as the isin+exchange split. SYMBOL is included too because
    bhav_copy's symbol can carry a remark-flag suffix (e.g. BSE's
    trailing '#') on some trade dates but not others for the same
    isin+exchange+series -- without SYMBOL in the grouping key, those
    rows would collide on (isin, exchange, series, trade_date) at upsert
    time. Known tradeoff: the '#'-suffixed variant is usually sparse/
    non-contiguous, so it will rarely accumulate 14 consecutive rows and
    its RSI14 will mostly stay NULL -- expected, not a bug (see the
    changelog's table-level comment for more).
    """
    results = []
    for (isin, exchange, series, symbol), group in df.groupby(["isin", "exchange", "series", "symbol"], sort=False):
        group_sorted = group.sort_values("trade_date")
        results.append(compute_rsi14_for_isin(group_sorted))
    return pd.concat(results, ignore_index=True)
