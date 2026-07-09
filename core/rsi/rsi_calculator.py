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

RSI_PERIOD = 14


def compute_rsi14_for_isin(df_isin):
    """
    df_isin: DataFrame for a SINGLE isin, columns [isin, symbol, trade_date,
    close, prev_close], already sorted by trade_date ASCENDING. Index does
    not matter, will be reset internally.

    Returns a new DataFrame with columns
    [isin, symbol, trade_date, gain, loss, avg_gain, avg_loss, rsi14] --
    one row per input row. gain/loss are NaN only where prev_close itself
    is NaN (typically the isin's very first ever listed day, if that
    falls inside the fetched window). avg_gain/avg_loss/rsi14 stay NaN
    until the seed (first `period` consecutive rows with a valid
    gain/loss), populated from there on.
    """
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
        seed_pos = first_valid_idx + RSI_PERIOD - 1  # 0-indexed row where the seed lands
        if seed_pos < n:
            avg_gain.iloc[seed_pos] = gain.iloc[first_valid_idx:seed_pos + 1].mean()
            avg_loss.iloc[seed_pos] = loss.iloc[first_valid_idx:seed_pos + 1].mean()

            for i in range(seed_pos + 1, n):
                avg_gain.iloc[i] = (avg_gain.iloc[i - 1] * (RSI_PERIOD - 1) + gain.iloc[i]) / RSI_PERIOD
                avg_loss.iloc[i] = (avg_loss.iloc[i - 1] * (RSI_PERIOD - 1) + loss.iloc[i]) / RSI_PERIOD

    rs = avg_gain / avg_loss
    rsi14 = 100 - (100 / (1 + rs))

    return pd.DataFrame({
        "isin": df_isin["isin"],
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
    df: DataFrame across ALL isins, columns [isin, symbol, trade_date,
    close, prev_close]. Does NOT need to be pre-sorted -- sorting happens
    per group.

    Returns the concatenated per-isin result of compute_rsi14_for_isin(),
    one row per (isin, trade_date) in the input.
    """
    results = []
    for isin, group in df.groupby("isin", sort=False):
        group_sorted = group.sort_values("trade_date")
        results.append(compute_rsi14_for_isin(group_sorted))
    return pd.concat(results, ignore_index=True)
