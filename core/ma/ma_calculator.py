# core/ma/ma_calculator.py
#
# Pure moving-average math -- a plain trailing simple moving average
# (SMA) over `close`, computed per (security_id, exchange) group, one
# row per (security_id, exchange, trade_date) input row. Used by all
# three window sizes (MA9/MA50/MA200) across all three timeframes
# (daily/weekly/monthly) -- the math itself doesn't care about window
# size or which source table the closes came from, only how many rows
# to average over.
#
# Unlike RSI's Wilder smoothing, MA carries NO recursive state -- each
# row is a completely fresh recompute from its own trailing window of
# closes (a plain pandas rolling().mean()), so there's no seed/step
# distinction and no gap-continuity logic needed here: a genuine gap in
# a security's history just means fewer real closes are available
# inside the window for the rows immediately after it, which
# min_periods=window already handles correctly (NaN until `window` real
# rows exist in the trailing set, same as an ordinary insufficient-
# history case).
#
# security_id-keyed, isin-lineage-bridged input (bhav_copy_adjusted /
# bhav_copy_w / bhav_copy_m are all already this shape, see their own
# 2026-08-26 repoints) means a stock whose isin changed mid-history
# still gets one continuous rolling window across the change, same
# benefit RSI and the weekly/monthly rollups already have.

import pandas as pd


def compute_ma(df, window, column_name):
    """
    df: DataFrame with columns [security_id, isin, exchange, symbol,
    trade_date, close], one row per (security_id, exchange, trade_date)
    -- as produced by core.ma.ma_persistence.fetch_closes_for_ma().

    Returns a DataFrame with [security_id, isin, exchange, symbol,
    trade_date, column_name] -- one row per input row, `column_name`
    (e.g. "MA9") holding the trailing `window`-period simple moving
    average of close as of that row, NaN for the first (window - 1)
    rows of each (security_id, exchange) group (not enough history
    yet -- min_periods=window, never a partial-window average).

    isin/symbol carried through as the row's OWN source value (audit
    trail only, same role they play in bhav_copy_w/bhav_copy_m and
    rsi14*_workbook), not touched by the rolling computation itself.
    """
    if df.empty:
        return pd.DataFrame(columns=["security_id", "isin", "exchange", "symbol", "trade_date", column_name])

    work = df.sort_values(["security_id", "exchange", "trade_date"]).reset_index(drop=True)
    work[column_name] = (
        work.groupby(["security_id", "exchange"])["close"]
        .transform(lambda s: s.rolling(window=window, min_periods=window).mean())
    )
    return work[["security_id", "isin", "exchange", "symbol", "trade_date", column_name]]
