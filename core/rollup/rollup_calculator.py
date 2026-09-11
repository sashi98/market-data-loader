# core/rollup/rollup_calculator.py
#
# Pure rollup math -- turns a DataFrame of daily bhav_copy_adjusted rows
# (one per security_id+exchange+trade_date, already deduped/eligibility-
# filtered/isin-lineage-bridged -- see
# rollup_persistence.fetch_daily_rows()) into one row per
# security_id+exchange+period, per Story A's rollup-mapping table
# (claude/user-story-weekly-monthly-ohlc-rsi-candlestick.md).
#
# REPOINTED 2026-08-26 -- grouping/keying moves from (isin, exchange) to
# (security_id, exchange), same fix as core/rsi/rsi_persistence.py's own
# repoint: a stock whose isin changed mid-history (e.g. MWL's
# SME-to-mainboard migration) now rolls up as ONE continuous
# weekly/monthly series instead of fragmenting at the isin boundary.
# isin is still carried through in the input/output shape as an ordinary
# audit column (which source isin the period's LAST trading day actually
# traded under), same role series/symbol already play, but it is no
# longer part of the grouping key.
#
# prevClose is NOT simply "the prior row's close" the way daily
# bhav_copy's own prev_close column works -- it is always the PRIOR
# PERIOD's close for the same security_id+exchange, never the last day's
# own prev_close. Within THIS call's own input, that falls out of a
# plain shift(1) once rows are grouped and sorted by period. The one
# case that can't be resolved purely from this call's input: a
# security_id's EARLIEST period present in df has no preceding period IN
# THIS INPUT to shift from -- for Part 1 (whole-history input) that's a
# genuine "security's actual first-ever period, no prior period exists
# at all" case (prevClose correctly stays NULL). For Part 2 (a single
# new period's worth of daily rows per call), EVERY period is "the
# earliest in this call's input" by construction, so the caller MUST
# supply prior_close_lookup (sourced from the rollup table itself --
# see rollup_persistence.fetch_prior_close_lookup()) or every
# incrementally-rolled-up period would incorrectly show a NULL
# prevClose/ltp_percent_change.

import pandas as pd

from core.rollup.period import period_start


REQUIRED_COLUMNS = [
    "security_id", "isin", "exchange", "series", "symbol", "trade_date",
    "open", "high", "low", "close", "last", "tot_trd_qty", "tot_trd_val", "total_trades",
]

OUTPUT_COLUMNS = [
    "security_id", "isin", "exchange", "series", "symbol", "open", "high", "low", "close", "last",
    "prev_close", "tot_trd_qty", "tot_trd_val", "trade_date", "total_trades", "ltp_percent_change",
]


def compute_rollup(df, period_type, prior_close_lookup=None):
    """
    df must have REQUIRED_COLUMNS. Returns a DataFrame with
    OUTPUT_COLUMNS -- one row per (security_id, exchange, period_start),
    sorted by security_id, exchange, trade_date (the period start).
    Empty input produces an empty-but-correctly-shaped output.

    prior_close_lookup: optional {(security_id, exchange, period_start): close}
    for periods whose preceding period is not itself present in df --
    see module docstring.
    """
    if df.empty:
        return pd.DataFrame(columns=OUTPUT_COLUMNS)

    prior_close_lookup = prior_close_lookup or {}

    work = df.copy()
    work["period_start"] = work["trade_date"].apply(lambda d: period_start(d, period_type))
    work = work.sort_values("trade_date")

    rows = []
    for (security_id, exchange, p_start), g in work.groupby(["security_id", "exchange", "period_start"], sort=False):
        first_row = g.iloc[0]
        last_row = g.iloc[-1]
        rows.append({
            "security_id": security_id,
            "isin": last_row["isin"],
            "exchange": exchange,
            "series": last_row["series"],
            "symbol": last_row["symbol"],
            "open": first_row["open"],
            "high": g["high"].max(),
            "low": g["low"].min(),
            "close": last_row["close"],
            "last": last_row["last"],
            "tot_trd_qty": g["tot_trd_qty"].sum(min_count=1),
            "tot_trd_val": g["tot_trd_val"].sum(min_count=1),
            "trade_date": p_start,
            "total_trades": g["total_trades"].sum(min_count=1),
        })

    result = pd.DataFrame(rows).sort_values(["security_id", "exchange", "trade_date"]).reset_index(drop=True)

    # Shift(1) within each security_id+exchange group over THIS call's
    # own output rows -- resolves prevClose for any period whose
    # preceding period is ALSO present in this call's input (always true
    # for Part 1's whole-history input, after the first period).
    result["prev_close"] = result.groupby(["security_id", "exchange"])["close"].shift(1)

    missing_prior = result["prev_close"].isna()
    if missing_prior.any() and prior_close_lookup:
        for idx in result.index[missing_prior]:
            key = (result.at[idx, "security_id"], result.at[idx, "exchange"], result.at[idx, "trade_date"])
            if key in prior_close_lookup:
                result.at[idx, "prev_close"] = prior_close_lookup[key]

    # DEFENSIVE CAST -- prior_close_lookup's own values should already be
    # plain floats (fetch_prior_close_lookup() now casts explicitly, after
    # a live crash: raw psycopg2 cursors return Postgres NUMERIC as
    # decimal.Decimal, not float, which broke the subtraction below with
    # "unsupported operand type(s) for -: 'float' and 'decimal.Decimal'").
    # Kept here too, one call further downstream, so ANY future caller
    # passing a differently-sourced lookup dict can't reintroduce the same
    # crash silently -- pd.to_numeric is a no-op if the column is already
    # clean float64.
    result["prev_close"] = pd.to_numeric(result["prev_close"], errors="coerce")

    result["ltp_percent_change"] = _percent_change(result["close"], result["prev_close"])

    return result[OUTPUT_COLUMNS]


def _percent_change(close, prev_close):
    """(close - prev_close) / prev_close * 100, rounded to 2dp -- NaN
    (not an error/inf) wherever prev_close is NULL or zero."""
    pct = pd.Series(float("nan"), index=close.index)
    valid = prev_close.notna() & (prev_close != 0)
    pct.loc[valid] = ((close.loc[valid] - prev_close.loc[valid]) / prev_close.loc[valid] * 100).round(2)
    return pct
