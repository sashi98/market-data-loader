# core/price_series/adjusted_series.py
#
# Builds bhav_copy_adjusted -- the single, stabilized, continuous price
# series every price-based indicator (RSI, moving averages, weekly/
# monthly rollups) should read from, instead of each one independently
# joining bhav_copy + corporate_actions + security_identity_lineage
# live, per query. See
# claude/bhav-copy-adjusted-clean-price-series-design-2026-08-23.md
# (TrackMyTrade project docs) for the full design.
#
# Two kinds of "noise" this removes, both confirmed on MWL
# (isin INE0JYY01011 -> INE0JYY01029, 1:10 split + SME-to-mainboard
# migration, ex_date 2026-07-10) during the 2026-08-23 RSI diagnosis:
#
#   1. Price-level (splits/bonuses) -- every historical open/high/low/
#      close/last is back-adjusted to TODAY's scale, not just gated at
#      the ex_date row the way the old per-query
#      ADJUSTMENT_FACTOR_JOIN_SQL (core/corporate_actions/adjustment.py,
#      still used by the RSI-only read path) was.
#   2. Identity-level (isin changes) -- bridged via
#      security_identity_lineage into one continuous security_id, so a
#      company's price history never appears to restart from zero.
#
# prev_close is deliberately NOT an independently re-adjusted raw
# value -- it's the previous row's own already-adjusted close (a plain
# shift within each security's sorted series). That's what structurally
# retires the close/prev_close asymmetry bug class
# (claude/rsi-adjustment-factor-prev-close-fix-2026-08-23.md) rather
# than just patching around it: there's no second, independent
# adjustment computation left to drift out of sync with the first.
#
# Always a full recompute per exchange, same "safe to always fully
# rebuild" philosophy as core/rsi/rsi_persistence.py /
# loaders/rsi14d_loader.py -- ON CONFLICT DO UPDATE, no incremental
# mode.

import numpy as np
import pandas as pd
from psycopg2.extras import execute_values

from core.rsi.rsi_continuity import ELIGIBLE_SERIES_EXISTS_SQL, MIN_LIQUIDITY_FILTER_SQL, TIEBREAK_RANK_SQL


class AdjustedSeriesError(Exception):
    """Raised when fetching bhav_copy/corporate_actions/lineage or upserting bhav_copy_adjusted fails."""
    pass


LINEAGE_SQL = "SELECT old_isin, new_isin FROM security_identity_lineage"


def fetch_isin_lineage_map(conn):
    """
    old_isin -> new_isin, single hop, straight from
    security_identity_lineage. resolve_security_id() below walks
    multiple hops if a chain is more than one migration long.
    """
    try:
        with conn.cursor() as cur:
            cur.execute(LINEAGE_SQL)
            rows = cur.fetchall()
    except Exception as e:
        raise AdjustedSeriesError(f"Failed to fetch security_identity_lineage: {e}")
    return {old_isin: new_isin for old_isin, new_isin in rows}


def resolve_security_id(isin, lineage_map, max_hops=10):
    """
    Follows old_isin -> new_isin hops until no further mapping exists,
    returning the final (most current) isin as the security_id. The
    overwhelming majority of isins have no lineage row at all and
    resolve to themselves in one step.

    max_hops is a defensive cap against a cyclical/malformed lineage
    entry -- should never happen, lineage rows are deliberate reviewed
    data (see the table's own Liquibase comment), but a cap costs
    nothing and turns a bad row into a wrong-looking security_id
    instead of an infinite loop.
    """
    current = isin
    for _ in range(max_hops):
        next_isin = lineage_map.get(current)
        if next_isin is None:
            return current
        current = next_isin
    return current


RAW_BHAV_COPY_SQL = f"""
    WITH ranked AS (
        SELECT bc.isin, bc.series, bc.symbol, bc.trade_date,
               bc.open, bc.high, bc.low, bc.close, bc.last,
               bc.tot_trd_qty, bc.tot_trd_val, bc.total_trades,
               {TIEBREAK_RANK_SQL}
          FROM bhav_copy bc
         WHERE bc.exchange = %(exchange)s
           AND bc.isin = ANY(%(isins)s)
           AND {ELIGIBLE_SERIES_EXISTS_SQL}
           AND {MIN_LIQUIDITY_FILTER_SQL}
    )
    SELECT isin, series, symbol, trade_date,
           open, high, low, close, last,
           tot_trd_qty, tot_trd_val, total_trades
      FROM ranked
     WHERE continuity_rank = 1
     ORDER BY isin, trade_date ASC
"""


def fetch_raw_bhav_copy(conn, exchange, isins):
    """
    Continuity-eligible, same-day-tiebroken bhav_copy rows for one
    exchange, restricted to `isins` -- the SAME eligibility/tiebreak
    rules core/rsi/rsi_persistence.py already applies (core/rsi/
    rsi_continuity.py), reused here rather than reimplemented, since
    bhav_copy_adjusted needs to guarantee exactly one row per
    (security_id, exchange, trade_date) for ANY downstream consumer,
    not just RSI.

    CHUNKED 2026-09-13 -- `isins` is now REQUIRED (was: every eligible
    isin for the exchange, fetched in one call). The caller
    (bhav_copy_d_price_adjustment_runner.py) now passes one
    security-id batch at a time -- see chunk_isins_by_security() below
    for why the batching happens by security_id, not by isin or date
    range. This is what actually bounds STEP 4's own memory footprint;
    upsert_bhav_copy_adjusted()'s COMMIT_EVERY_ROWS chunking (below)
    only ever bounded the Postgres transaction size, not this
    process's own memory -- the full unchunked fetch+build was still
    what caused the 2026-09-12/13 crash.
    """
    try:
        return pd.read_sql(RAW_BHAV_COPY_SQL, conn, params={"exchange": exchange, "isins": list(isins)})
    except Exception as e:
        raise AdjustedSeriesError(f"Failed to fetch raw bhav_copy for {exchange}: {e}")


DISTINCT_ISINS_SQL = f"""
    SELECT DISTINCT bc.isin
      FROM bhav_copy bc
     WHERE bc.exchange = %(exchange)s
       AND {ELIGIBLE_SERIES_EXISTS_SQL}
       AND {MIN_LIQUIDITY_FILTER_SQL}
"""


def fetch_distinct_isins(conn, exchange):
    """
    ADDED 2026-09-13 (chunking fix) -- just the distinct,
    continuity-eligible isins for one exchange, none of the OHLC
    columns and no same-day tiebreak (irrelevant to a plain distinct
    listing). Used to plan security-id batches BEFORE fetching any
    actual price rows -- see chunk_isins_by_security().
    """
    try:
        with conn.cursor() as cur:
            cur.execute(DISTINCT_ISINS_SQL, {"exchange": exchange})
            return [row[0] for row in cur.fetchall()]
    except Exception as e:
        raise AdjustedSeriesError(f"Failed to fetch distinct isins for {exchange}: {e}")


# ADDED 2026-09-13 (chunking fix) -- number of security_ids' worth of
# isins fetched+built+upserted per batch. Chunking by security_id
# (never by date range) is the only safe boundary here: this module's
# own docstring and bhav_copy_d_price_adjustment_runner.py's header
# both establish that a full recompute is required PER SECURITY,
# because a newly-discovered corporate action can retroactively change
# any historical row for that security, and build_adjusted_series()'s
# prev_close/adjustment-factor math needs a security's entire sorted
# history in one call to be correct. Date-range chunking would violate
# that. Security_id chunking preserves it exactly -- each batch still
# gets one security's FULL history, just fewer securities per call.
# ~500 securities/batch keeps each batch to roughly a few hundred
# thousand rows even for a security with years of history, well under
# the ~1.8-2.7M-row full-exchange size that caused the OOM.
DEFAULT_SECURITY_BATCH_SIZE = 500


def chunk_isins_by_security(isins, lineage_map, batch_size=DEFAULT_SECURITY_BATCH_SIZE):
    """
    Groups isins by their resolved security_id -- so every isin that
    bridges into the same continuous identity via
    security_identity_lineage stays in the SAME batch, since
    build_adjusted_series() needs a security's full sorted history in
    one call -- then splits those security_id groups into batches of
    up to `batch_size` security_ids each.

    Returns a list of isin-lists, one per batch. The batches partition
    the input isins exactly (every isin appears in exactly one batch,
    grouped with every other isin sharing its security_id), so a
    caller summing per-batch security_count / row_count / etc. across
    batches gets the correct exchange-wide total with no double
    counting and no risk of splitting one security's history across
    two batches.
    """
    isins_by_security = {}
    for isin in isins:
        security_id = resolve_security_id(isin, lineage_map)
        isins_by_security.setdefault(security_id, []).append(isin)

    security_ids = list(isins_by_security.keys())
    batches = []
    for start in range(0, len(security_ids), batch_size):
        batch_security_ids = security_ids[start:start + batch_size]
        batch_isins = [isin for security_id in batch_security_ids for isin in isins_by_security[security_id]]
        batches.append(batch_isins)
    return batches


MATCHED_CORPORATE_ACTIONS_SQL = """
    SELECT isin, ex_date, adjustment_factor
      FROM corporate_actions
     WHERE reconciliation_status = 'MATCHED'
       AND adjustment_factor IS NOT NULL
"""


def fetch_matched_corporate_actions(conn):
    """
    Every MATCHED corporate action with a trusted adjustment_factor,
    isin-keyed -- exchange-agnostic by design (a single action's factor
    applies identically to every exchange the isin trades on; ISIN
    itself is a depository-level identifier, not exchange-level -- same
    as the old ADJUSTMENT_FACTOR_JOIN_SQL never filtered by exchange
    either).
    """
    try:
        return pd.read_sql(MATCHED_CORPORATE_ACTIONS_SQL, conn)
    except Exception as e:
        raise AdjustedSeriesError(f"Failed to fetch matched corporate_actions: {e}")


ADJUSTED_COLUMNS = [
    "security_id", "source_isin", "series", "symbol", "trade_date",
    "open", "high", "low", "close", "last", "prev_close",
    "tot_trd_qty", "tot_trd_val", "total_trades", "adjustment_factor_applied",
]


def build_adjusted_series(raw_df, actions_df, lineage_map):
    """
    Core transform: raw per-isin bhav_copy rows + matched corporate
    actions + isin lineage -> one clean, continuous, adjusted row per
    (security_id, trade_date) for a single exchange.

    Steps (see the module docstring and
    claude/bhav-copy-adjusted-clean-price-series-design-2026-08-23.md):
      1. Map every isin (in both raw_df and actions_df) to its
         security_id via resolve_security_id() -- bridges an isin
         change into one continuous identity.
      2. Sort each security_id's rows by trade_date -- stitches rows
         from different source isins into one ordered series.
      3. cumulative_factor(trade_date) = product of every matched
         action's adjustment_factor for this security_id with
         ex_date > trade_date -- applied to open/high/low/close/last
         for that row (the whole OHLC, not just close, so weekly/
         monthly rollups and any candlestick-based indicator get clean
         data too).
      4. prev_close = the PREVIOUS row's own already-adjusted close (a
         plain shift within the sorted per-security series) -- not a
         second, independently-adjusted raw value.

    Returns a DataFrame with ADJUSTED_COLUMNS. Caller is expected to
    add the constant `exchange` column before upserting (kept out of
    this function since it's the same for every row in one call).
    """
    if raw_df.empty:
        return pd.DataFrame(columns=ADJUSTED_COLUMNS)

    df = raw_df.copy()
    df["security_id"] = df["isin"].apply(lambda isin: resolve_security_id(isin, lineage_map))
    df = df.rename(columns={"isin": "source_isin"})

    actions = actions_df.copy()
    if not actions.empty:
        actions["security_id"] = actions["isin"].apply(lambda isin: resolve_security_id(isin, lineage_map))

    # FIXED 2026-08-26 -- this used to loop over every security_id
    # (~3,900-5,900 of them per exchange), building a full sort_values()
    # + reset_index() COPY of that security's own small DataFrame,
    # appending each to a Python list, then pd.concat()-ing all of them
    # back into one frame at the end. That's ~N small pandas objects
    # (each with its own index/block-manager overhead) held in memory
    # simultaneously, PLUS a final concat that briefly needs memory for
    # both the sum of all those pieces AND the new combined frame --
    # exactly the kind of memory-amplification pattern that took down a
    # 2026-08-26 rerun with a numpy ArrayMemoryError (failing to
    # allocate a mere 30KB, a sign of real memory exhaustion, not a
    # one-off fluke). Same output, one pass now: a single global sort by
    # (security_id, trade_date) instead of ~N tiny sorts, the
    # adjustment factor written into one preallocated numpy array
    # instead of N separate DataFrame columns, and prev_close via a
    # single vectorized groupby().shift() instead of N per-group shifts
    # + a final concat.
    df = df.sort_values(["security_id", "trade_date"], kind="mergesort").reset_index(drop=True)

    n = len(df)
    applied = np.ones(n, dtype=float)
    trade_dates = df["trade_date"].to_numpy()

    if not actions.empty:
        actions_by_security = {
            sec_id: (
                grp["ex_date"].to_numpy(),
                grp["adjustment_factor"].astype(float).to_numpy(),
            )
            for sec_id, grp in actions.groupby("security_id", sort=False)
        }
        # Still a per-security, per-row loop for the actual factor
        # math (correctness-critical, and each security's own action
        # list is small) -- but now writing into a flat numpy array by
        # integer position instead of building a new DataFrame column
        # per security, so it carries none of the per-object pandas
        # overhead the old version did.
        for security_id, row_positions in df.groupby("security_id", sort=False).indices.items():
            ex_dates, factors = actions_by_security.get(security_id, (None, None))
            if ex_dates is None or len(ex_dates) == 0:
                continue  # already defaulted to 1.0 above
            for pos in row_positions:
                mask = ex_dates > trade_dates[pos]
                if mask.any():
                    applied[pos] = float(factors[mask].prod())

    df["adjustment_factor_applied"] = applied

    for col in ("open", "high", "low", "close", "last"):
        df[col] = df[col].astype(float) * df["adjustment_factor_applied"]

    df["prev_close"] = df.groupby("security_id", sort=False)["close"].shift(1)

    return df[ADJUSTED_COLUMNS]


UPSERT_SQL = """
    INSERT INTO bhav_copy_adjusted (
        security_id, source_isin, exchange, series, symbol, trade_date,
        open, high, low, close, last, prev_close,
        tot_trd_qty, tot_trd_val, total_trades, adjustment_factor_applied
    )
    VALUES %s
    ON CONFLICT (security_id, exchange, trade_date) DO UPDATE SET
        source_isin = EXCLUDED.source_isin,
        series = EXCLUDED.series,
        symbol = EXCLUDED.symbol,
        open = EXCLUDED.open,
        high = EXCLUDED.high,
        low = EXCLUDED.low,
        close = EXCLUDED.close,
        last = EXCLUDED.last,
        prev_close = EXCLUDED.prev_close,
        tot_trd_qty = EXCLUDED.tot_trd_qty,
        tot_trd_val = EXCLUDED.tot_trd_val,
        total_trades = EXCLUDED.total_trades,
        adjustment_factor_applied = EXCLUDED.adjustment_factor_applied,
        updated_at = CURRENT_TIMESTAMP
"""

UPSERT_COLUMNS = [
    "security_id", "source_isin", "exchange", "series", "symbol", "trade_date",
    "open", "high", "low", "close", "last", "prev_close",
    "tot_trd_qty", "tot_trd_val", "total_trades", "adjustment_factor_applied",
]


# FIXED 2026-08-26 -- upsert_bhav_copy_adjusted() used to run its ENTIRE
# upsert (potentially ~1.8-2.7M rows per exchange) as one uncommitted
# transaction, committing only once at the very end. That's fine the
# FIRST time a table is populated (every row is a plain INSERT), but
# every subsequent run hits the ON CONFLICT DO UPDATE path for nearly
# every row instead -- UPDATEs are far more expensive than INSERTs in
# Postgres (new tuple version + index maintenance per row), and holding
# ~1.8M of them open in one transaction is exactly what took down a
# 2026-08-26 rerun with a Postgres "out of memory" error mid-upsert,
# even though the row count barely changed from the first (all-insert)
# run two days earlier. Committing every COMMIT_EVERY_ROWS rows instead
# bounds each transaction's memory/WAL footprint.
#
# Trade-off: this is no longer strictly all-or-nothing -- if a later
# chunk fails, earlier chunks in this call are already committed. That
# is an acceptable trade for a full-recompute-every-run loader (see
# this module's own docstring and bhav_copy_adjustment_loader.py's):
# ON CONFLICT DO UPDATE makes every row idempotent, so simply re-running
# the whole loader after a failure is safe and self-correcting -- there
# is no partial/incremental mode whose invariants a partial commit could
# violate.
COMMIT_EVERY_ROWS = 100_000


def upsert_bhav_copy_adjusted(conn, adjusted_df):
    if adjusted_df.empty:
        return 0

    raw_values = adjusted_df[UPSERT_COLUMNS].itertuples(index=False, name=None)
    values = [
        tuple(None if isinstance(v, float) and pd.isna(v) else v for v in row)
        for row in raw_values
    ]
    try:
        for start in range(0, len(values), COMMIT_EVERY_ROWS):
            chunk = values[start:start + COMMIT_EVERY_ROWS]
            with conn.cursor() as cur:
                execute_values(cur, UPSERT_SQL, chunk, page_size=1000)
            conn.commit()
    except Exception as e:
        conn.rollback()
        raise AdjustedSeriesError(f"Failed to upsert bhav_copy_adjusted: {e}")
    return len(values)


# ADDED 2026-08-30 -- bhav_copy_adjusted_metadata (002.07.01's own
# migration) so STEP 4's own runner can check its OWN freshness
# independently, instead of only running because STEP 2 happened to
# find new bhav copy data that cycle. One row PER EXCHANGE, upserted
# every run -- see that changelog's own comment for why this table's
# shape genuinely differs from every other metadata table here (STEP 4
# has no date-range concept at all, always a full recompute).

def upsert_bhav_copy_adjusted_metadata(conn, exchange, run_status, latest_trade_date, row_count,
                                       processing_time_ms=None, error_message=None):
    """
    Upserts bhav_copy_adjusted_metadata by exchange (its own unique
    key -- one row per exchange, not accumulating history). Commits
    immediately, same idiom as every other metadata-table writer in
    this repo (e.g. core/rollup/rollup_persistence.py's upsert_metadata()).
    """
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT id FROM bhav_copy_adjusted_metadata WHERE exchange = %s", (exchange,))
            existing = cur.fetchone()
            if existing:
                cur.execute(
                    """
                    UPDATE bhav_copy_adjusted_metadata
                       SET run_status = %s, latest_trade_date = %s, row_count = %s,
                           processing_time_ms = %s, error_message = %s, updated_at = CURRENT_TIMESTAMP
                     WHERE id = %s
                    """,
                    (run_status, latest_trade_date, row_count, processing_time_ms, error_message, existing[0]),
                )
            else:
                cur.execute(
                    """
                    INSERT INTO bhav_copy_adjusted_metadata
                        (exchange, run_status, latest_trade_date, row_count,
                         processing_time_ms, error_message, created_at, updated_at)
                    VALUES (%s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                    """,
                    (exchange, run_status, latest_trade_date, row_count, processing_time_ms, error_message),
                )
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise AdjustedSeriesError(f"Failed to upsert bhav_copy_adjusted_metadata for {exchange}: {e}")


def is_fresh_through(conn, ceiling_date):
    """
    True if BOTH NSE and BSE already have a SUCCESS
    bhav_copy_adjusted_metadata row whose latest_trade_date covers
    ceiling_date -- i.e. this cycle's own latest trading session (4pm
    today through 3:30pm tomorrow) is already reflected in
    bhav_copy_adjusted, so there's genuinely nothing new for STEP 4 to
    do. Same convention as core/corporate_actions/
    corporate_actions_persistence.py's own is_fresh_through().

    An exchange with NO successful row at all counts as NOT fresh --
    same as one whose latest SUCCESS row's latest_trade_date falls
    short of ceiling_date.
    """
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT exchange, latest_trade_date FROM bhav_copy_adjusted_metadata WHERE run_status = 'SUCCESS'"
            )
            latest_by_exchange = dict(cur.fetchall())
    except Exception as e:
        raise AdjustedSeriesError(f"Failed to check bhav_copy_adjusted_metadata freshness: {e}")

    for exchange in ("NSE", "BSE"):
        latest = latest_by_exchange.get(exchange)
        if latest is None or latest < ceiling_date:
            return False
    return True
