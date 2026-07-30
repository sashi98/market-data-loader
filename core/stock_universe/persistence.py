# core/stock_universe/persistence.py
#
# DB read/write logic for stock_universe_update_listener.py -- checking
# whether all five exchange uploads are currently SUCCESS, reading/
# writing the enrichment cursor, fetching the ISIN worklist, and writing
# fundamentals back to stock_universe.
#
# REDESIGNED: NSE and BSE are now resolved and written COMPLETELY
# INDEPENDENTLY, per explicit instruction -- no more "common fields
# shared across every row sharing an isin" concept, no more
# EXCHANGE_SPECIFIC_FIELDS/split_fields_by_scope. A dual-listed
# company's NSE row is populated purely from (NSE official API +
# yfinance NSE ticker); its BSE row is populated purely from (BSE
# official API + yfinance BSE ticker). Neither side ever influences the
# other's row. This is a deliberate reversal of the earlier ISIN-merged
# design -- accepting more total API calls per dual-listed company in
# exchange for each row reflecting ONLY its own exchange's real data,
# with zero risk of one exchange's value leaking onto the other's row.

from psycopg2.extras import Json
import math

REQUIRED_DATA_SOURCES = ["nse_data", "nse_sme_data", "bse_data", "bse_sme_data", "nse_fno_data"]

# Confirmed real, not hypothetical -- stock_universe currently contains
# mutual fund plan units and ETFs alongside real equities (e.g. "NIPPON
# INDIA EQUITY HYBRID FUND - SEGREGATED PORTFOLIO 2DIRECT QUARTERLY
# DIVIDEND PLANREINVESTMENT", symbol "11AGG"). Neither BSE's Group nor
# Instrument column can discriminate this -- both are shared with real
# equity rows (confirmed: Nilkamal Limited and this same fund both have
# Group='B', Instrument='Equity'). Company name is the only reliable
# signal available on the columns actually captured from CSV. Checked
# case-insensitively, substring match -- this is a KEYWORD list, not an
# exact symbol list, deliberately: it catches every current AND future
# fund/ETF entry automatically, not just the specific ones already seen.
EXCLUDED_NAME_KEYWORDS = ["%MUTUAL FUND%", "%ETF%", "%SEGREGATED PORTFOLIO%"]


class StockUniversePersistenceError(Exception):
    """Raised when a stock_universe/stock_universe_metadata/
    stock_universe_enrichment_run query fails."""
    pass


def normalize_field_value(value):
    """
    Converts a numpy-scalar-style value (numpy.int64, numpy.float64,
    etc.) to its native Python equivalent via .item(); every other
    value type passes through completely unchanged, INCLUDING
    lists/dicts -- deliberately NOT wrapped in psycopg2.extras.Json
    here, since that would break printing (a Json-wrapped value prints
    as "<psycopg2.extras.Json object at 0x...>", not the actual list
    content -- worse than the numpy display issue this function exists
    to fix). Json-wrapping stays local to update_stock_fundamentals
    below, applied only right before the actual SQL execution, since
    it's purely a psycopg2-parameter concern, not a general "clean up
    this value" concern.

    Used by BOTH update_stock_fundamentals below AND
    stock_universe_update_listener.py's own [OK] log line, so what gets
    logged is exactly what gets written (numpy display artifacts
    aside), not two different representations of the same data.

    CONFIRMED REAL via a live test run -- the nse package's
    totalMarketCap sometimes comes back as numpy.int64, and psycopg2
    cannot adapt numpy types at all ("can't adapt type 'numpy.int64'"),
    failing the whole UPDATE. Deliberately general rather than fixed to
    one known field, since tradingview_client.py's tier reads from a
    pandas DataFrame too, and pandas values are commonly numpy-typed by
    default. Duck-typed -- no numpy import needed here.
    """
    if hasattr(value, "item") and not isinstance(value, (list, dict, str)):
        return value.item()
    return value


def fetch_latest_metadata_status(conn):
    """
    Returns {data_source: (id, status)} for the MOST RECENT row per
    data source -- the latest upload attempt for each, whether it
    succeeded or not, not just "has it ever succeeded."
    """
    query = """
        SELECT DISTINCT ON (data_source) data_source, id, status
          FROM stock_universe_metadata
         ORDER BY data_source, id DESC
    """
    try:
        with conn.cursor() as cur:
            cur.execute(query)
            rows = cur.fetchall()
        return {data_source: (row_id, status) for data_source, row_id, status in rows}
    except Exception as e:
        raise StockUniversePersistenceError(f"Failed to fetch latest metadata status: {e}")


def is_batch_ready(metadata_status):
    """
    True only if every one of the five required data sources has a row
    at all AND its most recent attempt is status='success'. Missing a
    data source entirely (never uploaded) counts as not ready, same as
    one that failed.
    """
    if not all(ds in metadata_status for ds in REQUIRED_DATA_SOURCES):
        return False
    return all(status == "success" for _, status in metadata_status.values())


def get_max_metadata_id(metadata_status):
    return max(row_id for row_id, _ in metadata_status.values())


def fetch_last_processed_cursor(conn):
    """
    The listener's real cursor -- the last_processed_metadata_id of the
    MOST RECENT row (ORDER BY id DESC LIMIT 1) with status IN
    ('SUCCESS', 'PARTIAL'), NOT MAX(last_processed_metadata_id). Those
    are only the same value if it happens to be strictly monotonically
    increasing across every row forever, which is an assumption, not a
    guarantee (a future retry, backfill, or manual correction could
    break it silently) -- see 001.04.00's own table comment for the
    same reasoning documented at the schema level.

    Returns 0, not None, when the table is empty or nothing has ever
    succeeded -- an empty result set and a NULL value are different
    things, and 0 is what "nothing processed yet, everything is
    eligible" actually means to every caller downstream.
    """
    query = """
        SELECT last_processed_metadata_id
          FROM stock_universe_enrichment_run
         WHERE status IN ('SUCCESS', 'PARTIAL')
         ORDER BY id DESC
         LIMIT 1
    """
    try:
        with conn.cursor() as cur:
            cur.execute(query)
            row = cur.fetchone()
        return row[0] if row else 0
    except Exception as e:
        raise StockUniversePersistenceError(f"Failed to fetch enrichment cursor: {e}")


def fetch_isin_exchange_map(conn):
    """
    {isin_number: {"nse_symbol": str_or_None, "nse_exchange": str_or_None, "bse_symbol": str_or_None, "bse_exchange": str_or_None, "bse_security_code": str_or_None}}
    for every isin currently in stock_universe, EXCLUDING anything
    matching EXCLUDED_NAME_KEYWORDS (mutual fund plan units, ETFs) --
    filtered here at the query level so these never enter the work
    queue at all. Confirmed real via a live test run: 10/10 of the
    first alphabetical BSE rows were exactly this kind of contamination,
    all returning HTTP 404 from yfinance.

    nse_exchange ("NSE" or "NSE SME") tracks WHICH exchange nse_symbol
    actually came from -- needed because NSE's own official API
    (nse_client.py) requires a different "series" code for SME stocks
    than for mainboard ones. bse_exchange ("BSE" or "BSE SME") tracks
    the same thing for the BSE side.

    Still ONE work item per ISIN, purely as a convenient way to fetch
    both sides' identifiers in a single query -- but per the current
    design, the isin's NSE side and BSE side are then resolved and
    written COMPLETELY INDEPENDENTLY by the caller (see
    stock_universe_update_listener.py's _worker), not merged. This map
    is just "here are the identifiers for both sides, if they exist,"
    nothing more.

    bse_symbol (text, e.g. "ACCPL") is separate from bse_security_code
    (numeric, e.g. "523031") -- yfinance and the bse package both need
    the numeric code for their own BSE conventions ("523031.BO",
    equityMetaInfo(security_code)), but TradingView needs the plain
    text symbol instead (TradingView doesn't use BSE's numeric scrip
    codes at all).

    Preference within each family: NSE over NSE SME, BSE over BSE SME --
    mainboard preferred over SME when a company happens to have a row
    under both (shouldn't normally happen given the cross-segment prune
    fix, but handled defensively here rather than assumed).
    """
    exclude_clause = " AND ".join("name_of_company NOT ILIKE %s" for _ in EXCLUDED_NAME_KEYWORDS)
    query = f"""
        SELECT isin_number, exchange, symbol, security_code
          FROM stock_universe
         WHERE {exclude_clause}
         ORDER BY isin_number, exchange
    """
    try:
        with conn.cursor() as cur:
            cur.execute(query, EXCLUDED_NAME_KEYWORDS)
            rows = cur.fetchall()
    except Exception as e:
        raise StockUniversePersistenceError(f"Failed to fetch isin/exchange map: {e}")

    isin_map = {}
    for isin_number, exchange, symbol, security_code in rows:
        entry = isin_map.setdefault(isin_number, {"nse_symbol": None, "nse_exchange": None, "bse_symbol": None, "bse_exchange": None, "bse_security_code": None})

        if exchange == "NSE":
            entry["nse_symbol"] = symbol
            entry["nse_exchange"] = exchange
        elif exchange == "NSE SME" and entry["nse_symbol"] is None:
            entry["nse_symbol"] = symbol
            entry["nse_exchange"] = exchange

        if exchange == "BSE":
            entry["bse_symbol"] = symbol
            entry["bse_exchange"] = exchange
            entry["bse_security_code"] = security_code
        elif exchange == "BSE SME" and entry["bse_symbol"] is None:
            entry["bse_symbol"] = symbol
            entry["bse_exchange"] = exchange
            entry["bse_security_code"] = security_code

    return isin_map


# Columns written by this pipeline that are constrained to a specific
# DECIMAL(precision, scale) in stock_universe (001.01.00) -- a value
# whose absolute magnitude is >= 10**(precision - scale) will not fit,
# and Postgres rejects the ENTIRE UPDATE statement for even one such
# column, taking every other -- otherwise perfectly good -- field in
# the same statement down with it (see update_stock_fundamentals'
# own docstring). Deliberately only the numeric fundamentals columns
# this pipeline actually writes -- not every DECIMAL column in the
# table (e.g. FACE_VALUE, which only the Java CSV parsers ever touch).
DECIMAL_COLUMN_LIMITS = {
    "market_capitalization": (20, 2),
    "price_to_earnings_ratio": (10, 2),
    "price_to_book_ratio": (10, 2),
    "eps": (10, 2),
    "return_on_equity": (6, 2),
    "debt_to_equity_ratio": (10, 2),
    "dividend_yield": (6, 2),
}


def _sanitize_numeric_fields(fields):
    """
    Splits fields into (clean, dropped). clean is safe to hand straight
    to update_stock_fundamentals' UPDATE; dropped is a list of
    (column, original_value, reason) tuples for anything that would
    make that UPDATE fail outright:

      - non-finite values (inf, -inf, nan) -- CONFIRMED REAL from a
        full production run, sourced from yfinance/pandas ratios
        computed with a zero denominator (e.g. P/E with zero EPS).
        Postgres numeric columns cannot store these at all, in ANY
        precision -- this check applies to every numeric field
        regardless of DECIMAL_COLUMN_LIMITS.
      - finite values whose magnitude exceeds the specific column's own
        DECIMAL(precision, scale) range, per DECIMAL_COLUMN_LIMITS --
        CONFIRMED REAL: a handful of penny-stock/near-zero-equity
        ISINs producing ratios in the hundreds of millions on the same
        run, a genuine (if extreme) computed value, not a data error,
        but still too large for that column to hold.

    CONFIRMED REAL BUG in the first version of this function: it used
    `isinstance(value, float)` to decide whether a value was even
    eligible for the inf/nan check at all. That is NOT a reliable test
    for "is this numeric" -- tradingview_client.py's tier reads values
    straight out of a pandas DataFrame row (`value = row[tv_field]`),
    which are numpy scalar types, not necessarily plain Python float
    (e.g. numpy.float32 does not subclass float at all, unlike
    numpy.float64). An inf value backed by such a type sailed straight
    through isinstance's gate untouched, past normalize_field_value
    too, and reached Postgres raw -- confirmed via a live run where
    the exact same 6 ISINs failed with "cannot hold an infinite value"
    both BEFORE this function existed and AFTER, while the separate
    magnitude-overflow case (a different bug, fixed correctly the first
    time) worked as intended on the same run. Fixed here by attempting
    float(value) UNCONDITIONALLY instead of gating on isinstance first
    -- float() itself already handles native float, any numpy numeric
    width, Decimal, and even numeric strings ('inf', '1e400')
    uniformly, and simply raises for anything genuinely non-numeric
    (strings like "Capital Goods", lists, dates, None), which is
    exactly the signal needed to leave those fields untouched.

    Non-numeric fields (sector, industry, index_list, date_of_listing,
    etc.) pass through untouched into clean -- float(value) raises
    TypeError/ValueError for these, which is treated as "not this
    function's concern," not as a reason to drop the field.

    Deliberately drops rather than coerces/clamps/rounds -- a clamped
    value would silently misrepresent the real (if extreme or
    undefined) figure as something else entirely; dropping the single
    field and recording why (see stock_universe_update_listener.py's
    _record_audit) keeps that fact visible instead of hidden.
    """
    clean = {}
    dropped = []

    for column, value in fields.items():
        try:
            numeric_value = float(value)
        except (TypeError, ValueError):
            clean[column] = value
            continue

        if math.isnan(numeric_value) or math.isinf(numeric_value):
            dropped.append((column, value, "non-finite value (inf/nan)"))
            continue

        limits = DECIMAL_COLUMN_LIMITS.get(column)
        if limits is not None:
            precision, scale = limits
            max_abs = 10 ** (precision - scale)
            if abs(numeric_value) >= max_abs:
                dropped.append((column, value, f"exceeds DECIMAL({precision},{scale}) range (abs value must be < 10^{precision - scale})"))
                continue

        clean[column] = value

    return clean, dropped


def update_stock_fundamentals(conn, isin_number, exchange, fields):
    """
    Updates the SINGLE stock_universe row matching WHERE isin_number = %s
    AND exchange = %s -- nothing else. Replaces the earlier
    update_stock_fundamentals_by_isin(), which wrote common fields to
    every row sharing an isin plus a separate exchange-scoped pass --
    that whole "common vs exchange-specific" concept is gone. Every
    field written here is scoped to exactly one row, always, no
    exceptions. If a dual-listed company's NSE and BSE rows both need
    updating, this gets called TWICE, once per exchange, by the caller
    -- see stock_universe_update_listener.py's _worker.

    Built dynamically, column by column, since not every stock returns
    every field from every source -- a stock missing priceToBook, say,
    should leave that column untouched, not overwrite it with NULL.

    fields keys must already be real stock_universe column names --
    this function does not know or care about any source's own field
    names.

    Every value is passed through normalize_field_value() (numpy ->
    native) plus an additional Json-wrap step HERE specifically (not in
    normalize_field_value(), see that function's own docstring for why)
    for list/dict values -- psycopg2 does NOT automatically adapt a
    plain Python list/dict to Postgres's JSON column type the way it
    does for strings/numbers/dates.

    CONFIRMED REAL via a full production run: yfinance/pandas ratios
    occasionally come back as a non-finite float (inf/-inf/nan, e.g. a
    P/E computed with zero EPS) or as a finite-but-extreme value (a
    penny-stock/near-zero-equity ISIN producing a ratio in the hundreds
    of millions) that overflows its column's DECIMAL(precision, scale)
    range. Before this was handled, ANY one such field in the dict
    failed the ENTIRE single-statement UPDATE below, silently costing
    every other -- otherwise perfectly good -- field for that same
    (isin, exchange) row, not just the one bad value. fields is now run
    through _sanitize_numeric_fields() first: anything unsafe is
    dropped from what gets written, never coerced/clamped/rounded into
    something fake. If sanitization leaves nothing left to write at
    all, no UPDATE is issued.

    Returns the list of (column, original_value, reason) tuples for
    everything _sanitize_numeric_fields() dropped -- the caller (see
    _worker) is responsible for logging/auditing these; this function
    only ever decides what NOT to write, it never writes a dropped
    field's value under any circumstance.

    Caller is responsible for commit()/rollback() -- this only executes
    the UPDATE, matching db_client's own autocommit=False convention.
    """
    if not fields:
        return []

    fields, dropped = _sanitize_numeric_fields(fields)
    if not fields:
        return dropped

    def _prepare_for_sql(value):
        value = normalize_field_value(value)
        if isinstance(value, (list, dict)):
            return Json(value)
        return value

    set_clause = ", ".join(f"{column} = %s" for column in fields.keys())
    values = [_prepare_for_sql(v) for v in fields.values()] + [isin_number, exchange]
    query = f"""
        UPDATE stock_universe
           SET {set_clause}, updated_at = CURRENT_TIMESTAMP
         WHERE isin_number = %s AND exchange = %s
    """
    try:
        with conn.cursor() as cur:
            cur.execute(query, values)
    except Exception as e:
        raise StockUniversePersistenceError(f"Failed to update fundamentals for isin={isin_number}, exchange={exchange}: {e}")

    return dropped


def record_enrichment_run(conn, last_processed_metadata_id, total_stocks, stocks_enriched, stocks_failed, status, started_at):
    """
    Inserts exactly ONE new row -- this table is append-only history,
    never updated in place, matching stock_universe_metadata's own
    convention. Commits itself, since this is always the last thing a
    batch does. Returns the new row's id (via RETURNING), needed by
    the caller to tie any stock_universe_enrichment_audit rows for this
    same run back to it via run_id -- see record_enrichment_audit_rows
    below.

    total_stocks/stocks_enriched/stocks_failed now count (isin,
    exchange) pairs, not isins and not stock_universe rows generically
    -- following the switch to per-exchange-independent enrichment,
    each side of a dual-listed company counts separately. The column
    names themselves weren't renamed (would need another migration for
    a naming-only change), but the values they hold now mean this.
    """
    query = """
        INSERT INTO stock_universe_enrichment_run
            (last_processed_metadata_id, total_stocks, stocks_enriched, stocks_failed, status, started_at, completed_at)
        VALUES (%s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
        RETURNING id
    """
    try:
        with conn.cursor() as cur:
            cur.execute(query, (last_processed_metadata_id, total_stocks, stocks_enriched, stocks_failed, status, started_at))
            run_id = cur.fetchone()[0]
        conn.commit()
        return run_id
    except Exception as e:
        conn.rollback()
        raise StockUniversePersistenceError(f"Failed to record enrichment run outcome: {e}")


def record_enrichment_audit_rows(conn, run_id, audit_rows):
    """
    Bulk-inserts one row per non-success (isin, exchange) side outcome
    from this run into stock_universe_enrichment_audit, tied to run_id
    via FK (001.06.00). audit_rows is a list of dicts with keys
    isin_number, exchange, outcome, reason -- see
    stock_universe_update_listener.py's _record_audit, the only
    producer of this list. ONLY ever called with EXCLUDED/NO_DATA/
    FAILED/FIELD_DROPPED rows, by design -- fully successful sides with
    nothing dropped are never written here (see 001.06.00's own table
    comment: they already have a home in stock_universe itself). A
    no-op if audit_rows is empty, rather than an error -- a clean run
    with nothing to report is the common case, not an exception.

    Commits itself, same as record_enrichment_run, since this always
    runs immediately after that in the same batch-completion step.
    """
    if not audit_rows:
        return

    query = """
        INSERT INTO stock_universe_enrichment_audit
            (run_id, isin_number, exchange, outcome, reason)
        VALUES (%s, %s, %s, %s, %s)
    """
    values = [
        (run_id, row["isin_number"], row["exchange"], row["outcome"], row["reason"])
        for row in audit_rows
    ]
    try:
        with conn.cursor() as cur:
            cur.executemany(query, values)
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise StockUniversePersistenceError(f"Failed to record enrichment audit rows for run_id={run_id}: {e}")


def set_maintenance_running(conn, is_running):
    """
    Updates the SINGLETON maintenance_status row (id=1, from 001.05.00)
    to reflect whether a maintenance job (currently just this enrichment
    listener, but the table itself is generic -- see 001.05.00's own
    comment) is running right now. A Spring Security filter on the Java
    side checks this on EVERY single request to show a global
    "maintenance in progress" banner across the whole app -- this table
    should never have more than one row, and this function always
    targets id=1 specifically.

    Setting is_running=True also refreshes started_at to now(); setting
    it False leaves started_at UNCHANGED, so it still reflects when the
    most recent job actually began.

    CRITICAL: callers MUST call this with is_running=False in a
    finally block, not just on the normal success path -- if a job
    crashes and this is never cleared, the ENTIRE APP stays locked in
    "maintenance mode" for every user, forever, until someone notices
    and fixes it manually. See stock_universe_update_listener.py's
    run_enrichment_batch for the try/finally that guarantees this.

    Deliberately only called for REAL runs, never --limit test runs --
    see run_enrichment_batch's own guard for why (a developer running a
    quick test shouldn't lock out the whole app for every user).
    """
    query = """
        UPDATE maintenance_status
           SET is_running = %s,
               started_at = CASE WHEN %s THEN CURRENT_TIMESTAMP ELSE started_at END,
               updated_at = CURRENT_TIMESTAMP
         WHERE id = 1
    """
    try:
        with conn.cursor() as cur:
            cur.execute(query, (is_running, is_running))
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise StockUniversePersistenceError(f"Failed to update maintenance status: {e}")
