# core/corporate_actions/corporate_actions_persistence.py
#
# Two-stage persistence for corporate actions:
#   1. persist_raw() -- idempotent upsert into corporate_actions_raw, one
#      row per (isin, exchange, action_type, ex_date). Re-running the
#      loader for an overlapping date range is always safe -- ON
#      CONFLICT DO UPDATE refreshes the raw fields rather than erroring
#      or duplicating.
#   2. reconcile() -- reads back BOTH exchanges' raw rows for the
#      (isin, action_type, ex_date) keys touched by THIS run only (not a
#      full-table reconciliation every time), and upserts the canonical
#      corporate_actions row for each, computing ADJUSTMENT_FACTOR =
#      face_value_new / face_value_old when both sides agree.
#
# bhav_copy is never touched by anything in this module -- see
# 013.02.00's changelog comment (tmt/src/main/resources/db/changelog)
# for why (raw/as-traded prices stay the source of truth; adjustment is
# applied read-time by the RSI queries via adjustment.py).
#
# Neither function commits/rolls back its own transaction --
# corporate_actions_loader.py wraps persist_raw() + reconcile() together
# in ONE transaction per run, since a raw row with no matching
# reconciliation pass would be a confusing half-done state to leave
# behind on a crash mid-run.

from decimal import Decimal, InvalidOperation

RECONCILIATION_MATCHED = "MATCHED"
RECONCILIATION_NSE_ONLY = "NSE_ONLY"
RECONCILIATION_BSE_ONLY = "BSE_ONLY"
RECONCILIATION_CONFLICT = "CONFLICT"

# Ratios within this tolerance are treated as "the same" during
# cross-exchange comparison -- accounts for harmless rounding
# differences between the two feeds' free-text ratio wording, not a
# genuine disagreement about what happened.
RATIO_MATCH_TOLERANCE = Decimal("0.0001")


class CorporateActionsPersistenceError(Exception):
    """Raised when raw persistence or reconciliation fails. Caller should roll back the transaction."""
    pass


def resolve_bse_scrip_codes(conn, bse_raw_rows):
    """
    BSE's corporate-actions API reports each row against its own numeric
    scrip_code, NOT isin -- unlike NSE, which reports isin directly.
    Confirmed against a real live response (2026-08-10): the raw dict
    has scrip_code/short_name/long_name/Ex_date/Purpose/RD_Date/exdate,
    no isin field of any casing at all.

    Resolves scrip_code -> isin via stock_universe.SECURITY_CODE -- the
    same BSE numeric scrip code already captured there for yfinance
    ticker-building (see 001.01.00's changelog comment) -- and injects
    the resolved value as a plain "isin" key into each row dict, which
    corporate_actions_parser.py's BSE field_map already knows how to
    pick up (its isin candidate list already includes a bare "isin" key).

    Rows whose scrip_code has no stock_universe match are DROPPED here,
    not raised as an error -- same "out of scope, not a failure" spirit
    as the parser's non-SPLIT/BONUS action-type filtering. An unmatched
    scrip_code typically means a debt instrument, mutual fund/ETF, or a
    delisted security stock_universe doesn't carry at all -- none of
    which this pipeline computes RSI for anyway.

    Returns (resolved_rows, unresolved_count).
    """
    if not bse_raw_rows:
        return [], 0

    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT security_code, isin_number FROM stock_universe
                 WHERE exchange IN ('BSE', 'BSE SME') AND security_code IS NOT NULL
                """
            )
            scrip_to_isin = {str(code).strip(): isin for code, isin in cur.fetchall()}
    except Exception as e:
        raise CorporateActionsPersistenceError(f"Failed to load BSE scrip_code -> isin map: {e}")

    resolved_rows = []
    unresolved_count = 0
    for raw in bse_raw_rows:
        scrip_code = raw.get("scrip_code")
        isin = scrip_to_isin.get(str(scrip_code).strip()) if scrip_code is not None else None
        if isin is None:
            unresolved_count += 1
            continue
        resolved_rows.append({**raw, "isin": isin})

    return resolved_rows, unresolved_count


def resolve_nse_symbols(conn, nse_raw_rows):
    """
    NSE's corporate-actions CSV export (download_nse_corporate_actions_csv())
    reports each row against its own trading SYMBOL, NOT isin -- unlike
    NSE's JSON endpoint (download_nse_corporate_actions()), whose
    response already includes an "isin" field directly. This only
    matters for the CSV path; the JSON path never needed this function.

    Resolves symbol -> isin via stock_universe.symbol (exchange='NSE'),
    the same table/column resolve_bse_scrip_codes() above resolves BSE's
    scrip_code through, and injects the resolved value as a plain
    "isin" key into each row dict, which
    corporate_actions_parser.py's NSE field_map already knows how to
    pick up (its isin candidate list already includes a bare "isin"
    key).

    Rows whose symbol has no stock_universe match are DROPPED here, not
    raised as an error -- same "out of scope, not a failure" spirit as
    resolve_bse_scrip_codes() (debt instruments, delisted symbols, etc.
    that stock_universe doesn't carry).

    Returns (resolved_rows, unresolved_count).
    """
    if not nse_raw_rows:
        return [], 0

    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT symbol, isin_number FROM stock_universe
                 WHERE exchange = 'NSE' AND symbol IS NOT NULL
                """
            )
            symbol_to_isin = {str(sym).strip().upper(): isin for sym, isin in cur.fetchall()}
    except Exception as e:
        raise CorporateActionsPersistenceError(f"Failed to load NSE symbol -> isin map: {e}")

    resolved_rows = []
    unresolved_count = 0
    for raw in nse_raw_rows:
        symbol = (raw.get("SYMBOL") or "").strip().upper()
        isin = symbol_to_isin.get(symbol) if symbol else None
        if isin is None:
            unresolved_count += 1
            continue
        resolved_rows.append({**raw, "isin": isin})

    return resolved_rows, unresolved_count


def upsert_corporate_actions_metadata(
    conn, exchange, from_date, to_date, run_status,
    summary=None, processing_time_ms=None, error_message=None,
):
    """
    Upserts corporate_actions_metadata by (exchange, from_date, to_date)
    -- the run-level audit record for the CSV pipeline, analogous to
    bhavcopy_persistence.py's _upsert_metadata()/bhav_copy_metadata, but
    keyed by date RANGE rather than a single trade_date (see
    015.01.00's changelog comment for why).

    summary: the dict returned by csv_pipeline.py's
    process_corporate_actions_rows()/run_csv_pipeline() (or None on a
    hard failure before a summary was ever produced) -- total_rows_downloaded,
    nse_parsed_count/bse_parsed_count (only one side is ever populated,
    since a single call is always one exchange), unresolved_isin_count,
    and newly_matched_keys are pulled out of it here so callers don't
    each have to know this table's exact column shape.

    run_status: caller-supplied ("SUCCESS" or "FAILED") -- this module
    doesn't infer success/failure itself, matching bhavcopy_persistence.py's
    convention of the caller deciding.

    Part of the caller's transaction -- does NOT commit/rollback itself,
    same contract as persist_raw()/reconcile() above.
    """
    summary = summary or {}
    total_rows_downloaded = summary.get("total_rows_downloaded", 0)
    total_rows_persisted = summary.get("nse_parsed_count", 0) + summary.get("bse_parsed_count", 0)
    unresolved_isin_count = summary.get("unresolved_isin_count", 0)
    newly_matched_count = len(summary.get("newly_matched_keys") or [])

    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id FROM corporate_actions_metadata WHERE exchange = %s AND from_date = %s AND to_date = %s",
                (exchange, from_date, to_date),
            )
            existing = cur.fetchone()

            if existing:
                cur.execute(
                    """
                    UPDATE corporate_actions_metadata
                       SET run_status = %s,
                           total_rows_downloaded = %s,
                           total_rows_persisted = %s,
                           unresolved_isin_count = %s,
                           newly_matched_count = %s,
                           processing_time_ms = %s,
                           error_message = %s,
                           updated_at = CURRENT_TIMESTAMP
                     WHERE id = %s
                    """,
                    (
                        run_status, total_rows_downloaded, total_rows_persisted,
                        unresolved_isin_count, newly_matched_count, processing_time_ms,
                        error_message, existing[0],
                    ),
                )
            else:
                cur.execute(
                    """
                    INSERT INTO corporate_actions_metadata
                        (exchange, from_date, to_date, run_status, total_rows_downloaded,
                         total_rows_persisted, unresolved_isin_count, newly_matched_count,
                         processing_time_ms, error_message, created_at, updated_at)
                    VALUES
                        (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                    """,
                    (
                        exchange, from_date, to_date, run_status, total_rows_downloaded,
                        total_rows_persisted, unresolved_isin_count, newly_matched_count,
                        processing_time_ms, error_message,
                    ),
                )
    except Exception as e:
        raise CorporateActionsPersistenceError(f"Failed to upsert corporate_actions_metadata: {e}")


def persist_raw(conn, parsed_rows, source_url):
    """
    parsed_rows: list of dicts from corporate_actions_parser.py (isin,
    symbol, exchange, action_type, ex_date, raw_ratio_text,
    face_value_old, face_value_new).

    Returns the set of (isin, action_type, ex_date) keys touched by this
    call -- pass straight into reconcile() so only the actually-new/
    -updated keys get reconciled, not the whole raw table.

    Part of the caller's transaction -- does NOT commit/rollback itself
    (see module docstring).
    """
    if not parsed_rows:
        return set()

    touched_keys = set()
    try:
        with conn.cursor() as cur:
            for row in parsed_rows:
                cur.execute(
                    """
                    INSERT INTO corporate_actions_raw
                        (isin, symbol, exchange, action_type, ex_date, raw_ratio_text,
                         face_value_old, face_value_new, source_url, ingested_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
                    ON CONFLICT (isin, exchange, action_type, ex_date) DO UPDATE SET
                        symbol = EXCLUDED.symbol,
                        raw_ratio_text = EXCLUDED.raw_ratio_text,
                        face_value_old = EXCLUDED.face_value_old,
                        face_value_new = EXCLUDED.face_value_new,
                        source_url = EXCLUDED.source_url,
                        ingested_at = CURRENT_TIMESTAMP
                    """,
                    (
                        row["isin"], row["symbol"], row["exchange"], row["action_type"],
                        row["ex_date"], row["raw_ratio_text"], row["face_value_old"],
                        row["face_value_new"], source_url,
                    ),
                )
                touched_keys.add((row["isin"], row["action_type"], row["ex_date"]))
    except Exception as e:
        raise CorporateActionsPersistenceError(f"Failed to persist raw corporate actions: {e}")

    return touched_keys


def _factor_from_face_values(face_value_old, face_value_new):
    """
    ADJUSTMENT_FACTOR = face_value_new / face_value_old -- the multiplier
    applied to every trade_date's close/prev_close STRICTLY BEFORE
    ex_date, so the isin's price series stays continuous across the
    action. Works uniformly for both action types since the parser
    normalizes BOTH split face values and bonus share ratios into the
    same (old, new) pair shape.

    Returns None if either value is missing or old is zero (can't divide).
    """
    if face_value_old is None or face_value_new is None:
        return None
    try:
        if face_value_old == 0:
            return None
        return (Decimal(face_value_new) / Decimal(face_value_old)).quantize(Decimal("0.000001"))
    except (InvalidOperation, ZeroDivisionError):
        return None


def _ratios_agree(nse_row, bse_row):
    """
    Returns True/False if both sides have a computable factor and they
    can be compared, or None if at least one side's ratio text didn't
    parse to a usable number -- None means "can't confirm agreement OR
    disagreement", not "they match".
    """
    nse_factor = _factor_from_face_values(nse_row["face_value_old"], nse_row["face_value_new"])
    bse_factor = _factor_from_face_values(bse_row["face_value_old"], bse_row["face_value_new"])
    if nse_factor is None or bse_factor is None:
        return None
    return abs(nse_factor - bse_factor) <= RATIO_MATCH_TOLERANCE


def reconcile(conn, touched_keys):
    """
    touched_keys: set of (isin, action_type, ex_date) tuples, typically
    straight from persist_raw()'s return value.

    For each key, reads back corporate_actions_raw's NSE and BSE rows (if
    present) and upserts ONE canonical corporate_actions row, per the
    RECONCILIATION_STATUS rules documented in 013.02.00's changelog
    comment: MATCHED (both exchanges reported the event, and any
    computable ratios agree), NSE_ONLY/BSE_ONLY (only one exchange
    reported it), CONFLICT (both reported it with genuinely different,
    both-computable ratios).

    Returns the list of (isin, action_type, ex_date) keys that newly
    became MATCHED (with a usable adjustment_factor) in THIS call, which
    is exactly the set corporate_actions_loader.py should trigger a
    targeted RSI reprocess for -- a key that was already MATCHED before
    this run is deliberately excluded, so re-running the loader over an
    overlapping date range doesn't keep re-triggering the same rebuild.

    Part of the caller's transaction (see persist_raw()'s docstring) --
    does not commit/rollback itself.
    """
    if not touched_keys:
        return []

    newly_matched_keys = []

    try:
        with conn.cursor() as cur:
            for isin, action_type, ex_date in touched_keys:
                cur.execute(
                    """
                    SELECT exchange, symbol, raw_ratio_text, face_value_old, face_value_new
                      FROM corporate_actions_raw
                     WHERE isin = %s AND action_type = %s AND ex_date = %s
                    """,
                    (isin, action_type, ex_date),
                )
                rows_by_exchange = {}
                for exchange, symbol, raw_ratio_text, fv_old, fv_new in cur.fetchall():
                    rows_by_exchange[exchange] = {
                        "symbol": symbol, "raw_ratio_text": raw_ratio_text,
                        "face_value_old": fv_old, "face_value_new": fv_new,
                    }

                nse_row = rows_by_exchange.get("NSE")
                bse_row = rows_by_exchange.get("BSE")

                if nse_row and bse_row:
                    agree = _ratios_agree(nse_row, bse_row)
                    nse_confirmed, bse_confirmed = True, True
                    symbol = nse_row["symbol"] or bse_row["symbol"]
                    if agree is False:
                        status = RECONCILIATION_CONFLICT
                        factor = None
                    else:
                        # agree is True, or None (one/both sides
                        # unparseable) -- either way both exchanges
                        # confirm the EVENT happened, so it's MATCHED;
                        # factor is only populated when at least one side
                        # actually produced a usable number.
                        status = RECONCILIATION_MATCHED
                        factor = (
                            _factor_from_face_values(nse_row["face_value_old"], nse_row["face_value_new"])
                            or _factor_from_face_values(bse_row["face_value_old"], bse_row["face_value_new"])
                        )
                elif nse_row:
                    status = RECONCILIATION_NSE_ONLY
                    factor = _factor_from_face_values(nse_row["face_value_old"], nse_row["face_value_new"])
                    nse_confirmed, bse_confirmed = True, False
                    symbol = nse_row["symbol"]
                elif bse_row:
                    status = RECONCILIATION_BSE_ONLY
                    factor = _factor_from_face_values(bse_row["face_value_old"], bse_row["face_value_new"])
                    nse_confirmed, bse_confirmed = False, True
                    symbol = bse_row["symbol"]
                else:
                    # Shouldn't happen -- a touched key always came from
                    # at least one freshly-persisted raw row -- but skip
                    # rather than crash the whole reconciliation batch.
                    continue

                cur.execute(
                    """
                    SELECT id, reconciliation_status FROM corporate_actions
                     WHERE isin = %s AND ex_date = %s AND action_type = %s
                    """,
                    (isin, ex_date, action_type),
                )
                existing = cur.fetchone()
                was_already_matched = existing is not None and existing[1] == RECONCILIATION_MATCHED

                if existing:
                    cur.execute(
                        """
                        UPDATE corporate_actions
                           SET symbol = %s, adjustment_factor = %s,
                               nse_confirmed = %s, bse_confirmed = %s,
                               reconciliation_status = %s, updated_at = CURRENT_TIMESTAMP
                         WHERE id = %s
                        """,
                        (symbol, factor, nse_confirmed, bse_confirmed, status, existing[0]),
                    )
                else:
                    cur.execute(
                        """
                        INSERT INTO corporate_actions
                            (isin, symbol, action_type, ex_date, adjustment_factor,
                             nse_confirmed, bse_confirmed, reconciliation_status, created_at)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
                        """,
                        (isin, symbol, action_type, ex_date, factor, nse_confirmed, bse_confirmed, status),
                    )

                if status == RECONCILIATION_MATCHED and factor is not None and not was_already_matched:
                    newly_matched_keys.append((isin, action_type, ex_date))
    except Exception as e:
        raise CorporateActionsPersistenceError(f"Reconciliation failed: {e}")

    return newly_matched_keys
