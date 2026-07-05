# core/bhavcopy_parser.py
#
# Parses a single BhavCopy CSV file (NSE or BSE -- both use the same
# UDiFF-style column format) into a list of row dicts matching the
# `bhav_copy` table's columns. Mirrors BhavCopyCSVParser.java +
# BaseCSVParser.java exactly:
#
#   - Comma-delimited, fields trimmed, header names matched case-insensitively
#   - Duplicate (isin, symbol) rows within the same file -- first occurrence
#     wins, later duplicates silently dropped (LinkedHashMap put-if-absent
#     semantics in the Java parser)
#   - Numeric fields (open/high/low/close/last/prevClose) -- parsed to
#     2 decimal places, HALF_UP rounding; null (None) if blank/unparseable
#   - totTrdQty / totalTrades -- parsed as integers; None if blank/unparseable
#   - tradeDate -- tried against several formats (same list DateUtil.java
#     tries); None if none match
#   - ltpPercentChange = (close - prevClose) / prevClose * 100 -- None if
#     either is None or prevClose is zero. NOT rounded (matches Java's
#     MathContext.DECIMAL128, effectively full precision).
#   - `exchange` field comes from the CSV's own "Src" column, NOT hardcoded
#     to "NSE"/"BSE" -- matches BhavCopyCSVParser.java exactly.
#
# Validation (non-empty file, trade date matches the requested date) is
# done here too, since a bad file should never reach the persistence step.

import csv
from datetime import datetime
from decimal import Decimal, ROUND_HALF_UP, InvalidOperation

# CSV header names (case-insensitive match against actual file headers)
HEADER_TRADE_DATE = "TradDt"
HEADER_ISIN = "ISIN"
HEADER_SYMBOL = "TckrSymb"
HEADER_SECURITY_SERIES = "SctySrs"
HEADER_SOURCE = "Src"
HEADER_OPEN_PRICE = "OpnPric"
HEADER_HIGH_PRICE = "HghPric"
HEADER_LOW_PRICE = "LwPric"
HEADER_CLOSE_PRICE = "ClsPric"
HEADER_LAST_PRICE = "LastPric"
HEADER_PREVIOUS_CLOSE_PRICE = "PrvsClsgPric"
HEADER_TOTAL_TRADING_VOLUME = "TtlTradgVol"
HEADER_TOTAL_TURNOVER_VALUE = "TtlTrfVal"
HEADER_TOTAL_NUMBER_OF_TRANSACTIONS = "TtlNbOfTxsExctd"

# Same format list DateUtil.parseDateSafe tries, in the same order.
DATE_FORMATS = [
    "%d-%b-%y",     # dd-MMM-yy
    "%d-%m-%Y",     # dd-MM-yyyy
    "%d/%m/%Y",     # dd/MM/yyyy
    "%Y-%m-%d",     # yyyy-MM-dd
    "%m/%d/%Y",     # MM/dd/yyyy
    "%d-%b-%Y",     # dd-MMM-yyyy
]


class BhavCopyParseError(Exception):
    """Raised when the file is empty, unreadable, or its trade date doesn't match the expected date."""
    pass


def _parse_date_safe(value):
    if not value or not value.strip():
        return None
    value = value.strip()
    for fmt in DATE_FORMATS:
        try:
            return datetime.strptime(value, fmt).date()
        except ValueError:
            continue
    return None


def _parse_decimal_safe(value, places=2):
    if not value or not value.strip():
        return None
    try:
        return Decimal(value.strip()).quantize(Decimal(10) ** -places, rounding=ROUND_HALF_UP)
    except (InvalidOperation, ValueError):
        return None


def _parse_int_safe(value):
    if not value or not value.strip():
        return None
    try:
        # Mirrors parseLongSafe -- straight int parse, no decimal stripping
        # (unlike parseIntegerSafe/parseBigIntegerSafe, which do strip decimals --
        # the CSV parser uses parseLongSafe for these two fields).
        return int(value.strip())
    except ValueError:
        return None


def _calculate_ltp_percent_change(close, prev_close):
    if close is None or prev_close is None or prev_close == 0:
        return None
    try:
        return (close - prev_close) * Decimal(100) / prev_close
    except (InvalidOperation, ZeroDivisionError):
        return None


def _build_header_lookup(fieldnames):
    """Case-insensitive header name -> actual fieldname in the file."""
    return {name.strip().lower(): name for name in fieldnames}


def _get_value(row, header_lookup, column_name):
    actual_key = header_lookup.get(column_name.lower())
    if actual_key is None:
        return ""
    value = row.get(actual_key)
    return value.strip() if value else ""


def parse_bhavcopy_csv(file_path, expected_trade_date):
    """
    Parses one BhavCopy CSV file.

    file_path:            path to the CSV file
    expected_trade_date:  python date object -- must match every parsed
                           row's trade date, or BhavCopyParseError is raised

    Returns a list of row dicts (deduped by isin+symbol, first occurrence
    wins), each with keys:
        symbol, exchange, series, open, high, low, close, last, prevClose,
        totTrdQty, totTrdVal, tradeDate, totalTrades, isin, ltpPercentChange

    Raises BhavCopyParseError if the file is empty/unreadable, or if any
    row's trade date doesn't match expected_trade_date.
    """
    try:
        with open(file_path, "r", encoding="utf-8-sig", newline="") as f:
            reader = csv.DictReader(f)
            if reader.fieldnames is None:
                raise BhavCopyParseError(f"No header row found in {file_path}")

            header_lookup = _build_header_lookup(reader.fieldnames)
            raw_rows = list(reader)
    except FileNotFoundError:
        raise BhavCopyParseError(f"File not found: {file_path}")
    except (OSError, csv.Error) as e:
        raise BhavCopyParseError(f"Error reading {file_path}: {e}")

    if not raw_rows:
        raise BhavCopyParseError(f"No data rows found in {file_path}")

    deduped = {}  # (isin+symbol) -> row dict, first occurrence wins

    for raw_row in raw_rows:
        symbol = _get_value(raw_row, header_lookup, HEADER_SYMBOL)
        exchange = _get_value(raw_row, header_lookup, HEADER_SOURCE)
        series = _get_value(raw_row, header_lookup, HEADER_SECURITY_SERIES)
        isin = _get_value(raw_row, header_lookup, HEADER_ISIN)

        trade_date = _parse_date_safe(_get_value(raw_row, header_lookup, HEADER_TRADE_DATE))

        open_price = _parse_decimal_safe(_get_value(raw_row, header_lookup, HEADER_OPEN_PRICE))
        high_price = _parse_decimal_safe(_get_value(raw_row, header_lookup, HEADER_HIGH_PRICE))
        low_price = _parse_decimal_safe(_get_value(raw_row, header_lookup, HEADER_LOW_PRICE))
        close_price = _parse_decimal_safe(_get_value(raw_row, header_lookup, HEADER_CLOSE_PRICE))
        last_price = _parse_decimal_safe(_get_value(raw_row, header_lookup, HEADER_LAST_PRICE))
        prev_close_price = _parse_decimal_safe(_get_value(raw_row, header_lookup, HEADER_PREVIOUS_CLOSE_PRICE))

        tot_trd_qty = _parse_int_safe(_get_value(raw_row, header_lookup, HEADER_TOTAL_TRADING_VOLUME))
        tot_trd_val = _parse_decimal_safe(_get_value(raw_row, header_lookup, HEADER_TOTAL_TURNOVER_VALUE))
        total_trades = _parse_int_safe(_get_value(raw_row, header_lookup, HEADER_TOTAL_NUMBER_OF_TRANSACTIONS))

        ltp_percent_change = _calculate_ltp_percent_change(close_price, prev_close_price)

        row_dict = {
            "symbol": symbol,
            "exchange": exchange,
            "series": series,
            "open": open_price,
            "high": high_price,
            "low": low_price,
            "close": close_price,
            "last": last_price,
            "prevClose": prev_close_price,
            "totTrdQty": tot_trd_qty,
            "totTrdVal": tot_trd_val,
            "tradeDate": trade_date,
            "totalTrades": total_trades,
            "isin": isin,
            "ltpPercentChange": ltp_percent_change,
        }

        key = (isin or "") + (symbol or "")
        if key not in deduped:
            deduped[key] = row_dict

    parsed_rows = list(deduped.values())

    # -- Validate trade date matches expected_trade_date --
    mismatched = [r for r in parsed_rows if r["tradeDate"] != expected_trade_date]
    if mismatched:
        actual_dates = {str(r["tradeDate"]) for r in mismatched}
        raise BhavCopyParseError(
            f"Trade date mismatch in {file_path}: expected {expected_trade_date}, "
            f"found {actual_dates}"
        )

    return parsed_rows
