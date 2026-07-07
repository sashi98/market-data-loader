# core/bhavcopy_parser.py
#
# Parses a single BhavCopy CSV file into a list of row dicts matching the
# `bhav_copy` table's columns. Supports THREE header formats, auto-
# detected from the file's own header row -- no need for the caller to
# say which one it expects:
#
#   v2 (UDiFF, current -- NSE + BSE dates >= 01-Jan-2024):
#     TradDt,BizDt,Sgmt,Src,FinInstrmTp,FinInstrmId,ISIN,TckrSymb,SctySrs,
#     XpryDt,FininstrmActlXpryDt,StrkPric,OptnTp,FinInstrmNm,OpnPric,
#     HghPric,LwPric,ClsPric,LastPric,PrvsClsgPric,UndrlygPric,SttlmPric,
#     OpnIntrst,ChngInOpnIntrst,TtlTradgVol,TtlTrfVal,TtlNbOfTxsExctd,
#     SsnId,NewBrdLotQty,Rmks,Rsvd1,Rsvd2,Rsvd3,Rsvd4
#
#   v1 (legacy NSE -- NSE dates < 01-Jan-2024 only):
#     SYMBOL,SERIES,OPEN,HIGH,LOW,CLOSE,LAST,PREVCLOSE,TOTTRDQTY,
#     TOTTRDVAL,TIMESTAMP,TOTALTRADES,ISIN
#     Notable differences from v2:
#       - No Src column -- exchange is hardcoded to "NSE" for this format
#       - No TradDt column -- trade date comes from TIMESTAMP instead
#         (format "DD-MMM-YYYY", e.g. "14-DEC-2023")
#       - Trailing comma per row (stray empty column) -- harmless, simply
#         ignored since columns are looked up by name, not position
#
#   v3 (legacy BSE -- BSE dates < 01-Jan-2024 only):
#     ISIN,TckrSymb,FinInstrmId,FinInstrmNm,SctySrs,OpnPric,HghPric,
#     LwPric,ClsPric,LastPric,PrvsClsgPric,TtlTradgVol,TtlTrfVal,TradDt,
#     TtlNbOfTxsExctd,FinInstrmTp,OffclCorpActnEvtId,RptgDt,
#     TradRegnOrgn,MktTpandId,InstrmId,InstrmNm,FftyTwWkHgh,FftyTwWkLw,
#     UnitOfMeasr,SttlmPric,AvrgPric,Ccy,Rsvd01,Rsvd02,Rsvd03,Rsvd04
#     IMPORTANT: this format is almost identical to v2 (same TradDt,
#     TckrSymb, SctySrs, OpnPric/HghPric/LwPric/ClsPric/LastPric/
#     PrvsClsgPric/TtlTradgVol/TtlTrfVal/TtlNbOfTxsExctd/ISIN column
#     names) -- the ONLY meaningful difference is that Src is MISSING.
#     A naive "has TradDt -> v2" check would misdetect this as v2 and
#     silently persist exchange="" (empty string) for every row instead
#     of raising an error -- _detect_format() below specifically checks
#     for Src's absence to catch this. exchange is hardcoded to "BSE".
#
# Otherwise mirrors BhavCopyCSVParser.java + BaseCSVParser.java exactly:
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
#
# Validation (non-empty file, trade date matches the requested date) is
# done here too, since a bad file should never reach the persistence step.

import csv
from datetime import datetime
from decimal import Decimal, ROUND_HALF_UP, InvalidOperation

# -- v2 (UDiFF) header names --
HEADER_V2_TRADE_DATE = "TradDt"
HEADER_V2_ISIN = "ISIN"
HEADER_V2_SYMBOL = "TckrSymb"
HEADER_V2_SECURITY_SERIES = "SctySrs"
HEADER_V2_SOURCE = "Src"
HEADER_V2_OPEN_PRICE = "OpnPric"
HEADER_V2_HIGH_PRICE = "HghPric"
HEADER_V2_LOW_PRICE = "LwPric"
HEADER_V2_CLOSE_PRICE = "ClsPric"
HEADER_V2_LAST_PRICE = "LastPric"
HEADER_V2_PREVIOUS_CLOSE_PRICE = "PrvsClsgPric"
HEADER_V2_TOTAL_TRADING_VOLUME = "TtlTradgVol"
HEADER_V2_TOTAL_TURNOVER_VALUE = "TtlTrfVal"
HEADER_V2_TOTAL_NUMBER_OF_TRANSACTIONS = "TtlNbOfTxsExctd"

# -- v1 (legacy NSE) header names --
HEADER_V1_SYMBOL = "SYMBOL"
HEADER_V1_SERIES = "SERIES"
HEADER_V1_OPEN_PRICE = "OPEN"
HEADER_V1_HIGH_PRICE = "HIGH"
HEADER_V1_LOW_PRICE = "LOW"
HEADER_V1_CLOSE_PRICE = "CLOSE"
HEADER_V1_LAST_PRICE = "LAST"
HEADER_V1_PREVIOUS_CLOSE_PRICE = "PREVCLOSE"
HEADER_V1_TOTAL_TRADING_VOLUME = "TOTTRDQTY"
HEADER_V1_TOTAL_TURNOVER_VALUE = "TOTTRDVAL"
HEADER_V1_TIMESTAMP = "TIMESTAMP"
HEADER_V1_TOTAL_NUMBER_OF_TRANSACTIONS = "TOTALTRADES"
HEADER_V1_ISIN = "ISIN"

FORMAT_V1 = "v1"
FORMAT_V2 = "v2"
FORMAT_V3 = "v3"

# Same format list DateUtil.parseDateSafe tries, in the same order --
# used for both v1's TIMESTAMP and v2's TradDt.
DATE_FORMATS = [
    "%d-%b-%y",     # dd-MMM-yy
    "%d-%m-%Y",     # dd-MM-yyyy
    "%d/%m/%Y",     # dd/MM/yyyy
    "%Y-%m-%d",     # yyyy-MM-dd
    "%m/%d/%Y",     # MM/dd/yyyy
    "%d-%b-%Y",     # dd-MMM-yyyy
]


class BhavCopyParseError(Exception):
    """Raised when the file is empty, unreadable, has an unrecognized
    header format, or its trade date doesn't match the expected date."""
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
        #
        # EXCEPTION: v3 (legacy BSE) represents whole-number volume/trade
        # counts with a decimal suffix (e.g. "3490.00" instead of "3490") --
        # confirmed against a real BSE_EQ_BHAVCOPY file. A direct int()
        # call raises ValueError on any string containing ".", which this
        # function used to silently swallow into None -- quietly dropping
        # real data instead of raising an error. Parsing via float() first
        # handles both "3490" and "3490.00" correctly, and is a no-op for
        # v1/v2's already-clean integer strings.
        return int(float(value.strip()))
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


def _detect_format(header_lookup):
    """
    Auto-detects v1 vs v2 vs v3 from the file's own header row.

    IMPORTANT: v2 and v3 both have TradDt -- the discriminator between
    them is whether Src is ALSO present. Checking for TradDt alone would
    misdetect v3 as v2 and silently produce exchange="" for every row
    instead of raising an error (see module docstring).

    Raises BhavCopyParseError if none of the three patterns match.
    """
    has_trade_date = HEADER_V2_TRADE_DATE.lower() in header_lookup
    has_source = HEADER_V2_SOURCE.lower() in header_lookup

    if has_trade_date and has_source:
        return FORMAT_V2
    if has_trade_date and not has_source:
        return FORMAT_V3
    if HEADER_V1_SYMBOL.lower() in header_lookup and HEADER_V1_TIMESTAMP.lower() in header_lookup:
        return FORMAT_V1
    raise BhavCopyParseError(
        f"Unrecognized BhavCopy header format -- neither '{HEADER_V2_TRADE_DATE}'+'{HEADER_V2_SOURCE}' (v2/UDiFF), "
        f"'{HEADER_V2_TRADE_DATE}' without '{HEADER_V2_SOURCE}' (v3/BSE legacy), "
        f"nor '{HEADER_V1_SYMBOL}'+'{HEADER_V1_TIMESTAMP}' (v1/NSE legacy) found in headers."
    )


def _parse_row_v2(raw_row, header_lookup):
    symbol = _get_value(raw_row, header_lookup, HEADER_V2_SYMBOL)
    exchange = _get_value(raw_row, header_lookup, HEADER_V2_SOURCE)  # from file, NOT hardcoded
    series = _get_value(raw_row, header_lookup, HEADER_V2_SECURITY_SERIES)
    isin = _get_value(raw_row, header_lookup, HEADER_V2_ISIN)

    trade_date = _parse_date_safe(_get_value(raw_row, header_lookup, HEADER_V2_TRADE_DATE))

    open_price = _parse_decimal_safe(_get_value(raw_row, header_lookup, HEADER_V2_OPEN_PRICE))
    high_price = _parse_decimal_safe(_get_value(raw_row, header_lookup, HEADER_V2_HIGH_PRICE))
    low_price = _parse_decimal_safe(_get_value(raw_row, header_lookup, HEADER_V2_LOW_PRICE))
    close_price = _parse_decimal_safe(_get_value(raw_row, header_lookup, HEADER_V2_CLOSE_PRICE))
    last_price = _parse_decimal_safe(_get_value(raw_row, header_lookup, HEADER_V2_LAST_PRICE))
    prev_close_price = _parse_decimal_safe(_get_value(raw_row, header_lookup, HEADER_V2_PREVIOUS_CLOSE_PRICE))

    tot_trd_qty = _parse_int_safe(_get_value(raw_row, header_lookup, HEADER_V2_TOTAL_TRADING_VOLUME))
    tot_trd_val = _parse_decimal_safe(_get_value(raw_row, header_lookup, HEADER_V2_TOTAL_TURNOVER_VALUE))
    total_trades = _parse_int_safe(_get_value(raw_row, header_lookup, HEADER_V2_TOTAL_NUMBER_OF_TRANSACTIONS))

    return {
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
        "ltpPercentChange": _calculate_ltp_percent_change(close_price, prev_close_price),
    }


def _parse_row_v3(raw_row, header_lookup):
    """
    v3 (legacy BSE) -- same column names as v2 for every field EXCEPT
    exchange, which has no source column here and is hardcoded to "BSE".
    """
    symbol = _get_value(raw_row, header_lookup, HEADER_V2_SYMBOL)
    exchange = "BSE"  # v3 has no Src column -- hardcoded, this format is BSE-only
    series = _get_value(raw_row, header_lookup, HEADER_V2_SECURITY_SERIES)
    isin = _get_value(raw_row, header_lookup, HEADER_V2_ISIN)

    trade_date = _parse_date_safe(_get_value(raw_row, header_lookup, HEADER_V2_TRADE_DATE))

    open_price = _parse_decimal_safe(_get_value(raw_row, header_lookup, HEADER_V2_OPEN_PRICE))
    high_price = _parse_decimal_safe(_get_value(raw_row, header_lookup, HEADER_V2_HIGH_PRICE))
    low_price = _parse_decimal_safe(_get_value(raw_row, header_lookup, HEADER_V2_LOW_PRICE))
    close_price = _parse_decimal_safe(_get_value(raw_row, header_lookup, HEADER_V2_CLOSE_PRICE))
    last_price = _parse_decimal_safe(_get_value(raw_row, header_lookup, HEADER_V2_LAST_PRICE))
    prev_close_price = _parse_decimal_safe(_get_value(raw_row, header_lookup, HEADER_V2_PREVIOUS_CLOSE_PRICE))

    tot_trd_qty = _parse_int_safe(_get_value(raw_row, header_lookup, HEADER_V2_TOTAL_TRADING_VOLUME))
    tot_trd_val = _parse_decimal_safe(_get_value(raw_row, header_lookup, HEADER_V2_TOTAL_TURNOVER_VALUE))
    total_trades = _parse_int_safe(_get_value(raw_row, header_lookup, HEADER_V2_TOTAL_NUMBER_OF_TRANSACTIONS))

    return {
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
        "ltpPercentChange": _calculate_ltp_percent_change(close_price, prev_close_price),
    }


def _parse_row_v1(raw_row, header_lookup):
    symbol = _get_value(raw_row, header_lookup, HEADER_V1_SYMBOL)
    exchange = "NSE"  # v1 has no Src column -- hardcoded, this format is NSE-only
    series = _get_value(raw_row, header_lookup, HEADER_V1_SERIES)
    isin = _get_value(raw_row, header_lookup, HEADER_V1_ISIN)

    trade_date = _parse_date_safe(_get_value(raw_row, header_lookup, HEADER_V1_TIMESTAMP))

    open_price = _parse_decimal_safe(_get_value(raw_row, header_lookup, HEADER_V1_OPEN_PRICE))
    high_price = _parse_decimal_safe(_get_value(raw_row, header_lookup, HEADER_V1_HIGH_PRICE))
    low_price = _parse_decimal_safe(_get_value(raw_row, header_lookup, HEADER_V1_LOW_PRICE))
    close_price = _parse_decimal_safe(_get_value(raw_row, header_lookup, HEADER_V1_CLOSE_PRICE))
    last_price = _parse_decimal_safe(_get_value(raw_row, header_lookup, HEADER_V1_LAST_PRICE))
    prev_close_price = _parse_decimal_safe(_get_value(raw_row, header_lookup, HEADER_V1_PREVIOUS_CLOSE_PRICE))

    tot_trd_qty = _parse_int_safe(_get_value(raw_row, header_lookup, HEADER_V1_TOTAL_TRADING_VOLUME))
    tot_trd_val = _parse_decimal_safe(_get_value(raw_row, header_lookup, HEADER_V1_TOTAL_TURNOVER_VALUE))
    total_trades = _parse_int_safe(_get_value(raw_row, header_lookup, HEADER_V1_TOTAL_NUMBER_OF_TRANSACTIONS))

    return {
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
        "ltpPercentChange": _calculate_ltp_percent_change(close_price, prev_close_price),
    }


def parse_bhavcopy_csv(file_path, expected_trade_date):
    """
    Parses one BhavCopy CSV file -- auto-detects v1 (legacy NSE) vs v2
    (UDiFF, current NSE + all BSE) from the file's own header row.

    file_path:            path to the CSV file
    expected_trade_date:  python date object -- must match every parsed
                           row's trade date, or BhavCopyParseError is raised

    Returns a list of row dicts (deduped by isin+symbol, first occurrence
    wins), each with keys:
        symbol, exchange, series, open, high, low, close, last, prevClose,
        totTrdQty, totTrdVal, tradeDate, totalTrades, isin, ltpPercentChange

    Raises BhavCopyParseError if the file is empty/unreadable, has an
    unrecognized header format, or if any row's trade date doesn't match
    expected_trade_date.
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

    file_format = _detect_format(header_lookup)
    if file_format == FORMAT_V2:
        parse_row = _parse_row_v2
    elif file_format == FORMAT_V3:
        parse_row = _parse_row_v3
    else:
        parse_row = _parse_row_v1

    deduped = {}  # (isin+symbol) -> row dict, first occurrence wins

    for raw_row in raw_rows:
        row_dict = parse_row(raw_row, header_lookup)
        key = (row_dict["isin"] or "") + (row_dict["symbol"] or "")
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
