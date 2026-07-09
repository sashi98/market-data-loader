# core/trading_calendar.py
#
# Builds the trading date list by walking backward from to_date, skipping
# weekends (Sat/Sun) and holidays, until num_days trading dates are
# collected. Holidays are fetched lazily, one year at a time, via
# holiday_client -- only the year(s) actually walked into are fetched.

from datetime import datetime, timedelta

from core.holiday_client import get_holiday_dates_for_year, HolidayFetchError

SATURDAY = 5
SUNDAY = 6

# BhavCopy availability cutoff -- see compute_default_to_date() below.
# Before this hour, NSE/BSE's file for today is not reliably published
# yet; at or after it, today's file is very likely available. 8 PM is
# the safer side of the range (6 PM is the earliest it's ever sure NOT
# to be ready, but availability after that varies day to day).
BHAVCOPY_AVAILABILITY_CUTOFF_HOUR = 20


class TradingCalendarError(Exception):
    """Raised when the trading date range cannot be computed."""
    pass


def compute_default_to_date(now=None):
    """
    Default to_date suggested at the loader's to_date prompt -- a plain
    CALENDAR-day default (literal today or yesterday), deliberately NOT
    trading-day/holiday-aware. Weekend/holiday skipping still happens
    separately and backward from whatever to_date ends up being used
    (this default, or whatever the user types instead), via
    compute_trading_date_range() below -- the two are independent.

    Before BHAVCOPY_AVAILABILITY_CUTOFF_HOUR: default to yesterday.
    At or after it: default to today.

    `now` is injectable (a datetime) for testing; defaults to the real
    current time.
    """
    now = now or datetime.now()
    if now.hour >= BHAVCOPY_AVAILABILITY_CUTOFF_HOUR:
        return now.date()
    return now.date() - timedelta(days=1)


def compute_trading_date_range(to_date, num_days, tmt_app_base_url, jwt_token):
    """
    Walks backward from to_date (inclusive), skipping weekends and holidays,
    until num_days trading dates are collected.

    Returns a dict:
        from_date           -- earliest date in the range
        trading_date_list    -- sorted ascending, exactly num_days dates
        weekend_count         -- number of Sat/Sun dates skipped
        holiday_count          -- number of holiday dates skipped (that
                                   were not already counted as a weekend)

    Raises TradingCalendarError if holidays can't be fetched for a year
    encountered along the way.

    NOTE: kept for any future caller that wants "last N trading days" --
    the loaders' own prompts use compute_trading_date_range_between()
    below instead, since typing an exact from_date is more intuitive
    than typing a day count.
    """
    if num_days <= 0:
        raise TradingCalendarError(f"num_days must be positive, got: {num_days}")

    collected = []
    current = to_date
    holiday_cache_by_year = {}
    weekend_count = 0
    holiday_count = 0

    while len(collected) < num_days:
        year = current.year
        if year not in holiday_cache_by_year:
            try:
                holiday_cache_by_year[year] = get_holiday_dates_for_year(
                    tmt_app_base_url, jwt_token, year
                )
            except HolidayFetchError as e:
                raise TradingCalendarError(
                    f"Failed to fetch holidays for year {year}: {e}"
                )

        is_weekend = current.weekday() in (SATURDAY, SUNDAY)
        is_holiday = current in holiday_cache_by_year[year]

        if is_weekend:
            weekend_count += 1
        elif is_holiday:
            # Only count as a holiday if it isn't already a weekend --
            # avoids double-counting a holiday that falls on Sat/Sun.
            holiday_count += 1
        else:
            collected.append(current)

        current -= timedelta(days=1)

    collected.sort()
    from_date = collected[0]

    return {
        "from_date": from_date,
        "trading_date_list": collected,
        "weekend_count": weekend_count,
        "holiday_count": holiday_count,
    }


def compute_trading_date_range_between(from_date, to_date, tmt_app_base_url, jwt_token):
    """
    Walks backward from to_date down to from_date (BOTH inclusive),
    skipping weekends and holidays, collecting every trading day actually
    in that range -- no target day-COUNT involved, unlike
    compute_trading_date_range() above. The caller supplies both ends of
    the range explicitly.

    Returns a dict:
        from_date        -- same value passed in (kept for symmetry with
                             compute_trading_date_range()'s return shape --
                             NOT recomputed, even if from_date itself
                             happens to be a weekend/holiday and so has no
                             matching row in trading_date_list)
        trading_date_list -- sorted ascending, every trading day strictly
                             between from_date and to_date (inclusive)
        weekend_count     -- number of Sat/Sun dates skipped
        holiday_count     -- number of holiday dates skipped (that were
                             not already counted as a weekend)

    Raises TradingCalendarError if from_date is after to_date, or if
    holidays can't be fetched for a year encountered along the way.
    """
    if from_date > to_date:
        raise TradingCalendarError(
            f"from_date ({from_date}) must not be after to_date ({to_date})."
        )

    collected = []
    current = to_date
    holiday_cache_by_year = {}
    weekend_count = 0
    holiday_count = 0

    while current >= from_date:
        year = current.year
        if year not in holiday_cache_by_year:
            try:
                holiday_cache_by_year[year] = get_holiday_dates_for_year(
                    tmt_app_base_url, jwt_token, year
                )
            except HolidayFetchError as e:
                raise TradingCalendarError(
                    f"Failed to fetch holidays for year {year}: {e}"
                )

        is_weekend = current.weekday() in (SATURDAY, SUNDAY)
        is_holiday = current in holiday_cache_by_year[year]

        if is_weekend:
            weekend_count += 1
        elif is_holiday:
            holiday_count += 1
        else:
            collected.append(current)

        current -= timedelta(days=1)

    collected.sort()

    return {
        "from_date": from_date,
        "trading_date_list": collected,
        "weekend_count": weekend_count,
        "holiday_count": holiday_count,
    }
