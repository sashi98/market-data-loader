# core/trading_calendar.py
#
# Builds the trading date list by walking backward from to_date, skipping
# weekends (Sat/Sun) and holidays, until num_days trading dates are
# collected. Holidays are fetched lazily, one year at a time, via
# holiday_client -- only the year(s) actually walked into are fetched.

from datetime import timedelta

from core.holiday_client import get_holiday_dates_for_year, HolidayFetchError

SATURDAY = 5
SUNDAY = 6


class TradingCalendarError(Exception):
    """Raised when the trading date range cannot be computed."""
    pass


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
