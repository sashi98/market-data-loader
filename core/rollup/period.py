# core/rollup/period.py
#
# Calendar period-start keying for Story A's weekly/monthly OHLC rollup
# (bhav_copy_w / bhav_copy_m) -- see
# claude/user-story-weekly-monthly-ohlc-rsi-candlestick.md, Story A
# decision 1, this project's Claude Project docs.
#
# A rolled-up row's trade_date is ALWAYS the calendar period-START
# (Monday for a week, the 1st for a month) -- regardless of whether
# that calendar date itself was a trading day. This module is
# DELIBERATELY pure calendar arithmetic, with NO holiday-calendar
# lookup involved: since the rollup only ever aggregates rows that
# already exist in bhav_copy (which itself only has rows for real
# trading days), "the first/last actual trading day in the period"
# falls straight out of grouping daily rows by period_start and taking
# MIN/MAX(trade_date) within each group -- there is no need to
# separately ask "was Monday a holiday?" to get open/close right. See
# rollup_calculator.compute_rollup() for where that grouping happens.

from datetime import date, timedelta

WEEKLY = "W"
MONTHLY = "M"

PERIOD_TYPES = (WEEKLY, MONTHLY)


def period_start(trade_date, period_type):
    """
    The calendar period-start date for trade_date -- the Monday of its
    week (period_type=WEEKLY) or the 1st of its month (period_type=MONTHLY).
    """
    if period_type == WEEKLY:
        return trade_date - timedelta(days=trade_date.weekday())
    if period_type == MONTHLY:
        return trade_date.replace(day=1)
    raise ValueError(f"Unknown period_type: {period_type!r} -- expected one of {PERIOD_TYPES}")


def next_period_start(start, period_type):
    """
    The following period's start date -- e.g. the next Monday, or the
    1st of next month. `start` is assumed to already be a period-start
    date itself (callers walking forward one period at a time always
    pass one in); this is not re-validated here.
    """
    if period_type == WEEKLY:
        return start + timedelta(days=7)
    if period_type == MONTHLY:
        year = start.year + (1 if start.month == 12 else 0)
        month = 1 if start.month == 12 else start.month + 1
        return date(year, month, 1)
    raise ValueError(f"Unknown period_type: {period_type!r} -- expected one of {PERIOD_TYPES}")


def pending_period_starts(cursor, period_type, today):
    """
    Every period-start strictly after `cursor` whose period has
    ALREADY FULLY ENDED as of `today` (calendar-clock trigger, Story A
    decision 2) -- i.e. next_period_start(p) <= today. Ordered ascending
    (oldest first), so a caller catching up after downtime processes
    periods in the order they closed.

    `cursor` is itself a period-start date (bhav_copy_w_metadata /
    bhav_copy_m_metadata's own MAX(trade_date) for this exchange) --
    walking starts from the period AFTER it, never re-including cursor
    itself. Returns [] if cursor is None (nothing to catch up
    automatically -- Part 1's bulk backfill has not been run yet for
    this exchange; see the listener's own handling of that case) or if
    the very next period after cursor hasn't ended yet.
    """
    if cursor is None:
        return []

    periods = []
    p = next_period_start(cursor, period_type)
    while next_period_start(p, period_type) <= today:
        periods.append(p)
        p = next_period_start(p, period_type)
    return periods
