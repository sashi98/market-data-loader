# core/indicators/dispatch.py
#
# Registry-driven mapping from indicator_id -> the functions that
# actually do its work. This indirection is what lets a new indicator
# (Story 8's MA9, for example) get picked up by indicators_listener.py
# with ZERO changes to the listener itself -- add a row to
# indicators_registry, add an entry here, done.
#
# Each indicator provides two callables:
#   run(conn, trade_date) -- computes and writes exactly one new date.
#     Same contract as core.rsi.rsi_incremental.run_incremental_update():
#     raises on real failure, returns a summary dict on success.
#   bootstrap(conn) -- returns this indicator's own workbook table's
#     current MAX(trade_date), or None if it has no rows at all yet
#     (Part 1's bulk backfill has never run for it). Used ONLY to
#     auto-fill indicators_workbook_metadata.latest_trade_date the first
#     time an indicator runs under this framework -- its IWM row starts
#     out with latest_trade_date NULL, and the listener must not assume
#     any particular starting point (walking forward from the beginning
#     of bhav_copy_metadata's history would mean reprocessing years of
#     data one day at a time through the incremental path).

from collections import namedtuple

from core.rsi.rsi_incremental import run_incremental_update, get_current_max_date

IndicatorHandlers = namedtuple("IndicatorHandlers", ["run", "bootstrap"])

DISPATCH = {
    "rsi14d": IndicatorHandlers(run=run_incremental_update, bootstrap=get_current_max_date),
}


def get_handlers(indicator_id):
    """
    Returns the IndicatorHandlers for indicator_id, or None if this
    indicator_id has no dispatch entry yet -- e.g. it exists in
    indicators_registry (seeded ahead of time, like rsi14w/rsi14m/ma9/
    ma50) but its loader has not actually been built. Callers should
    treat a missing dispatch entry as "not ready yet", the same
    soft-skip spirit as an isin with no prior workbook row -- never as
    a failure.
    """
    return DISPATCH.get(indicator_id)
