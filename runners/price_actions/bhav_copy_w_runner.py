# runners/price_actions/bhav_copy_w_runner.py
#
# STEP 5 -- Weekly Bhav Copy Runner. Thin, single-purpose wrapper (SRP)
# around core/rollup/rollup_runner.py's shared blind-recompute engine
# -- see that module's own header for the full reasoning; this file
# only supplies WEEKLY's own period_type/table names.
#
# BLIND-UPSERT REDESIGN 2026-09-06 -- run() now takes
# (env_values, exchange, start_date, end_date) instead of
# (env_values, today=None) -- the scheduler now supplies exactly which
# exchange and which [start_date, end_date] window to recompute (always
# [DEFAULT_START_DATE, LATEST_TRADE_DATE] when it decides to run this
# step at all), and calls this once per exchange itself. No freshness
# check or date discovery left in this file or in rollup_runner.py.
#
# Naming note: named bhav_copy_w_runner.py (not "bhv_copy_w_runner.py")
# to match this repo's existing "bhav_copy_*" naming everywhere else
# (bhav_copy_metadata, bhav_copy_w, bhav_copy_w_metadata, etc.).

from core.rollup.period import WEEKLY
from core.rollup import rollup_runner
from core.recompute import mode as recompute_mode
from core.recompute import queue as recompute_queue

CONFIG = {
    "period_type": WEEKLY,
    "label": "week",
    "rollup_table": "bhav_copy_w",
    "metadata_table": "bhav_copy_w_metadata",
    "target": recompute_queue.TARGET_WEEKLY_ROLLUP,
    "run_audit_table": "bhav_copy_w_run_audit",
}


def run(env_values, exchange, start_date, end_date, mode_of_recompute=recompute_mode.FULL):
    """STEP 5 -- Weekly Bhav Copy Runner, for ONE exchange."""
    print(f"STEP 5 -- Weekly Bhav Copy Runner [{exchange}]")
    return rollup_runner.run(env_values, CONFIG, exchange, start_date, end_date, mode_of_recompute)
