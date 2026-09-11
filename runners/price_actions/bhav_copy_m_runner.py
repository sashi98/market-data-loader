# runners/price_actions/bhav_copy_m_runner.py
#
# STEP 6 -- Monthly Bhav Copy Runner. Thin, single-purpose wrapper
# (SRP) around core/rollup/rollup_runner.py's shared blind-recompute
# engine -- see that module's own header for the full reasoning; this
# file only supplies MONTHLY's own period_type/table names. See
# bhav_copy_w_runner.py's own header for the full context on why
# WEEKLY/MONTHLY share one engine instead of duplicating it.
#
# BLIND-UPSERT REDESIGN 2026-09-06 -- run() now takes
# (env_values, exchange, start_date, end_date) instead of
# (env_values, today=None) -- see bhav_copy_w_runner.py's own header
# for the full reasoning, identical here.
#
# Naming note: named bhav_copy_m_runner.py (not "bhv_copy_m_runner.py")
# to match this repo's existing "bhav_copy_*" naming everywhere else.

from core.rollup.period import MONTHLY
from core.rollup import rollup_runner

CONFIG = {
    "period_type": MONTHLY,
    "label": "month",
    "rollup_table": "bhav_copy_m",
    "metadata_table": "bhav_copy_m_metadata",
    "run_audit_table": "bhav_copy_m_run_audit",
}


def run(env_values, exchange, start_date, end_date):
    """STEP 6 -- Monthly Bhav Copy Runner, for ONE exchange."""
    print(f"STEP 6 -- Monthly Bhav Copy Runner [{exchange}]")
    rollup_runner.run(env_values, CONFIG, exchange, start_date, end_date)
