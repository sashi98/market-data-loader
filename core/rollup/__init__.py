# core/rollup -- Story A's weekly/monthly OHLC rollup
# (claude/user-story-weekly-monthly-ohlc-rsi-candlestick.md, this
# project's Claude Project docs). Shared between loaders/bhav_copy_w_loader.py,
# loaders/bhav_copy_m_loader.py, and bhav_copy_wm_rollup_listener.py so
# the weekly and monthly paths can never drift apart -- both are the
# same code, parameterized by period_type/table names, not two
# hand-copied implementations.
