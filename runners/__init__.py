# listener/__init__.py
#
# Three domain subpackages (fundamentals/, price_actions/, technicals/).
# fundamentals/ deliberately empty.
#
# UPDATED 2026-08-30 for the scheduler redesign (see
# claude/scheduler-redesign-loaders-removal-2026-08-29.md, TrackMyTrade
# project) -- loaders_main.py and loaders/ are gone; the old distinction
# ("loaders/ is menu-driven, listeners/ is always-on") no longer holds
# because there's no menu-driven path left at all. bhavcopy_scheduler_main.py
# (repo root) is now the ONLY entry point, and it calls straight into
# price_actions/bhavcopy_listener.py (STEPS 2-6) and technicals/'s
# per-indicator runner modules (STEP 8) within the SAME cycle -- there
# is no more separate, independently-polling technicals/indicators_
# listener.py process; that file (and its own root-level launcher,
# indicators_main_listener.py) were both retired to _to_delete/ as part
# of this redesign.
