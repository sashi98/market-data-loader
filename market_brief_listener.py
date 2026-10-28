# market_brief_listener.py -- TMT-MDL-US-0004
#
# Watches the Market Daily Brief inbox and loads each trade date's MARKET_DAILY_BRIEF_<DD-MMM-YYYY>.json
# (+ .html) into tmt's market_daily_brief table, for the Dashboard's Market Brief tab (TMT-UI-US-0016).
# See core/market_brief/loader.py for the rules.
#
# Folder: DATA_MARKET_DATA_LOADER_MARKET_BRIEF_DIR in config/.env if set, else
# <root>/data/market_data_loader/market_brief (the same <root> config/.env sits in). Under it:
#   inbox/      Sashikant drops the downloaded files here
#   processed/  loaded files
#   failed/     rejected files, each with <name>.reason.txt
#
# python market_brief_listener.py           poll every POLL_INTERVAL_SECONDS, forever
# python market_brief_listener.py --once    one pass, then exit

import argparse
import logging
import os
import sys
import time
from pathlib import Path

from core.env_validator import load_and_validate_env, EnvValidationError, ENV_FILE_PATH, _parse_env_file
from core.db_client import get_connection, DbConnectionError
from core.logging_setup import start_run_logging
from core.market_brief.loader import process_inbox, save_brief

POLL_INTERVAL_SECONDS = 30
DIR_KEY = "DATA_MARKET_DATA_LOADER_MARKET_BRIEF_DIR"

logger = logging.getLogger("market_brief_listener")


def resolve_brief_dir():
    configured = os.environ.get(DIR_KEY)
    if not configured:
        try:
            configured = _parse_env_file(ENV_FILE_PATH).get(DIR_KEY)
        except OSError:
            configured = None
    if configured:
        return Path(configured)
    return ENV_FILE_PATH.parent.parent / "data" / "market_data_loader" / "market_brief"


def poll_once(env_values, brief_dir, last_waiting):
    def save(brief):
        conn = get_connection(env_values)
        try:
            save_brief(conn, brief)
        finally:
            conn.close()

    try:
        result = process_inbox(brief_dir, save)
    except DbConnectionError as e:
        logger.warning(f"  [WARNING] Database not reachable, files stay in the inbox: {e}")
        return last_waiting
    except Exception as e:
        logger.error(f"  [ERROR] Market brief pass failed, files stay in the inbox: {e}")
        return last_waiting

    for trade_date in result.loaded:
        logger.info(f"  [OK] Market brief {trade_date} saved.")
    for name, reason in result.failed:
        logger.warning(f"  [FAILED] {name}: {reason} (moved to failed/)")
    waiting = sorted(set(result.waiting))
    if waiting and waiting != last_waiting:
        logger.info(f"  Waiting for: {', '.join(waiting)}")
    return waiting


def run():
    parser = argparse.ArgumentParser(description="Market Daily Brief loader (watched folder)")
    parser.add_argument("--once", action="store_true", help="One pass over the inbox, then exit.")
    args = parser.parse_args()

    with start_run_logging("market_brief_listener"):
        try:
            env_values = load_and_validate_env()
        except EnvValidationError as e:
            logger.error(f"[FAILED] {e}")
            sys.exit(1)

        brief_dir = resolve_brief_dir()
        (brief_dir / "inbox").mkdir(parents=True, exist_ok=True)
        logger.info(f"Market brief listener -- watching {brief_dir / 'inbox'} every {POLL_INTERVAL_SECONDS}s.")

        waiting = []
        if args.once:
            poll_once(env_values, brief_dir, waiting)
            return
        try:
            while True:
                waiting = poll_once(env_values, brief_dir, waiting)
                time.sleep(POLL_INTERVAL_SECONDS)
        except KeyboardInterrupt:
            logger.info("Shutting down market brief listener.")


if __name__ == "__main__":
    run()
