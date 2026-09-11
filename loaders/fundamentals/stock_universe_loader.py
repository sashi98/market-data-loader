# loaders/fundamentals/stock_universe_loader.py
#
# market-data-loader -- Stock Universe enrichment, one-shot menu entry
# point (menu item 1 in loaders_main.py).
#
# Thin wrapper around stock_universe_update_listener.py's own
# poll_once() -- the exact same one-shot logic that file's own run()
# uses when invoked with --once (see its module docstring). Reused
# rather than duplicated: poll_once() already does the real work --
# checks whether a complete NSE+BSE bhav copy batch has landed since
# the last enrichment cursor, and if so runs one full enrichment pass
# (core/stock_universe/*), setting/clearing the maintenance_status
# banner around it for the duration. If nothing new has landed since
# the last run, it prints a short "nothing to do" message and returns
# -- never blocks waiting for one to appear, unlike the listener's own
# long-running LISTEN/NOTIFY loop.
#
# stock_universe_update_listener.py itself stays at the market-data-loader
# repo root, unmoved by this loaders/ reorg (2026-08-26) -- it is a
# real deployment dependency, not just a loaders/ menu convenience:
# tmt-env-setup/templates/docker-compose.template.yml starts it there
# directly as a long-running daemon (`python stock_universe_update_listener.py`),
# referenced by that exact top-level path.
#
# Exposes run() -- the standard entry point every loader under loaders/
# provides, so loaders_main.py (the menu launcher) can invoke it generically.
#
# Can also be run standalone for development/testing:
#   python loaders/fundamentals/stock_universe_loader.py

import sys
from pathlib import Path

# Allow `from core.xxx import yyy` and `from stock_universe_update_listener
# import ...` when run directly as a script -- repo root, same convention
# as every other loader's sys.path.insert.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from core.env_validator import load_and_validate_env, EnvValidationError
from core.logging_setup import start_run_logging
from stock_universe_update_listener import poll_once


def run():
    """Standard entry point called by loaders_main.py (or directly, standalone)."""
    with start_run_logging("stock_universe_loader"):
        try:
            env_values = load_and_validate_env()
        except EnvValidationError as e:
            print(f"  [FAILED] {e}")
            sys.exit(1)

        poll_once(env_values)


if __name__ == "__main__":
    run()
