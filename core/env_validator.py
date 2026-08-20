# core/env_validator.py
#
# Loads the shared TMT .env file (../../config/.env relative to this repo's
# root -- same file the `tmt` Spring Boot app reads via springboot3-dotenv)
# and validates that every key market-data-loader depends on is present
# and non-blank.
#
# Fails fast, before any network call or credential prompt, if anything
# is missing -- printing exactly which key(s) are absent.

import os
from pathlib import Path

REQUIRED_KEYS = [
    "SPRING_DATASOURCE_URL",
    "POSTGRES_USER",
    "POSTGRES_PASSWORD",
    "TMT_APP_BASE_URL",
    "MARKET_DATA_LOADER_DOWNLOAD_DIR",
    # Added 2026-08-19 for bhav_copy_schedule_listener.py -- the ONLY
    # listener in this repo that authenticates against tmt's REST API
    # (POST /api/data-integration/bhav-copy/{exchange}/{date} requires
    # ROLE_ADMIN + a JWT, see tmt's SecurityConfig.java). Listed here
    # (not just read ad hoc by that one script) so a missing/blank value
    # fails fast at startup with a clear message, same as every other
    # required key -- consistent with this module's own "fail fast, print
    # exactly what's missing" design, rather than that one listener
    # discovering it's missing credentials only when it tries to log in.
    # Every OTHER listener still gets these back in the resolved dict too
    # (harmless -- same as MARKET_DATA_LOADER_DOWNLOAD_DIR already being
    # required even for listeners that don't use it) since this file is
    # shared across all of them.
    "TMT_ADMIN_USER_ID",
    "TMT_ADMIN_PASSWORD",
]

# market-data-loader/core/env_validator.py -> market-data-loader/ -> app/ -> track-my-trade/
# .env lives at track-my-trade/config/.env
ENV_FILE_PATH = Path(__file__).resolve().parent.parent.parent.parent / "config" / ".env"


class EnvValidationError(Exception):
    """Raised when the .env file is missing or required keys are absent/blank."""
    pass


def _parse_env_file(path):
    """
    Minimal .env parser -- KEY=VALUE per line, '#' comments, blank lines
    ignored. No quoting/escaping support needed -- matches the simple
    style already used in config/.env.
    """
    values = {}
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if "=" not in line:
                continue
            key, _, value = line.partition("=")
            values[key.strip()] = value.strip()
    return values


def load_and_validate_env():
    """
    Loads config/.env and validates required keys.
    Returns a dict of the required key/value pairs on success.
    Raises EnvValidationError with a clear message on failure.
    """
    if not ENV_FILE_PATH.exists():
        raise EnvValidationError(
            f".env file not found at expected path: {ENV_FILE_PATH}"
        )

    all_values = _parse_env_file(ENV_FILE_PATH)

    missing = []
    resolved = {}
    for key in REQUIRED_KEYS:
        value = all_values.get(key)
        if not value:
            missing.append(key)
        else:
            resolved[key] = value

    if missing:
        raise EnvValidationError(
            "Missing or blank required key(s) in "
            f"{ENV_FILE_PATH}:\n  - " + "\n  - ".join(missing)
        )

    return resolved
