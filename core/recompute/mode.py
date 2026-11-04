# core/recompute/mode.py -- TMT-MDL-US-0005
#
# mode_of_recompute: how a scheduler step recomputes its table.
#   FULL         every stock, whole history (the behaviour before TMT-MDL-US-0005).
#   INCREMENTAL  list-A stocks: only the new day(s), continuing from each stock's last stored row;
#                list-B stocks (history changed: corporate action, new stock, back-dated data, forced,
#                inconsistent state): whole history, those stocks only.
#   SHADOW       FULL is what gets written (TMT keeps showing trusted values); the INCREMENTAL result is
#                computed too, in memory, and compared with what FULL wrote. One recompute_shadow_check row
#                and one verdict log line per step / exchange / run. Used on dev to prove a step before
#                switching it to INCREMENTAL.
#
# One setting per step in config/.env (managed by tmt-env-setup). No default: a missing or unknown value
# stops the scheduler at startup, same as every other required key (Sashikant: no fallback defaults).

FULL = "FULL"
INCREMENTAL = "INCREMENTAL"
SHADOW = "SHADOW"
MODES = (FULL, INCREMENTAL, SHADOW)

# Step -> its config/.env key.
PRICE_ADJUSTMENT = "PRICE_ADJUSTMENT"
WEEKLY_ROLLUP = "WEEKLY_ROLLUP"
MONTHLY_ROLLUP = "MONTHLY_ROLLUP"
RSI14D = "RSI14D"
RSI14W = "RSI14W"
RSI14M = "RSI14M"

ENV_KEYS = {
    PRICE_ADJUSTMENT: "MDL_MODE_OF_RECOMPUTE_PRICE_ADJUSTMENT",
    WEEKLY_ROLLUP: "MDL_MODE_OF_RECOMPUTE_WEEKLY_ROLLUP",
    MONTHLY_ROLLUP: "MDL_MODE_OF_RECOMPUTE_MONTHLY_ROLLUP",
    RSI14D: "MDL_MODE_OF_RECOMPUTE_RSI14D",
    RSI14W: "MDL_MODE_OF_RECOMPUTE_RSI14W",
    RSI14M: "MDL_MODE_OF_RECOMPUTE_RSI14M",
}


class ModeOfRecomputeError(Exception):
    """A mode_of_recompute setting is missing or not one of FULL / INCREMENTAL / SHADOW."""
    pass


def parse_mode(value, key="mode_of_recompute"):
    mode = (value or "").strip().upper()
    if mode not in MODES:
        raise ModeOfRecomputeError(f"{key}={value!r} -- must be one of {', '.join(MODES)}.")
    return mode


def mode_of_recompute(env_values, step):
    """The configured mode for one step (PRICE_ADJUSTMENT / WEEKLY_ROLLUP / MONTHLY_ROLLUP)."""
    key = ENV_KEYS[step]
    return parse_mode(env_values.get(key), key)


def validate_all(env_values):
    """Parse every step's setting once at startup, so a bad value fails fast with every problem listed."""
    problems = []
    for step, key in ENV_KEYS.items():
        try:
            parse_mode(env_values.get(key), key)
        except ModeOfRecomputeError as e:
            problems.append(str(e))
    if problems:
        raise ModeOfRecomputeError("; ".join(problems))
