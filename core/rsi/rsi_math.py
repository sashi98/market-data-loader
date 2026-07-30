# core/rsi/rsi_math.py
#
# Pure RSI14 math (Wilder's smoothing) -- no I/O, no pandas dependency.
# Single source of truth for the seed/step/final-RSI formulas, shared by
# both the full-recompute path (rsi_calculator.py, vectorized over a
# whole isin's history) and the incremental single-date path
# (rsi_incremental.py, one date at a time). This is the Python-owned
# equivalent of what WilderRsiMath.java was for the abandoned Java daily
# listener -- see docs/epics/indicators-framework/
# session-handover-12-jul-2026.md for why that approach was abandoned
# (Python calls conn.commit() itself, explicitly, every time -- no
# ambient-transaction risk).
#
# Algorithm (period=14):
#   Seed: avg_gain = simple mean of the first `period` gains
#         avg_loss = simple mean of the first `period` losses
#   Every day after: avg_gain[i] = (avg_gain[i-1]*(period-1) + gain[i]) / period
#                     avg_loss[i] = (avg_loss[i-1]*(period-1) + loss[i]) / period
#                     rsi14 = 100 - 100/(1 + avg_gain/avg_loss)
#
# IMPORTANT -- compute_rsi14()'s edge cases are handled EXPLICITLY here,
# unlike rsi_calculator.py's original vectorized pandas version, which
# got them "for free" from numpy's float division silently returning
# inf/NaN instead of raising. Plain Python float division by zero raises
# ZeroDivisionError -- a real, expected case (a genuine zero-movement
# window, avg_gain == avg_loss == 0) would crash the incremental path if
# not branched explicitly. This bit the abandoned Java version too
# (WilderRsiMath.java had the same explicit branching, using BigDecimal).

RSI_PERIOD = 14


def compute_gain_loss(close, prev_close):
    """
    Returns (gain, loss) as a tuple of floats, or (None, None) if either
    input is None (typically an isin's very first ever listed day, where
    no prev_close exists yet).
    """
    if close is None or prev_close is None:
        return None, None
    delta = float(close) - float(prev_close)
    gain = delta if delta > 0 else 0.0
    loss = -delta if delta < 0 else 0.0
    return gain, loss


def seed(gains, losses):
    """
    Simple mean of exactly RSI_PERIOD gain/loss values -- an isin's first
    RSI_PERIOD trading days with a valid gain/loss. This is the ONLY
    place a Wilder average is ever created from scratch; every average
    after this point comes from step().

    Raises ValueError if either list's length is not exactly RSI_PERIOD.
    This is a defensive check, not the primary validation -- callers
    must enforce this themselves before calling seed().
    """
    if len(gains) != RSI_PERIOD or len(losses) != RSI_PERIOD:
        raise ValueError(
            f"seed() requires exactly {RSI_PERIOD} gain/loss values, "
            f"got {len(gains)} gains and {len(losses)} losses"
        )
    avg_gain = sum(gains) / RSI_PERIOD
    avg_loss = sum(losses) / RSI_PERIOD
    return avg_gain, avg_loss


def step(prior_avg_gain, prior_avg_loss, gain, loss):
    """
    Wilder's recursive step -- one day's incremental update on top of an
    already-seeded prior average. Requires prior_avg_gain/prior_avg_loss
    to be real numbers, not None -- callers must only invoke this once
    seed() has already run for the isin.
    """
    avg_gain = (prior_avg_gain * (RSI_PERIOD - 1) + gain) / RSI_PERIOD
    avg_loss = (prior_avg_loss * (RSI_PERIOD - 1) + loss) / RSI_PERIOD
    return avg_gain, avg_loss


def compute_rsi14(avg_gain, avg_loss):
    """
    rsi14 = 100 - 100/(1 + RS), RS = avg_gain/avg_loss -- including the
    two edge cases that plain Python float division does NOT handle
    safely (unlike numpy, which silently returns inf/NaN):
      - avg_gain == 0 AND avg_loss == 0 (zero net movement over the
        whole averaging window) -- returns None. Matches pandas
        producing NaN (0/0) on the full-recompute side.
      - avg_loss == 0 but avg_gain != 0 -- RSI saturates to exactly 100.
        Matches pandas producing +inf RS on the full-recompute side.

    Returns None if either input is None (still pre-seed).
    """
    if avg_gain is None or avg_loss is None:
        return None

    gain_is_zero = avg_gain == 0
    loss_is_zero = avg_loss == 0

    if gain_is_zero and loss_is_zero:
        return None
    if loss_is_zero:
        return 100.0

    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))
