# core/logging_setup.py
#
# Minimal-change file logging: console output stays exactly as-is (plain
# print() calls, completely unchanged, everywhere in every loader) -- this
# module just tees sys.stdout/sys.stderr to ALSO write into a timestamped
# file under logs/market-data-loader/ at the repo root, alongside the
# console. One log file per RUN (not a single rolling/appended file),
# named <loader_name>_<YYYY-MM-DD_HHMMSS>.log.
#
# Usage, from any loader's run():
#     from core.logging_setup import start_run_logging
#
#     def run():
#         with start_run_logging("rsi14d_loader"):
#             ... existing print()-based steps, completely unchanged ...
#
# stderr is teed too (not just stdout) so an uncaught traceback still
# ends up in the log file, not just on the console.

import sys
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path

# core/logging_setup.py -> core/ -> market-data-loader/ -> app/ -> track-my-trade/
LOGS_DIR = Path(__file__).resolve().parent.parent.parent.parent / "logs" / "market-data-loader"


class _Tee:
    """Writes every write()/flush() call through to TWO underlying
    streams -- the original console stream, and the log file -- so
    nothing already going to the console is lost or changed, it's just
    ALSO captured in the file."""

    def __init__(self, primary, secondary):
        self._primary = primary
        self._secondary = secondary

    def write(self, data):
        self._primary.write(data)
        self._secondary.write(data)

    def flush(self):
        self._primary.flush()
        self._secondary.flush()


@contextmanager
def start_run_logging(loader_name):
    """
    Tees stdout and stderr to a new timestamped file under
    logs/market-data-loader/ for the duration of the `with` block, then
    restores the original streams and closes the file.

    Every print() call in the wrapped code is completely unchanged --
    it still goes to the console exactly as before, and now also lands
    in the file. Yields the log file's Path, in case the caller wants to
    reference it (e.g. to print it into a summary).
    """
    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y-%m-%d_%H%M%S")
    log_path = LOGS_DIR / f"{loader_name}_{timestamp}.log"

    log_file = open(log_path, "w", encoding="utf-8")
    original_stdout = sys.stdout
    original_stderr = sys.stderr

    sys.stdout = _Tee(original_stdout, log_file)
    sys.stderr = _Tee(original_stderr, log_file)

    print(f"[LOGGING] Full output for this run is also being written to: {log_path}")

    try:
        yield log_path
    finally:
        sys.stdout = original_stdout
        sys.stderr = original_stderr
        log_file.close()
