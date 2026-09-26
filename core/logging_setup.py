# core/logging_setup.py
#
# Two things live here, layered on top of each other:
#
#   1. File logging (unchanged in spirit from before 2026-09-09): tees
#      sys.stdout/sys.stderr to ALSO write into a timestamped file under
#      logs/market-data-loader/ at the repo root, alongside the console.
#      One log file per RUN, named <loader_name>_<YYYY-MM-DD_HHMMSS>.log.
#      This part is what keeps ANY plain print() call -- including ones
#      in code not yet migrated to the logging module below -- captured
#      in the log file exactly as before. Nothing about this changed.
#
#   2. Log4j-style structured logging (NEW 2026-09-09): configures the
#      root logger, for the duration of the `with` block, with a
#      Formatter that renders each record as
#          09-SEP-2026 19:00:47 INFO  [bhavcopy_scheduler_main] - message
#      (our DD-MMM-YYYY HH:MM:SS convention, caps -- see core/date_format.py
#      -- not log4j's own default ISO timestamp) and points its handler
#      at sys.stdout -- which, inside the `with` block, IS the Tee from
#      #1. So a logger.info(...)/warning(...)/error(...) call automatically
#      goes to BOTH the console and the log file, through the exact same
#      path a print() call already does, with no separate FileHandler
#      needed and no risk of two independent writers interleaving into
#      the same file.
#
# Migration to logger.info()/warning()/error() calls (replacing print())
# is happening file-by-file, starting with the long-running scheduler/
# listener processes -- NOT everywhere at once. Any file not yet migrated
# keeps using plain print() and keeps working exactly as it always has,
# console AND file, via mechanism #1 alone. There is no "half-migrated"
# broken state.
#
# Usage, from any loader's run():
#     from core.logging_setup import start_run_logging
#
#     def run():
#         with start_run_logging("rsi14d_loader"):
#             ... existing print()-based steps, completely unchanged ...
#
# Usage, for a file migrated to structured logging:
#     import logging
#     logger = logging.getLogger("bhavcopy_scheduler_main")   # entry-point script: fixed name
#     # -- or, for a library/helper module imported by one -- :
#     logger = logging.getLogger(__name__)                    # e.g. "core.bhavcopy.bhavcopy_downloader"
#     ...
#     logger.info(f"...")
#     logger.warning(f"...")
#     logger.error(f"...")
#
# stderr is teed too (not just stdout) so an uncaught traceback still
# ends up in the log file, not just on the console.

import logging
import sys
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path

# TMT-MDL-BUG-0003 (2026-09-24) originally registered a warnings filter for
# pandas' "pandas only supports SQLAlchemy connectable..." UserWarning here
# (action="once", to surface it a single time per process rather than
# flooding the log on every pd.read_sql() call against a raw psycopg2
# connection). That registration was duplicated in core/db_client.py and,
# per Sashikant 2026-09-26, "once" wasn't actually deduping under this
# scheduler's concurrent indicator execution (ThreadPoolExecutor -- see
# _run_indicator() in bhavcopy_scheduler_main.py) -- CPython's "once"
# bookkeeping isn't safe against concurrent threads warning at the same
# moment. The fix (switched to action="ignore", which doesn't need any
# racy shared state) now lives solely in core/db_client.py, the module
# that actually owns the raw-connection tradeoff -- see the comment there.

# core/logging_setup.py -> core/ -> market-data-loader/ -> app/ -> track-my-trade/
LOGS_DIR = Path(__file__).resolve().parent.parent.parent.parent / "logs" / "market-data-loader"

LOG4J_STYLE_FORMAT = "%(asctime)s %(levelname)-5s [%(name)s] - %(message)s"


class _CapsDateFormatter(logging.Formatter):
    """
    Renders %(asctime)s as our DD-MMM-YYYY HH:MM:SS convention in caps
    (e.g. "09-SEP-2026 19:00:47") -- see core/date_format.py, the single
    source of truth for this format. logging.Formatter's own datefmt
    hook has no way to uppercase the result, so this overrides
    formatTime() directly instead of passing a datefmt string.
    """

    def formatTime(self, record, datefmt=None):
        # Local import to avoid a hard import-order dependency between
        # this module and core.date_format at module load time.
        from core.date_format import fmt_datetime
        return fmt_datetime(datetime.fromtimestamp(record.created))


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
        # Flush the file on every write -- the file object's default
        # buffering (an ~8KB block buffer, since it's not a tty) can sit
        # on the LAST partial chunk indefinitely once a listener goes
        # quiet (e.g. idling between poll cycles), so anything reading
        # the log file live (a tail -f, a monitoring script, another
        # process) can see it "frozen" far behind the console for a long
        # time even though nothing is actually wrong. The console side
        # doesn't need this (real terminals are already line-buffered),
        # so only the secondary/file stream is flushed here to keep the
        # extra syscall overhead minimal.
        self._secondary.flush()

    def flush(self):
        self._primary.flush()
        self._secondary.flush()


@contextmanager
def start_run_logging(loader_name):
    """
    Tees stdout and stderr to a new timestamped file under
    logs/market-data-loader/ for the duration of the `with` block, then
    restores the original streams and closes the file. ALSO configures
    the root logger (log4j-style Formatter, our DD-MMM-YYYY HH:MM:SS
    caps timestamp) with a single StreamHandler pointed at the now-teed
    sys.stdout, so any logger.info()/warning()/error() call in migrated
    code lands in both console and file too -- restored/removed on exit
    the same way the stdout/stderr swap is.

    Every print() call in not-yet-migrated code is completely
    unchanged -- it still goes to the console exactly as before, and
    now also lands in the file. Yields the log file's Path, in case the
    caller wants to reference it (e.g. to print it into a summary).
    """
    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y-%m-%d_%H%M%S")
    log_path = LOGS_DIR / f"{loader_name}_{timestamp}.log"

    log_file = open(log_path, "w", encoding="utf-8")
    original_stdout = sys.stdout
    original_stderr = sys.stderr

    sys.stdout = _Tee(original_stdout, log_file)
    sys.stderr = _Tee(original_stderr, log_file)

    root_logger = logging.getLogger()
    original_handlers = root_logger.handlers[:]
    original_level = root_logger.level
    for h in original_handlers:
        root_logger.removeHandler(h)
    structured_handler = logging.StreamHandler(stream=sys.stdout)
    structured_handler.setFormatter(_CapsDateFormatter(LOG4J_STYLE_FORMAT))
    root_logger.addHandler(structured_handler)
    root_logger.setLevel(logging.INFO)

    print(f"[LOGGING] Full output for this run is also being written to: {log_path}")

    try:
        yield log_path
    finally:
        root_logger.removeHandler(structured_handler)
        for h in original_handlers:
            root_logger.addHandler(h)
        root_logger.setLevel(original_level)
        sys.stdout = original_stdout
        sys.stderr = original_stderr
        log_file.close()
