"""
Single source of truth for how a date/datetime is turned into text for a
human to read -- log lines, print() output, error/exception messages.

Convention (per project decision, 2026-09-09, month caps added
2026-09-09): every human-facing date is DD-MMM-YYYY with MMM in caps
(e.g. "09-SEP-2026"), every human-facing datetime is
DD-MMM-YYYY HH:MM:SS the same way (e.g. "09-SEP-2026 19:00:47"). Nothing
else. This does NOT apply to:
  - external API request parameters / download URLs (NSE/BSE dictate
    their own formats there, e.g. YYYYMMDD -- not ours to change),
  - filenames (colons aren't legal in a Windows filename, so a literal
    HH:MM:SS can't go in one),
  - internal machine-read/write state (e.g. a JSON checkpoint file this
    same process reads back) -- no human ever reads that value as text.

Use fmt_date()/fmt_datetime() everywhere a date/datetime is interpolated
into a message a person will read, instead of relying on str(some_date)
(which silently gives ISO format, e.g. "2026-09-07") or .isoformat().
"""

from datetime import date, datetime

DATE_FORMAT = "%d-%b-%Y"
DATETIME_FORMAT = "%d-%b-%Y %H:%M:%S"


def fmt_date(value):
    """
    value -> "07-SEP-2026". None passes through as None (so an f-string
    still prints the word "None" rather than raising); a datetime is
    accepted too (only its date part is used, matching prior str(date)
    behavior when a datetime was accidentally handed to a date-only spot).
    """
    if value is None:
        return None
    if isinstance(value, datetime):
        value = value.date()
    if isinstance(value, date):
        return value.strftime(DATE_FORMAT).upper()
    return value


def fmt_datetime(value):
    """value -> "07-SEP-2026 14:03:21". None passes through as None."""
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.strftime(DATETIME_FORMAT).upper()
    if isinstance(value, date):
        # A bare date handed to the datetime formatter -- format it as a
        # date rather than raising, since a caller passing the wrong type
        # here shouldn't crash a log/error message.
        return value.strftime(DATE_FORMAT).upper()
    return value
