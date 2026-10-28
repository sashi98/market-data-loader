# core/market_brief/loader.py -- TMT-MDL-US-0004
#
# Loads the "India Market Daily Brief" files into tmt's market_daily_brief table (TMT-US-0020).
#
# Sashikant runs the Claude scheduled task by hand; it gives two files named after the TRADE DATE the data
# covers (not the day it ran):
#     MARKET_DAILY_BRIEF_24-OCT-2026.json   schema-validated data (required)
#     MARKET_DAILY_BRIEF_24-OCT-2026.html   the rendered dashboard (optional)
# and he downloads both into <brief dir>/inbox. Each poll of market_brief_listener.py calls process_inbox():
#   - a file still being written (changed in the last STABLE_SECONDS, or a browser .crdownload/.part) waits;
#   - a .json waits up to HTML_GRACE_SECONDS for its .html (the two downloads land seconds apart), then loads
#     alone;
#   - an .html with no .json waits (it is never loaded on its own);
#   - checks: valid JSON object, "asOfDate" equals the file name's trade date, "edition" present;
#   - good -> one upsert (a re-run for the same trade date replaces the row), files moved to processed/;
#     bad  -> files moved to failed/ with <name>.reason.txt next to them.
# A browser's duplicate-download suffix ("... (1).json") is accepted; the newest file of a trade date wins.
# Nothing here touches maintenance_status: loading a brief never locks the app.

import json
import re
import shutil
import time
from dataclasses import dataclass, field
from datetime import date, datetime
from pathlib import Path

FILE_RE = re.compile(r"^MARKET_DAILY_BRIEF_(\d{2}-[A-Za-z]{3}-\d{4})(?: \(\d+\))?\.(json|html)$", re.IGNORECASE)
PARTIAL_SUFFIXES = (".crdownload", ".part", ".tmp", ".download")
STABLE_SECONDS = 10
HTML_GRACE_SECONDS = 120

UPSERT_SQL = """
    INSERT INTO market_daily_brief
        (trade_date, edition, title, payload, html, schema_version, json_file_name, html_file_name, loaded_at, updated_at)
    VALUES (%s, %s, %s, %s::jsonb, %s, %s, %s, %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
    ON CONFLICT (trade_date) DO UPDATE SET
        edition = EXCLUDED.edition,
        title = EXCLUDED.title,
        payload = EXCLUDED.payload,
        html = EXCLUDED.html,
        schema_version = EXCLUDED.schema_version,
        json_file_name = EXCLUDED.json_file_name,
        html_file_name = EXCLUDED.html_file_name,
        loaded_at = CURRENT_TIMESTAMP,
        updated_at = CURRENT_TIMESTAMP
"""


class BriefError(Exception):
    """A brief file that can't be loaded -- the reason goes into failed/<name>.reason.txt."""


@dataclass
class Brief:
    trade_date: date
    edition: str
    title: str
    payload_text: str
    html: str
    schema_version: str
    json_name: str
    html_name: str


@dataclass
class InboxResult:
    loaded: list = field(default_factory=list)   # trade dates loaded this poll ("24-OCT-2026")
    failed: list = field(default_factory=list)   # (file name, reason)
    waiting: list = field(default_factory=list)  # file names left for the next poll


def parse_trade_date(text):
    """'24-OCT-2026' (any case) -> date(2026, 10, 24); BriefError if it isn't a real date."""
    try:
        return datetime.strptime(text.title(), "%d-%b-%Y").date()
    except ValueError:
        raise BriefError(f"'{text}' in the file name is not a date like 24-OCT-2026")


def fmt_trade_date(d):
    return d.strftime("%d-%b-%Y").upper()


def match_file(name):
    """(trade date text, 'json'|'html') for a brief file name, else None."""
    m = FILE_RE.match(name)
    return (m.group(1).upper(), m.group(2).lower()) if m else None


def build_brief(json_path, html_path=None):
    """Reads and checks one trade date's files; returns a Brief or raises BriefError."""
    found = match_file(json_path.name)
    if not found or found[1] != "json":
        raise BriefError(f"{json_path.name} is not named MARKET_DAILY_BRIEF_<DD-MMM-YYYY>.json")
    trade_date = parse_trade_date(found[0])
    try:
        text = json_path.read_text(encoding="utf-8-sig")
        payload = json.loads(text)
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as e:
        raise BriefError(f"{json_path.name} is not valid JSON: {e}")
    if not isinstance(payload, dict):
        raise BriefError(f"{json_path.name} must hold one JSON object")
    as_of = payload.get("asOfDate")
    try:
        as_of_date = date.fromisoformat(str(as_of)[:10]) if as_of else None
    except ValueError:
        as_of_date = None
    if as_of_date is None:
        raise BriefError(f"{json_path.name} has no valid asOfDate (YYYY-MM-DD)")
    if as_of_date != trade_date:
        raise BriefError(f"{json_path.name}: asOfDate {as_of_date.isoformat()} does not match the trade date "
                         f"{fmt_trade_date(trade_date)} in the file name")
    edition = payload.get("edition")
    if not isinstance(edition, str) or not edition.strip():
        raise BriefError(f"{json_path.name} has no edition")
    html = None
    if html_path is not None:
        try:
            html = html_path.read_text(encoding="utf-8-sig")
        except (OSError, UnicodeDecodeError) as e:
            raise BriefError(f"{html_path.name} could not be read: {e}")
        if "<html" not in html.lower():
            raise BriefError(f"{html_path.name} does not look like an HTML page")
    title = payload.get("title")
    schema_version = payload.get("schemaVersion") or payload.get("schema_version")
    return Brief(
        trade_date=trade_date,
        edition=edition.strip()[:40],
        title=title[:200] if isinstance(title, str) else None,
        payload_text=json.dumps(payload, ensure_ascii=False),
        html=html,
        schema_version=str(schema_version)[:40] if schema_version is not None else None,
        json_name=json_path.name,
        html_name=html_path.name if html_path is not None else None,
    )


def save_brief(conn, brief):
    """Upserts one brief; commits, or rolls back and re-raises."""
    try:
        with conn.cursor() as cur:
            cur.execute(UPSERT_SQL, (brief.trade_date, brief.edition, brief.title, brief.payload_text, brief.html,
                                     brief.schema_version, brief.json_name, brief.html_name))
        conn.commit()
    except Exception:
        conn.rollback()
        raise


def _move(path, folder):
    folder.mkdir(parents=True, exist_ok=True)
    target = folder / path.name
    if target.exists():
        target = folder / f"{path.stem}_{datetime.now().strftime('%Y%m%d_%H%M%S')}{path.suffix}"
    shutil.move(str(path), str(target))
    return target


def _fail(paths, folder, reason):
    for p in paths:
        if p is not None and p.exists():
            moved = _move(p, folder)
            moved.with_name(moved.name + ".reason.txt").write_text(reason + "\n", encoding="utf-8")


def process_inbox(brief_dir, save, now=None):
    """
    One pass over <brief_dir>/inbox. `save(brief)` stores a Brief (raises on failure; a DB failure leaves the
    files in the inbox for the next poll). Returns an InboxResult.
    """
    now = time.time() if now is None else now
    inbox, processed, failed = brief_dir / "inbox", brief_dir / "processed", brief_dir / "failed"
    inbox.mkdir(parents=True, exist_ok=True)
    result = InboxResult()

    groups = {}  # trade date text -> {"json": [paths], "html": [paths]}
    for p in sorted(inbox.iterdir()):
        if not p.is_file() or p.name.startswith(".") or p.name.endswith(".reason.txt"):
            continue
        if p.name.lower().endswith(PARTIAL_SUFFIXES):
            result.waiting.append(p.name)
            continue
        found = match_file(p.name)
        if not found:
            _fail([p], failed, f"{p.name} is not named MARKET_DAILY_BRIEF_<DD-MMM-YYYY>.json / .html")
            result.failed.append((p.name, "unexpected file name"))
            continue
        groups.setdefault(found[0], {"json": [], "html": []})[found[1]].append(p)

    for trade_text, files in sorted(groups.items()):
        jsons = sorted(files["json"], key=lambda p: p.stat().st_mtime)
        htmls = sorted(files["html"], key=lambda p: p.stat().st_mtime)
        everything = jsons + htmls
        if any(now - p.stat().st_mtime < STABLE_SECONDS for p in everything):
            result.waiting.extend(p.name for p in everything)
            continue
        if not jsons:
            result.waiting.extend(p.name for p in htmls)  # the .json may still be downloading
            continue
        json_path = jsons[-1]
        html_path = htmls[-1] if htmls else None
        if html_path is None and now - json_path.stat().st_mtime < HTML_GRACE_SECONDS:
            result.waiting.append(json_path.name)
            continue
        older = jsons[:-1] + htmls[:-1]
        try:
            brief = build_brief(json_path, html_path)
        except BriefError as e:
            _fail(everything, failed, str(e))
            result.failed.append((json_path.name, str(e)))
            continue
        save(brief)
        for p in [json_path, html_path] + older:
            if p is not None and p.exists():
                _move(p, processed)
        result.loaded.append(trade_text)
    return result
