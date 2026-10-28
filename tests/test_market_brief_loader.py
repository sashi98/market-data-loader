# TMT-MDL-US-0004 -- Market Daily Brief inbox rules, no database needed.
# Run: python tests/test_market_brief_loader.py   (or: python -m pytest tests/test_market_brief_loader.py)

import json
import os
import sys
import tempfile
import time
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from core.market_brief.loader import (  # noqa: E402
    process_inbox, build_brief, parse_trade_date, match_file, BriefError, HTML_GRACE_SECONDS,
)

GOOD = {"edition": "post-close-4pm", "title": "India Market Dashboard - Evening Update", "asOfDate": "2026-10-24"}
HTML = "<!doctype html><html><head><title>x</title></head><body>brief</body></html>"


def _dir():
    d = Path(tempfile.mkdtemp())
    (d / "inbox").mkdir()
    return d


def _write(folder, name, text, age=600):
    p = folder / "inbox" / name
    p.write_text(text, encoding="utf-8")
    t = time.time() - age
    os.utime(p, (t, t))
    return p


def _run(d):
    saved = []
    result = process_inbox(d, saved.append)
    return result, saved


def test_m1_names_and_dates():
    assert match_file("MARKET_DAILY_BRIEF_24-OCT-2026.json") == ("24-OCT-2026", "json")
    assert match_file("market_daily_brief_24-oct-2026.HTML") == ("24-OCT-2026", "html")
    assert match_file("MARKET_DAILY_BRIEF_24-OCT-2026 (1).json") == ("24-OCT-2026", "json")
    assert match_file("brief.json") is None
    assert parse_trade_date("24-oct-2026") == date(2026, 10, 24)
    try:
        parse_trade_date("31-FEB-2026")
        assert False, "31-FEB should fail"
    except BriefError:
        pass


def test_m2_json_and_html_are_loaded_and_moved():
    d = _dir()
    _write(d, "MARKET_DAILY_BRIEF_24-OCT-2026.json", json.dumps(GOOD))
    _write(d, "MARKET_DAILY_BRIEF_24-OCT-2026.html", HTML)
    result, saved = _run(d)
    assert result.loaded == ["24-OCT-2026"]
    b = saved[0]
    assert b.trade_date == date(2026, 10, 24) and b.edition == "post-close-4pm" and b.html == HTML
    assert b.title == "India Market Dashboard - Evening Update"
    assert json.loads(b.payload_text)["asOfDate"] == "2026-10-24"
    assert not list((d / "inbox").iterdir())
    assert sorted(p.name for p in (d / "processed").iterdir()) == [
        "MARKET_DAILY_BRIEF_24-OCT-2026.html", "MARKET_DAILY_BRIEF_24-OCT-2026.json"]


def test_m3_date_mismatch_goes_to_failed_with_reason():
    d = _dir()
    _write(d, "MARKET_DAILY_BRIEF_25-OCT-2026.json", json.dumps(GOOD))
    _write(d, "MARKET_DAILY_BRIEF_25-OCT-2026.html", HTML)
    result, saved = _run(d)
    assert not saved and len(result.failed) == 1
    reason = (d / "failed" / "MARKET_DAILY_BRIEF_25-OCT-2026.json.reason.txt").read_text(encoding="utf-8")
    assert "does not match" in reason
    assert (d / "failed" / "MARKET_DAILY_BRIEF_25-OCT-2026.html").exists()


def test_m4_bad_json_and_missing_edition_fail():
    d = _dir()
    _write(d, "MARKET_DAILY_BRIEF_23-OCT-2026.json", "{not json", age=600)
    _write(d, "MARKET_DAILY_BRIEF_22-OCT-2026.json", json.dumps({"asOfDate": "2026-10-22"}), age=600)
    result, saved = _run(d)
    assert not saved
    reasons = " | ".join(r for _, r in result.failed)
    assert "not valid JSON" in reasons and "no edition" in reasons


def test_m5_files_still_downloading_wait():
    d = _dir()
    _write(d, "MARKET_DAILY_BRIEF_24-OCT-2026.json", json.dumps(GOOD), age=1)       # just written
    _write(d, "MARKET_DAILY_BRIEF_23-OCT-2026.html", HTML)                          # .json not here yet
    _write(d, "MARKET_DAILY_BRIEF_22-OCT-2026.json.crdownload", "{")                # browser still busy
    result, saved = _run(d)
    assert not saved and not result.failed
    assert len(list((d / "inbox").iterdir())) == 3


def test_m6_json_alone_waits_for_html_then_loads():
    d = _dir()
    _write(d, "MARKET_DAILY_BRIEF_24-OCT-2026.json", json.dumps(GOOD), age=30)
    result, saved = _run(d)
    assert not saved and result.waiting == ["MARKET_DAILY_BRIEF_24-OCT-2026.json"]
    _write(d, "MARKET_DAILY_BRIEF_24-OCT-2026.json", json.dumps(GOOD), age=HTML_GRACE_SECONDS + 5)
    result, saved = _run(d)
    assert result.loaded == ["24-OCT-2026"] and saved[0].html is None


def test_m7_duplicate_download_newest_wins():
    d = _dir()
    _write(d, "MARKET_DAILY_BRIEF_24-OCT-2026.json", json.dumps({**GOOD, "title": "old"}), age=900)
    _write(d, "MARKET_DAILY_BRIEF_24-OCT-2026 (1).json", json.dumps({**GOOD, "title": "new"}), age=600)
    _write(d, "MARKET_DAILY_BRIEF_24-OCT-2026.html", HTML)
    result, saved = _run(d)
    assert saved[0].title == "new" and saved[0].json_name.endswith("(1).json")
    assert len(list((d / "processed").iterdir())) == 3


def test_m8_db_failure_keeps_files_in_inbox():
    d = _dir()
    _write(d, "MARKET_DAILY_BRIEF_24-OCT-2026.json", json.dumps(GOOD))
    _write(d, "MARKET_DAILY_BRIEF_24-OCT-2026.html", HTML)

    def boom(_):
        raise RuntimeError("db down")
    try:
        process_inbox(d, boom)
        assert False, "save error should surface"
    except RuntimeError:
        pass
    assert len(list((d / "inbox").iterdir())) == 2


def test_m9_unknown_file_goes_to_failed():
    d = _dir()
    _write(d, "notes.txt", "hello")
    result, _ = _run(d)
    assert result.failed and (d / "failed" / "notes.txt.reason.txt").exists()


if __name__ == "__main__":
    failures = 0
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            try:
                fn()
                print(f"[PASS] {name}")
            except Exception as e:  # noqa: BLE001
                failures += 1
                print(f"[FAIL] {name}: {e!r}")
    print(f"{'ALL PASSED' if not failures else f'{failures} FAILED'}")
    sys.exit(1 if failures else 0)
