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


def test_m3_file_name_date_differs_json_wins_with_note():
    d = _dir()
    _write(d, "MARKET_DAILY_BRIEF_25-OCT-2026.json", json.dumps(GOOD))     # JSON says 24-OCT
    _write(d, "MARKET_DAILY_BRIEF_25-OCT-2026.html", HTML)
    result, saved = _run(d)
    assert saved[0].trade_date == date(2026, 10, 24) and result.loaded == ["24-OCT-2026"]
    assert result.notes and "saved as 24-OCT-2026" in result.notes[0][1]
    assert saved[0].html == HTML


def test_m4_bad_json_and_missing_edition_fail():
    d = _dir()
    _write(d, "MARKET_DAILY_BRIEF_23-OCT-2026.json", "{not json", age=600)
    _write(d, "MARKET_DAILY_BRIEF_22-OCT-2026.json", json.dumps({"asOfDate": "2026-10-22"}), age=600)
    result, saved = _run(d)
    assert not saved
    reasons = " | ".join(r for _, r in result.failed)
    assert "not valid JSON" in reasons and "no edition" in reasons


def test_m10_trade_date_in_json_identifies_the_brief():
    d = _dir()
    _write(d, "MARKET_DAILY_BRIEF_28-OCT-2026.json", json.dumps({"edition": "post-close-4pm", "tradeDate": "28-OCT-2026"}))
    _write(d, "MARKET_DAILY_BRIEF_28-OCT-2026.html", HTML)
    result, saved = _run(d)
    assert saved[0].trade_date == date(2026, 10, 28) and result.loaded == ["28-OCT-2026"] and not result.notes
    # tradeDate wins over the file name too
    d = _dir()
    _write(d, "MARKET_DAILY_BRIEF_29-OCT-2026.json", json.dumps({"edition": "e", "tradeDate": "28-oct-2026", "asOfDate": "2026-10-28"}))
    result, saved = _run(d)
    assert saved[0].trade_date == date(2026, 10, 28) and result.notes


def test_m11_trade_date_and_as_of_date_disagree_fails():
    d = _dir()
    _write(d, "MARKET_DAILY_BRIEF_28-OCT-2026.json", json.dumps({"edition": "e", "tradeDate": "28-OCT-2026", "asOfDate": "2026-10-27"}),
           age=HTML_GRACE_SECONDS + 5)
    result, saved = _run(d)
    assert not saved and "disagree" in result.failed[0][1]
    _write(d, "MARKET_DAILY_BRIEF_28-OCT-2026.json", json.dumps({"edition": "e", "tradeDate": "28/10/2026"}), age=HTML_GRACE_SECONDS + 5)
    result, saved = _run(d)
    assert not saved and "not a date" in result.failed[0][1]


def test_m12_same_trade_date_twice_in_one_poll_newest_saved_last():
    d = _dir()
    # two uploads of the 28-OCT brief under different file names; the newer one must be saved last (it wins the upsert)
    _write(d, "MARKET_DAILY_BRIEF_28-OCT-2026.json", json.dumps({"edition": "e", "tradeDate": "28-OCT-2026", "title": "older"}),
           age=HTML_GRACE_SECONDS + 600)
    _write(d, "MARKET_DAILY_BRIEF_27-OCT-2026.json", json.dumps({"edition": "e", "tradeDate": "28-OCT-2026", "title": "newer"}),
           age=HTML_GRACE_SECONDS + 5)
    result, saved = _run(d)
    assert [b.title for b in saved] == ["older", "newer"] and result.loaded == ["28-OCT-2026", "28-OCT-2026"]


def test_m13_replaced_is_reported():
    d = _dir()
    _write(d, "MARKET_DAILY_BRIEF_24-OCT-2026.json", json.dumps(GOOD))
    _write(d, "MARKET_DAILY_BRIEF_24-OCT-2026.html", HTML)
    result = process_inbox(d, lambda brief: True)        # the DB said: an existing row was replaced
    assert result.replaced == ["24-OCT-2026"]


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



# TMT-MDL-BUG-0014 -- the 7 AM pre-market edition carries its date in latestTradeDate (no tradeDate / asOfDate).
PRE_MARKET = {"edition": "pre-market-7am", "latestTradeDate": "2026-11-16",
              "title": "India Market Dashboard - Morning Pre-Market & Overnight Brief",
              "reportPublishedDateTime": "Tuesday, 17 November 2026 · 7:00 AM IST"}
SAMPLE_DIR = Path(__file__).resolve().parent / "fixtures" / "market_brief"


def test_m14_latest_trade_date_identifies_the_brief():
    d = _dir()
    _write(d, "MARKET_DAILY_BRIEF_16-NOV-2026.json", json.dumps(PRE_MARKET), age=HTML_GRACE_SECONDS + 5)
    result, saved = _run(d)
    assert result.loaded == ["16-NOV-2026"] and not result.failed and not result.notes
    assert saved[0].trade_date == date(2026, 11, 16) and saved[0].edition == "pre-market-7am"


def test_m15_latest_trade_date_must_agree_with_other_dates():
    d = _dir()
    _write(d, "MARKET_DAILY_BRIEF_16-NOV-2026.json", json.dumps(dict(PRE_MARKET, asOfDate="2026-11-17")),
           age=HTML_GRACE_SECONDS + 5)
    result, saved = _run(d)
    assert not saved and "latestTradeDate" in result.failed[0][1] and "disagree" in result.failed[0][1]
    # tradeDate still comes first when both are present and agree
    _write(d, "MARKET_DAILY_BRIEF_16-NOV-2026.json", json.dumps(dict(PRE_MARKET, tradeDate="16-NOV-2026")),
           age=HTML_GRACE_SECONDS + 5)
    result, saved = _run(d)
    assert saved and saved[0].trade_date == date(2026, 11, 16)


def test_m16_no_date_at_all_names_all_three_keys():
    d = _dir()
    _write(d, "MARKET_DAILY_BRIEF_16-NOV-2026.json", json.dumps({"edition": "pre-market-7am"}), age=HTML_GRACE_SECONDS + 5)
    result, saved = _run(d)
    assert not saved and "latestTradeDate" in result.failed[0][1]


def test_m17_real_pre_market_sample_loads():
    # the real 16-NOV-2026 file that prod rejected; pins the new format so a loader change can't break it again
    b = build_brief(SAMPLE_DIR / "MARKET_DAILY_BRIEF_16-NOV-2026.json")
    assert b.trade_date == date(2026, 11, 16) and b.edition == "pre-market-7am" and b.note is None
    payload = json.loads(b.payload_text)
    for key in ("reportPublishedDateTime", "overnightDevelopments", "sectorsInFocusToday", "derivativesTracker"):
        assert key in payload, key

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
