# tests/test_browser_identity.py -- TMT-MDL-BUG-0008
# BSE's firewall rejected the scheduler's Chrome/120 User-Agent (HTTP 403 "Access Denied" on every BSE corporate
# actions request); every downloader must send the one shared, current User-Agent, and a firewall 403 is named.
# Run: python tests/test_browser_identity.py (or pytest)

import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from core.browser_identity import BROWSER_USER_AGENT, is_firewall_block  # noqa: E402
from core.bhavcopy import bhavcopy_downloader  # noqa: E402
from core.corporate_actions import corporate_actions_downloader  # noqa: E402


class Resp:
    def __init__(self, status, text):
        self.status_code, self.text = status, text


def test_b1_every_downloader_sends_the_shared_user_agent():
    assert bhavcopy_downloader.USER_AGENT == BROWSER_USER_AGENT
    assert corporate_actions_downloader.USER_AGENT == BROWSER_USER_AGENT


def test_b2_not_the_rejected_chrome_120():
    version = int(re.search(r"Chrome/(\d+)", BROWSER_USER_AGENT).group(1))
    assert version >= 130, "BSE answered Chrome/120 with 403 Access Denied (17-Nov-2026)"


def test_b3_firewall_403_is_recognised():
    page = "<HTML><HEAD>\n<TITLE>Access Denied</TITLE>\n</HEAD><BODY>\n<H1>Access Denied</H1>"
    assert is_firewall_block(Resp(403, page)) is True
    assert is_firewall_block(Resp(403, '{"error": "forbidden"}')) is False
    assert is_firewall_block(Resp(200, page)) is False
    assert is_firewall_block(None) is False


# 18-Nov-2026: from the prod container BSE answered 403 to plain requests with any User-Agent (TLS fingerprint);
# curl_cffi impersonating Chrome got the CSV. browser_get() uses curl_cffi when installed, else requests.
import requests  # noqa: E402
from core.browser_identity import browser_get, IMPERSONATE  # noqa: E402


class FakeCurl:
    def __init__(self, fail=None):
        self.calls, self.fail = [], fail

    def get(self, url, **kw):
        self.calls.append((url, kw))
        if self.fail:
            raise self.fail
        return Resp(200, "Security Code,Security Name")


def test_b4_curl_cffi_impersonates_chrome_and_leaves_the_user_agent_to_it():
    curl = FakeCurl()
    r = browser_get("https://api.bseindia.com/x", headers={"User-Agent": "old", "Referer": "https://www.bseindia.com/"},
                    params={"a": 1}, timeout=(10, 60), _curl=curl)
    url, kw = curl.calls[0]
    assert r.status_code == 200 and kw["impersonate"] == IMPERSONATE == "chrome"
    assert "User-Agent" not in kw["headers"] and kw["headers"]["Referer"] == "https://www.bseindia.com/"
    assert kw["params"] == {"a": 1} and kw["timeout"] == 70


def test_b5_curl_cffi_errors_surface_as_requests_errors():
    try:
        browser_get("https://api.bseindia.com/x", _curl=FakeCurl(fail=RuntimeError("curl: (28) timeout")))
        assert False, "should raise"
    except requests.exceptions.RequestException as e:
        assert "timeout" in str(e)


def test_b6_falls_back_to_requests_with_the_shared_user_agent():
    seen = {}
    original = requests.get

    def fake_get(url, **kw):
        seen.update(kw)
        return Resp(200, "ok")
    requests.get = fake_get
    try:
        import core.browser_identity as bi
        real = bi._curl_cffi
        bi._curl_cffi = lambda: None
        try:
            browser_get("https://api.bseindia.com/x", headers={"Referer": "r"})
        finally:
            bi._curl_cffi = real
    finally:
        requests.get = original
    assert seen["headers"]["User-Agent"] == BROWSER_USER_AGENT and seen["headers"]["Referer"] == "r"


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
    print("ALL PASSED" if not failures else f"{failures} FAILED")
    sys.exit(1 if failures else 0)
