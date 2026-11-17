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
