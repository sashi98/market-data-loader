# tests/probe_icici_listing_READONLY.py -- TMT-MDL-US-0008 (read-only: no database, no files written)
# Why does ICICI's downloads API answer Python with something that is not JSON? Tries the listing call with a few
# header sets and prints status, content type and the start of each answer. Run: python tests/probe_icici_listing_READONLY.py

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from core.browser_identity import browser_post, browser_get  # noqa: E402

URL = "https://apimf.icicipruamc.com/nms/v1/downloads/files"
BODY = {"categoryId": "26a073d7-08d2-4a95-95fa-f83a4ee51e40", "schemeCategory": "", "userType": "Investor",
        "fileType": "All", "page": "1", "size": "20", "filter": [], "categoryName": "OTHERS"}
SITE = {"Origin": "https://www.icicipruamc.com", "Referer": "https://www.icicipruamc.com/"}
VARIANTS = [
    ("loader headers", {"sourceUrl": "DOWNLOADS", "env": "api", **SITE}),
    ("+ json accept/content-type", {"sourceUrl": "DOWNLOADS", "env": "api", "Accept": "application/json, text/plain, */*",
                                    "Content-Type": "application/json", **SITE}),
    ("lower-case names", {"sourceurl": "DOWNLOADS", "env": "api", "accept": "application/json", **SITE}),
    ("no site headers", {"sourceUrl": "DOWNLOADS", "env": "api"}),
]

for label, headers in VARIANTS:
    try:
        r = browser_post(URL, headers=headers, json=BODY, timeout=(15, 30))
        text = r.text if hasattr(r, "text") else r.content.decode("utf-8", "replace")
        print(f"[{label}] HTTP {r.status_code}  type={r.headers.get('content-type')}  len={len(r.content)}")
        print("    " + text[:300].replace("\n", " "))
    except Exception as e:  # noqa: BLE001
        print(f"[{label}] ERROR {e}")

try:
    r = browser_get("https://www.icicipruamc.com/media-center/downloads", timeout=(15, 30))
    print(f"[site page GET] HTTP {r.status_code}  len={len(r.content)}")
except Exception as e:  # noqa: BLE001
    print(f"[site page GET] ERROR {e}")
