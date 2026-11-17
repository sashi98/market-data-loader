# tests/probe_bse_corpact_403_READONLY.py -- TMT-MDL-BUG-0008 (read-only: GET requests only, nothing is stored)
#
# BSE's corporate actions CSV export returns HTTP 403 on every scheduler run since at least 04-Oct-2026. This tries
# the same request with different header / cookie combinations and prints which (if any) gets a 200 with CSV rows.
#
# Run from the market-data-loader folder:   python tests/probe_bse_corpact_403_READONLY.py

import requests

URL = "https://api.bseindia.com/BseIndiaAPI/api/CorpactCSVDownload/w"
JSON_URL = "https://api.bseindia.com/BseIndiaAPI/api/DefaultData/w"
PAGE = "https://www.bseindia.com/corporates/corporate_act.aspx"
PARAMS = {"scripcode": " ", "Fdate": "20261101", "TDate": "20261117", "Purposecode": "", "strSearch": "S",
          "ddlindustrys": "", "ddlcategorys": "E", "segment": "0"}
CHROME = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
          "Chrome/130.0.0.0 Safari/537.36")
SCHEDULER = {"User-Agent": CHROME, "Accept": "application/json, text/plain, */*", "Accept-Language": "en-US,en;q=0.5",
             "Referer": PAGE}
BROWSER = {"User-Agent": CHROME, "Accept": "application/json, text/plain, */*", "Accept-Language": "en-US,en;q=0.9",
           "Referer": "https://www.bseindia.com/", "Origin": "https://www.bseindia.com",
           "Sec-Fetch-Site": "same-site", "Sec-Fetch-Mode": "cors", "Sec-Fetch-Dest": "empty",
           "sec-ch-ua": '"Chromium";v="130", "Google Chrome";v="130", "Not?A_Brand";v="99"',
           "sec-ch-ua-mobile": "?0", "sec-ch-ua-platform": '"Windows"'}


def show(name, r):
    body = r.content[:160].decode("utf-8", "replace").replace("\n", " | ")
    print(f"{name:<38} HTTP {r.status_code}  {r.headers.get('content-type', '')[:40]:<40} {len(r.content):>8} bytes  {body}")


def try_get(name, url, headers, session=None):
    try:
        r = (session or requests).get(url, headers=headers, params=PARAMS, timeout=(10, 60))
        show(name, r)
    except requests.RequestException as e:
        print(f"{name:<38} ERROR {e}")


print("1. as the scheduler sends it today")
try_get("csv  scheduler headers", URL, SCHEDULER)
print("2. browser-like headers (Origin, Sec-Fetch-*, client hints)")
try_get("csv  browser headers", URL, BROWSER)
print("3. browser headers + cookies from opening BSE's corporate actions page first")
s = requests.Session()
try:
    home = s.get(PAGE, headers={"User-Agent": CHROME, "Accept": "text/html,*/*"}, timeout=(10, 60))
    print(f"   page warm-up HTTP {home.status_code}, cookies: {', '.join(s.cookies.keys()) or 'none'}")
except requests.RequestException as e:
    print(f"   page warm-up ERROR {e}")
try_get("csv  browser headers + cookies", URL, BROWSER, s)
print("4. the JSON widget endpoint, same headers + cookies (different URL, for comparison)")
try_get("json browser headers + cookies", JSON_URL, BROWSER, s)

print("5. the scheduler's own function, with the windows it really asked for (403 in the logs)")
import sys  # noqa: E402
from datetime import date  # noqa: E402
from pathlib import Path  # noqa: E402
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from core.corporate_actions import corporate_actions_downloader as dl  # noqa: E402
for f, t in [(date(2026, 11, 14), date(2026, 11, 17)), (date(2026, 11, 13), date(2026, 11, 13)),
             (date(2026, 11, 7), date(2026, 11, 9)), (date(2026, 11, 1), date(2026, 11, 17)),
             (date(2024, 1, 1), date(2026, 11, 17))]:
    try:
        rows = dl.download_bse_corporate_actions_csv(f, t)
        print(f"   {f} .. {t}: OK, {len(rows)} rows")
    except Exception as e:  # noqa: BLE001
        print(f"   {f} .. {t}: {e}")

print("6. old Chrome/120 (403 until 17-Nov-2026) vs the scheduler's User-Agent now, same 14..17-Nov window")
OLD_UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
for name, ua in [("Chrome/120 (old)", OLD_UA), ("scheduler now (browser_identity)", dl.USER_AGENT)]:
    p = dict(PARAMS, Fdate="20261114", TDate="20261117")
    try:
        r = requests.get(URL, headers=dict(SCHEDULER, **{"User-Agent": ua}), params=p, timeout=(10, 60))
        show("   " + name, r)
    except requests.RequestException as e:
        print(f"   {name} ERROR {e}")
