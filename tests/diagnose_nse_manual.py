# -----------------------------------------------------------
# tests/diagnose_nse_manual.py
# Manual diagnostic: fetch stocks from the NSE official API exactly the way
# stock_universe enrichment does, and print what NSE actually sends back.
#
# Why: on 2026-10-04 prod enrichment got NSE official data for 0 of 3,162
# NSE-side stocks (all fell back to Yahoo), while dev got 2,561. The prod log
# has no NSE errors -- fetch_fundamentals_nse() swallows failures and returns
# {}. This script shows the real reason (package versions, the raw reply,
# exceptions, cookie folder) so dev and prod output can be compared side by
# side.
#
# Read-only: no database access, nothing written except the NSE cookie folder.
#
# Usage (from the market-data-loader folder):
#   dev  : python tests\diagnose_nse_manual.py
#          python tests\diagnose_nse_manual.py RELIANCE TCS --sme REXPIPES
#   prod : docker exec -w /opt/tmt/app/market-data-loader tmt-prod-mdl-stock-universe \
#              python tests/diagnose_nse_manual.py
#          (if the image predates this file, pipe it in instead:
#           Get-Content tests\diagnose_nse_manual.py | docker exec -i -w /opt/tmt/app/market-data-loader tmt-prod-mdl-stock-universe python -)
# -----------------------------------------------------------

import argparse
import json
import os
import platform
import sys
import tempfile
import traceback
from importlib import metadata
from pathlib import Path

ROOT = Path(os.getcwd())
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

LINE = "-" * 72


def section(title):
    print(f"\n{LINE}\n{title}\n{LINE}")


def short(value, limit=600):
    try:
        text = json.dumps(value, default=str)
    except Exception:
        text = repr(value)
    return text if len(text) <= limit else text[:limit] + f"... ({len(text)} chars)"


def describe(value):
    if isinstance(value, dict):
        return f"dict with keys {list(value.keys())[:15]}"
    if isinstance(value, list):
        return f"list of {len(value)}"
    return f"{type(value).__name__}: {short(value, 300)}"


def environment():
    section("1. Environment")
    print(f"python        : {sys.version.split()[0]} ({platform.platform()})")
    print(f"cwd           : {os.getcwd()}")
    print(f"user / HOME   : {os.environ.get('USER') or os.environ.get('USERNAME')} / {os.environ.get('HOME') or os.environ.get('USERPROFILE')}")
    for pkg in ("nse", "bse", "yfinance", "httpx", "requests", "curl_cffi", "urllib3", "certifi"):
        try:
            print(f"{pkg:<14}: {metadata.version(pkg)}")
        except metadata.PackageNotFoundError:
            print(f"{pkg:<14}: (not installed)")
    for var in ("HTTP_PROXY", "HTTPS_PROXY", "NO_PROXY", "http_proxy", "https_proxy"):
        if os.environ.get(var):
            print(f"{var:<14}: set")


def cookie_folder():
    section("2. NSE cookie folder (same one enrichment uses)")
    try:
        import stock_universe_update_listener as listener  # noqa: F401 -- only for the folder constant
        folder = Path(listener.NSE_DOWNLOAD_FOLDER)
    except Exception as e:
        folder = ROOT / "nse_downloads"
        print(f"(could not import the listener -- {type(e).__name__}: {e}; using {folder})")
    print(f"folder        : {folder}")
    print(f"exists        : {folder.exists()}")
    try:
        folder.mkdir(exist_ok=True)
        probe = folder / ".write_probe"
        probe.write_text("ok")
        print("writable      : yes")
        try:
            probe.unlink()
        except Exception:
            pass
    except Exception as e:
        print(f"writable      : NO -- {type(e).__name__}: {e}")
        folder = Path(tempfile.mkdtemp(prefix="nse_diag_"))
        print(f"using instead : {folder}")
    for f in sorted(folder.glob("*")):
        print(f"  file        : {f.name} ({f.stat().st_size} bytes)")
    return folder


def call(label, fn, *args, **kwargs):
    print(f"\n> {label}")
    try:
        result = fn(*args, **kwargs)
        print(f"  ok          : {describe(result)}")
        return result, None
    except Exception as e:
        print(f"  EXCEPTION   : {type(e).__name__}: {e}")
        print("  " + "  ".join(traceback.format_exc().splitlines(True)[-6:]))
        return None, e


def check_symbol(nse, symbol, series):
    section(f"3. {symbol} (series {series})")
    meta, _ = call(f"equityMetaInfo('{symbol}')", nse.equityMetaInfo, symbol)
    if isinstance(meta, dict):
        print(f"  isDelisted  : {meta.get('isDelisted')}")

    data, _ = call(f"getDetailedScripData('{symbol}', series='{series}')", nse.getDetailedScripData, symbol, series=series)
    verdict = "FAIL"
    if isinstance(data, dict):
        responses = data.get("equityResponse")
        if responses:
            sec = (responses[0] or {}).get("secInfo") or {}
            print(f"  sector      : {sec.get('sector')!r}")
            print(f"  industry    : {sec.get('industryInfo') or sec.get('basicIndustry')!r}")
            verdict = "OK" if sec.get("sector") else "NO SECTOR"
        else:
            print("  equityResponse missing or empty -- this is what enrichment silently treats as 'no data'.")
            print(f"  raw reply   : {short(data)}")
    elif data is not None:
        print(f"  raw reply   : {short(data)}")

    # Same call path enrichment uses (with retries and series fallback).
    try:
        from core.stock_universe.nse_client import fetch_fundamentals_nse
        fields, _ = call("fetch_fundamentals_nse (enrichment code path)", fetch_fundamentals_nse, nse, symbol,
                         "NSE SME" if series != "EQ" else "NSE")
        if isinstance(fields, dict):
            print(f"  fields      : {sorted(fields.keys())}")
            print(f"  sector      : {fields.get('sector')!r}")
    except Exception as e:
        print(f"  (could not import nse_client -- {type(e).__name__}: {e})")
    return verdict


def raw_http(nse, symbol):
    section(f"4. Raw HTTP from the same session ({symbol})")
    client = None
    for name in dir(nse):
        obj = getattr(nse, name, None)
        if obj is not None and hasattr(obj, "get") and hasattr(obj, "cookies"):
            client = obj
            print(f"session attr  : {name} ({type(obj).__module__}.{type(obj).__name__})")
            break
    if client is None:
        print("no HTTP session object found on the NSE instance -- skipping")
        return
    try:
        cookies = getattr(client, "cookies", None)
        names = list(cookies.keys()) if hasattr(cookies, "keys") else [c.name for c in cookies]
        print(f"cookies       : {names}")
    except Exception as e:
        print(f"cookies       : (unreadable: {e})")
    url = "https://www.nseindia.com/api/quote-equity"
    try:
        r = client.get(url, params={"symbol": symbol}, timeout=15)
        print(f"GET {url}?symbol={symbol}")
        print(f"  status      : {r.status_code}")
        print(f"  content-type: {r.headers.get('content-type')}")
        print(f"  body        : {r.text[:400]!r}")
    except Exception as e:
        print(f"  EXCEPTION   : {type(e).__name__}: {e}")


def main():
    parser = argparse.ArgumentParser(description="Fetch stocks from the NSE official API and show what comes back.")
    parser.add_argument("symbols", nargs="*", default=["RELIANCE", "TCS", "ARVEE"], help="mainboard symbols (series EQ)")
    parser.add_argument("--sme", nargs="*", default=["REXPIPES"], help="NSE SME symbols (series ST/SM/SZ)")
    args = parser.parse_args()

    environment()
    folder = cookie_folder()

    section("NSE session")
    try:
        from nse import NSE
    except Exception as e:
        print(f"cannot import nse: {type(e).__name__}: {e}")
        return 2

    results = {}
    try:
        with NSE(str(folder)) as nse:
            print("NSE() opened")
            for s in args.symbols:
                results[s] = check_symbol(nse, s, "EQ")
            for s in args.sme:
                verdict = "FAIL"
                for series in ("ST", "SM", "SZ"):
                    verdict = check_symbol(nse, s, series)
                    if verdict == "OK":
                        break
                results[s + " (SME)"] = verdict
            raw_http(nse, args.symbols[0] if args.symbols else "RELIANCE")
    except Exception as e:
        print(f"NSE() session FAILED: {type(e).__name__}: {e}")
        traceback.print_exc()
        return 2

    section("Summary")
    for s, v in results.items():
        print(f"  {s:<20} {v}")
    ok = sum(1 for v in results.values() if v == "OK")
    print(f"\n  {ok}/{len(results)} symbols returned an NSE sector.")
    return 0 if ok == len(results) else 1


if __name__ == "__main__":
    sys.exit(main())
