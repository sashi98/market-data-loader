# -----------------------------------------------------------
# tests/diagnose_bse_manual.py
# Manual diagnostic: call BSE equityMetaInfo the way stock_universe
# enrichment does, first with the installed bse package's own headers,
# then with the headers bse 3.3.3 sends (newer Chrome User-Agent plus
# Sec-Fetch-Site: same-site).
#
# Why: on 2026-10-05 prod enrichment got "403: Forbidden" for every BSE
# scrip (2,167 failed, 0 OK) after bse was pinned to 3.3.0. 3.3.3 changed
# only its request headers, so this shows whether those headers are what
# BSE now requires.
#
# Read-only: no database access.
#
# Usage (from the market-data-loader folder):
#   dev  : python tests\diagnose_bse_manual.py
#   prod : Get-Content tests\diagnose_bse_manual.py | docker exec -i -w /opt/tmt/app/market-data-loader tmt-prod-mdl-stock-universe python -
# -----------------------------------------------------------

import os
import sys
import tempfile
from importlib import metadata
from pathlib import Path

ROOT = Path(os.getcwd())
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

CODES = ["500325", "532540", "505533", "544356"]  # RELIANCE, TCS, + two that 403'd in prod
UA_333 = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
          "(KHTML, like Gecko) Chrome/153.0.0.0 Safari/537.36")
LINE = "-" * 72


def run(label, patch):
    from bse import BSE
    print(f"\n{LINE}\n{label}\n{LINE}")
    ok = 0
    with BSE(tempfile.mkdtemp(prefix="bse_diag_")) as bse:
        if patch:
            bse.session.headers.update({"User-Agent": UA_333, "Sec-Fetch-Site": "same-site"})
        print(f"User-Agent    : {bse.session.headers.get('User-Agent')}")
        print(f"Sec-Fetch-Site: {bse.session.headers.get('Sec-Fetch-Site')}")
        for code in CODES:
            try:
                meta = bse.equityMetaInfo(code)
                sector = (meta or {}).get("IndustryNew")
                print(f"  {code}: OK  sector={sector!r}")
                ok += 1
            except Exception as e:
                print(f"  {code}: FAIL {type(e).__name__}: {e}")
    print(f"  -> {ok}/{len(CODES)} OK")
    return ok


def main():
    print(f"python : {sys.version.split()[0]}")
    for pkg in ("bse", "requests", "urllib3"):
        try:
            print(f"{pkg:<7}: {metadata.version(pkg)}")
        except metadata.PackageNotFoundError:
            print(f"{pkg:<7}: (not installed)")
    a = run("A. Installed bse package headers (unchanged)", patch=False)
    b = run("B. bse 3.3.3 headers (Chrome/153 UA + Sec-Fetch-Site)", patch=True)
    print(f"\n{LINE}\nSummary: installed headers {a}/{len(CODES)}, 3.3.3 headers {b}/{len(CODES)}\n{LINE}")
    return 0 if a == len(CODES) else 1


if __name__ == "__main__":
    sys.exit(main())
