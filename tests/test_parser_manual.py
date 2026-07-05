# tests/test_parser_manual.py
#
# Quick manual verification of core/bhavcopy_parser.py against real
# downloaded files -- not an automated test suite, just a sanity check.
# Run from repo root:
#   python tests/test_parser_manual.py

import sys
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from core.bhavcopy_parser import parse_bhavcopy_csv, BhavCopyParseError

DOWNLOAD_DIR = Path("D:/MyProjectsWorkSpace/MY_DEV/track-my-trade/data/all")


def check_file(file_name, expected_date):
    file_path = DOWNLOAD_DIR / file_name
    print(f"\n{'=' * 60}")
    print(f"  {file_name}  (expected trade date: {expected_date})")
    print("=" * 60)
    try:
        rows = parse_bhavcopy_csv(str(file_path), expected_date)
    except BhavCopyParseError as e:
        print(f"  [FAILED] {e}")
        return

    print(f"  Row count (after dedup): {len(rows)}")
    print(f"  First row:")
    for key, value in rows[0].items():
        print(f"    {key:20s} = {value!r}")


if __name__ == "__main__":
    check_file("NSE-BC-03-Jul-2026.csv", date(2026, 7, 3))
    check_file("BSE-BC-03-Jul-2026.csv", date(2026, 7, 3))
