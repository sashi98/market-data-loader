# tests/test_bse_index_expansion.py -- TMT-MDL-BUG-0006
#
# BSE gives one "Index" per stock (the smallest broad index it is in). The broad indices nest, so index_list must be
# cumulative: a SENSEX stock is also in BSE 100 / 200 / 500 / 1000. Before the fix BSE 100 vs BSE 200 had 0 stocks in
# common and the tiers partitioned BSE 1000 (30 + 70 + 100 + 301 + 500). No network. Run: python tests/test_bse_index_expansion.py
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from core.stock_universe.bse_client import expand_bse_index, BSE_BROAD_TIERS  # noqa: E402


def test_each_tier_includes_every_larger_tier():
    assert expand_bse_index("BSE SENSEX") == ["BSE SENSEX", "BSE 100", "BSE 200", "BSE 500", "BSE 1000"]
    assert expand_bse_index("BSE 100") == ["BSE 100", "BSE 200", "BSE 500", "BSE 1000"]
    assert expand_bse_index("BSE 200") == ["BSE 200", "BSE 500", "BSE 1000"]
    assert expand_bse_index("BSE 500") == ["BSE 500", "BSE 1000"]
    assert expand_bse_index("BSE 1000") == ["BSE 1000"]


def test_spacing_and_case_tolerated_other_indices_kept():
    assert expand_bse_index(" bse  100 ") == ["BSE 100", "BSE 200", "BSE 500", "BSE 1000"]
    assert expand_bse_index("BSE SME IPO") == ["BSE SME IPO"]
    assert expand_bse_index("BSE IPO") == ["BSE IPO"]
    assert expand_bse_index(None) == [] and expand_bse_index("") == [] and expand_bse_index("-") == []


def test_tier_counts_become_cumulative():
    """The 19-Oct-2026 partition (30 / 70 / 100 / 301 / 500) becomes 30 / 100 / 200 / 501 / 1001."""
    stocks = (["BSE SENSEX"] * 30 + ["BSE 100"] * 70 + ["BSE 200"] * 100 + ["BSE 500"] * 301 + ["BSE 1000"] * 500)
    counts = {t: 0 for t in BSE_BROAD_TIERS}
    for idx in stocks:
        for t in expand_bse_index(idx):
            counts[t] += 1
    assert counts == {"BSE SENSEX": 30, "BSE 100": 100, "BSE 200": 200, "BSE 500": 501, "BSE 1000": 1001}, counts


if __name__ == "__main__":
    for name, fn in list(globals().items()):
        if name.startswith("test_"):
            fn()
            print("PASS", name)
