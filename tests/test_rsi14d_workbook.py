# tests/test_rsi14d_workbook.py
#
# Regression test for the 2026-08-23/24 MWL corporate-actions RSI
# saga -- two layered fixes, both covered here:
#
#   ROOT CAUSE #1 (2026-08-23, see
#   claude/rsi-adjustment-factor-prev-close-fix-2026-08-23.md) --
#   ADJUSTMENT_FACTOR_JOIN_SQL applied the same cumulative factor to
#   both `close` and `prev_close`, gated on `ex_date > trade_date`. On
#   the split's ex_date row itself that condition is false, so
#   `prev_close` (the prior session's RAW pre-split close) was left
#   unadjusted while `close` (correctly, already post-split) also
#   stayed unadjusted -- producing a huge false "loss" of 333.60 on
#   2026-07-10. Fixed by giving prev_close its own factor gated
#   `ex_date >= trade_date`.
#
#   ROOT CAUSE #2 (2026-08-24, see
#   claude/bhav-copy-adjusted-clean-price-series-design-2026-08-23.md)
#   -- MWL's ISIN changed (INE0JYY01011 -> INE0JYY01029) on the SAME
#   ex_date as the split (an SME-to-mainboard migration bundled with
#   the split). Every RSI query was isin-scoped, so the isin change
#   truncated MWL's visible history to zero on 2026-07-10, forcing
#   Wilder's 14-row seed to restart from scratch -- 13 NULL rsi14 rows
#   before the walk reseeded. Fixed by moving RSI's source from raw
#   bhav_copy to bhav_copy_adjusted (security_identity_lineage-bridged,
#   split-adjusted, continuous) and re-keying rsi14d_workbook on
#   security_id instead of isin (see 014.03.00's changelog and
#   core/rsi/rsi_persistence.py / rsi_incremental.py / rsi_continuity.py
#   / rsi_calculator.py, all repointed 2026-08-24).
#
# With both fixes applied, rsi14 on 2026-08-21 should read close to
# the broker/TradingView value (~62.81), against 4.34 with only fix #1
# applied (isin-scoped, truncated history) and much worse before either
# fix.
#
# This is a LIVE-DATABASE regression test, same category as
# tests/test_parser_manual.py (not a pure-fixture unit test like
# tests/test_rollup_calculator.py) -- it connects to the real Postgres
# instance configured in config/.env and asserts against whatever is
# currently in rsi14d_workbook. For the assertions to hold:
#   1. Both fixes above must be applied.
#   2. loaders/bhav_copy_adjustment_loader.py must have been run (so
#      bhav_copy_adjusted exists and is populated).
#   3. rsi14d_loader.py must have been re-run (full recompute) AFTER
#      both fixes, so rsi14d_workbook actually reflects them.
#
# Run from repo root:
#   python -m unittest tests.test_rsi14d_workbook -v
# or directly:
#   python tests/test_rsi14d_workbook.py

import sys
import unittest
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from core.env_validator import load_and_validate_env, EnvValidationError
from core.db_client import get_connection, DbConnectionError

# The known-good expected value for this scenario -- sourced from the
# AngleOne broker terminal and TradingView's own RSI(14) reading on
# 2026-08-21 (see the diagnostic conversation in
# claude/rsi-adjustment-factor-prev-close-fix-2026-08-23.md). Loaded as
# data here, not hardcoded inline in the assertion, so a future case
# for a different symbol/date can be added to EXPECTED_CASES without
# touching the test method itself.
EXPECTED_CASES = [
    {
        "label": "MWL split (1:10, ex_date=2026-07-10) -- post-fix RSI14",
        "isin": "INE0JYY01029",
        "exchange": "NSE",
        "symbol": "MWL",
        "trade_date": date(2026, 8, 21),
        "expected_rsi14": 62.81,
        # Small tolerance, not an exact match -- our Wilder implementation
        # deliberately matches TradingView's seeding convention (see
        # core/rsi/rsi_calculator.py's module docstring), but a broker
        # terminal's own feed/rounding can still differ by a few tenths.
        # Tight enough to catch a regression back to ~4.34, loose enough
        # to not be a flaky floating-point/rounding test.
        "tolerance": 1.5,
    },
]

# The exact ex_date row whose gain/loss the bug corrupted -- asserted
# directly (not just the downstream RSI14) so a future regression on
# the adjustment-factor SQL itself is caught right at the source, even
# if it happened to not move the 2026-08-21 RSI14 enough to fail the
# case above.
EX_DATE_ROW = {
    "isin": "INE0JYY01029",
    "exchange": "NSE",
    "trade_date": date(2026, 7, 10),
    # raw prev_close(370.25) * adjustment_factor(0.1) - raw close(36.65)
    # = 37.025 - 36.65 = 0.375
    "expected_loss": 0.375,
    "expected_gain": 0.0,
    "tolerance": 0.05,
}


class TestRsi14dWorkbookCorporateActionAdjustment(unittest.TestCase):
    """
    Live-DB regression test class for the corporate-actions
    prev_close adjustment-factor fix (2026-08-23).
    """

    @classmethod
    def setUpClass(cls):
        try:
            env_values = load_and_validate_env()
        except EnvValidationError as e:
            raise unittest.SkipTest(f"Cannot load config/.env -- skipping live-DB test: {e}")

        try:
            cls.conn = get_connection(env_values)
        except DbConnectionError as e:
            raise unittest.SkipTest(f"Cannot connect to Postgres -- skipping live-DB test: {e}")

    @classmethod
    def tearDownClass(cls):
        if getattr(cls, "conn", None) is not None:
            cls.conn.close()

    def _fetch_row(self, isin, exchange, trade_date):
        query = """
            SELECT security_id, symbol, trade_date, gain, loss, avg_gain, avg_loss, rsi14
              FROM rsi14d_workbook
             WHERE isin = %(isin)s AND exchange = %(exchange)s AND trade_date = %(trade_date)s
        """
        with self.conn.cursor() as cur:
            cur.execute(query, {"isin": isin, "exchange": exchange, "trade_date": trade_date})
            row = cur.fetchone()
        if row is None:
            return None
        security_id, symbol, trade_date_db, gain, loss, avg_gain, avg_loss, rsi14 = row
        return {
            "security_id": security_id,
            "symbol": symbol,
            "trade_date": trade_date_db,
            "gain": gain,
            "loss": loss,
            "avg_gain": avg_gain,
            "avg_loss": avg_loss,
            "rsi14": rsi14,
        }

    def test_isin_lineage_bridged_to_single_security_id(self):
        """
        MWL's pre-split/pre-migration row (old isin INE0JYY01011) and
        its post-split/post-migration row (new isin INE0JYY01029) must
        resolve to the SAME security_id -- proof the ISIN change (root
        cause #2) no longer fragments the RSI walk into two identities.
        """
        pre_row = self._fetch_row("INE0JYY01011", "NSE", date(2026, 7, 9))
        post_row = self._fetch_row("INE0JYY01029", "NSE", date(2026, 7, 10))
        self.assertIsNotNone(
            pre_row,
            "No rsi14d_workbook row for the OLD isin INE0JYY01011 on 2026-07-09 -- "
            "has rsi14d_loader.py been (re)run against bhav_copy_adjusted?",
        )
        self.assertIsNotNone(
            post_row,
            "No rsi14d_workbook row for the NEW isin INE0JYY01029 on 2026-07-10.",
        )
        self.assertEqual(
            pre_row["security_id"], post_row["security_id"],
            f"Expected the same security_id bridging INE0JYY01011 -> INE0JYY01029, got "
            f"{pre_row['security_id']!r} vs {post_row['security_id']!r} -- the isin-lineage bridge "
            f"isn't collapsing these into one continuous RSI walk.",
        )

    def test_ex_date_row_gain_loss_correctly_adjusted(self):
        """
        2026-07-10 (the split's ex_date) must show a small, realistic
        loss (~0.38) rather than the pre-fix 333.60 -- proves
        prev_close is now being scaled by prev_close_factor, not left
        raw.
        """
        row = self._fetch_row(EX_DATE_ROW["isin"], EX_DATE_ROW["exchange"], EX_DATE_ROW["trade_date"])
        self.assertIsNotNone(
            row,
            f"No rsi14d_workbook row for isin={EX_DATE_ROW['isin']} exchange={EX_DATE_ROW['exchange']} "
            f"trade_date={EX_DATE_ROW['trade_date']} -- has rsi14d_loader.py been run?",
        )

        self.assertAlmostEqual(
            float(row["loss"]), EX_DATE_ROW["expected_loss"], delta=EX_DATE_ROW["tolerance"],
            msg=(
                f"loss={row['loss']} on the split's ex_date is not close to the expected "
                f"~{EX_DATE_ROW['expected_loss']} -- looks like the pre-fix unadjusted "
                f"prev_close bug (expected ~333.60 in that state)."
            ),
        )
        self.assertAlmostEqual(
            float(row["gain"]), EX_DATE_ROW["expected_gain"], delta=EX_DATE_ROW["tolerance"],
            msg=f"gain={row['gain']} on the split's ex_date should be ~0 (price fell, adjusted for the split).",
        )

    def test_expected_rsi14_values(self):
        """
        Loads EXPECTED_CASES and asserts each one's rsi14d_workbook
        rsi14 against the known-good (broker/TradingView-sourced)
        expected value, within tolerance.
        """
        for case in EXPECTED_CASES:
            with self.subTest(case=case["label"]):
                row = self._fetch_row(case["isin"], case["exchange"], case["trade_date"])
                self.assertIsNotNone(
                    row,
                    f"No rsi14d_workbook row for isin={case['isin']} exchange={case['exchange']} "
                    f"trade_date={case['trade_date']} -- has rsi14d_loader.py been run?",
                )
                self.assertEqual(
                    row["symbol"], case["symbol"],
                    f"Expected symbol {case['symbol']} for isin={case['isin']}, found {row['symbol']!r}.",
                )
                self.assertIsNotNone(
                    row["rsi14"],
                    f"rsi14 is NULL for isin={case['isin']} exchange={case['exchange']} "
                    f"trade_date={case['trade_date']} -- not enough seeded history, or the row hasn't "
                    f"been (re)computed since the fix.",
                )
                self.assertAlmostEqual(
                    float(row["rsi14"]), case["expected_rsi14"], delta=case["tolerance"],
                    msg=(
                        f"{case['label']}: rsi14={row['rsi14']} on {case['trade_date']} is not close to "
                        f"the expected {case['expected_rsi14']} (broker/TradingView-sourced). If this is "
                        f"~4.3, the prev_close adjustment-factor fix likely hasn't been applied, or "
                        f"rsi14d_loader.py hasn't been re-run since it was."
                    ),
                )


if __name__ == "__main__":
    unittest.main()
