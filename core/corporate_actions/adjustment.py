# core/corporate_actions/adjustment.py
#
# Shared SQL fragment for applying corporate-actions price adjustment at
# RSI read-time. bhav_copy itself is never touched -- both
# rsi_incremental.py's CLOSES_FOR_DATE_SQL and rsi_persistence.py's
# fetch_bhav_copy_closes()/fetch_bhav_copy_closes_for_isin() join
# against this fragment and multiply close/prev_close by the resulting
# factor before gain/loss math ever sees them. See 013.02.00's
# changelog comment (tmt/src/main/resources/db/changelog) for the full
# design rationale.
#
# Only MATCHED rows with a non-NULL adjustment_factor are ever applied
# automatically -- NSE_ONLY/BSE_ONLY/CONFLICT rows require manual
# confirmation first (promoting them to MATCHED via a real second-source
# match, or a manual DB correction) before they affect any RSI number.
#
# A single isin can have MULTIPLE corporate actions over its history
# (e.g. two separate bonus issues) -- the cumulative factor for a given
# trade_date is the PRODUCT of every MATCHED action's factor whose
# ex_date is strictly after that trade_date, so the isin's price series
# stays continuous across every action, not just the most recent one.
# Postgres has no built-in PRODUCT aggregate, hence the EXP(SUM(LN(x)))
# trick -- safe here since adjustment_factor is always a positive
# multiplier (a ratio of positive face values / share counts).

# Two DISTINCT factors are exposed -- close_factor and prev_close_factor
# -- not one shared "factor". close on a given trade_date only needs
# adjusting for actions whose ex_date is STRICTLY AFTER that date (an
# ex_date row's own close is already expressed in post-action terms).
# prev_close, however, is the PRIOR session's raw close carried onto
# THIS row -- so on the ex_date row itself (trade_date == ex_date),
# prev_close still needs the adjustment even though close does not.
# Using a single factor gated only on "ex_date > trade_date" left the
# ex_date row's prev_close unadjusted (factor collapsed to 1 via the
# COALESCE), producing one wildly wrong gain/loss on the split day that
# then poisoned Wilder's RSI smoothing for weeks afterward -- this was
# the actual root cause of the MWL (INE0JYY01029) RSI bug, diagnosed
# 2026-08-23: raw prev_close(370.25) - raw close(36.65) = 333.60,
# exactly the corrupted `loss` value that had been sitting in
# rsi14d_workbook. close_factor keeps the ">" condition; prev_close_factor
# uses ">=" so it also picks up the ex_date row itself.
ADJUSTMENT_FACTOR_JOIN_SQL = """
    LEFT JOIN LATERAL (
        SELECT
            EXP(SUM(LN(ca.adjustment_factor))
                FILTER (WHERE ca.ex_date > bc.trade_date))  AS close_factor,
            EXP(SUM(LN(ca.adjustment_factor))
                FILTER (WHERE ca.ex_date >= bc.trade_date)) AS prev_close_factor
          FROM corporate_actions ca
         WHERE ca.isin = bc.isin
           AND ca.reconciliation_status = 'MATCHED'
           AND ca.adjustment_factor IS NOT NULL
    ) adj ON TRUE
"""
