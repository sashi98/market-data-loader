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

ADJUSTMENT_FACTOR_JOIN_SQL = """
    LEFT JOIN LATERAL (
        SELECT EXP(SUM(LN(ca.adjustment_factor))) AS factor
          FROM corporate_actions ca
         WHERE ca.isin = bc.isin
           AND ca.reconciliation_status = 'MATCHED'
           AND ca.adjustment_factor IS NOT NULL
           AND ca.ex_date > bc.trade_date
    ) adj ON TRUE
"""
