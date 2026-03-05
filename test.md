-- ─────────────────────────────────────────────────────────────
-- Feature distribution comparison: JPM tiers vs non-JPM tiers
-- For Bucket 1, Q4 2025
-- This explains WHY PSI is high when JPM accounts are included
-- ─────────────────────────────────────────────────────────────

WITH SPLIT AS (
    SELECT
        *,
        IFF(TIER IN (61, 62, 63), 'JPM', 'NON_JPM') AS TIER_GROUP
    FROM TDM_RISK_MGMT_HUB_CLONE_PL5.MODELED.PL_GEN3_COL_FEATURES_2025
    WHERE OBSERVATION_DATE BETWEEN '2025-09-01' AND '2025-11-30'
      AND DQ_BUCKET = 'Bucket 1'
      AND COMBINED_EXCLUSION_FLAG = 0
)

SELECT
    TIER_GROUP,
    COUNT(*)                                            AS TOTAL_LOANS,

    -- Features with highest PSI breaches
    -- Consecutive no DQ features
    ROUND(AVG(ATT_CONS_NODQ2UP_CNT), 2)                AS AVG_CONS_NODQ2UP,
    ROUND(MEDIAN(ATT_CONS_NODQ2UP_CNT), 2)             AS MED_CONS_NODQ2UP,
    ROUND(AVG(ATT_CONS_NODQ3UP_CNT), 2)                AS AVG_CONS_NODQ3UP,
    ROUND(MEDIAN(ATT_CONS_NODQ3UP_CNT), 2)             AS MED_CONS_NODQ3UP,
    ROUND(AVG(ATT_CONS_NODQCNT), 2)                    AS AVG_CONS_NODQCNT,

    -- Rollin features
    ROUND(AVG(ATT3_ROLLIN_B1_CNT_12M), 4)              AS AVG_ROLLIN_B1_12M,
    ROUND(AVG(ATT_DQCNT_3M), 4)                        AS AVG_DQCNT_3M,
    ROUND(AVG(ATT_DQ2UP_CNT_6M), 4)                    AS AVG_DQ2UP_6M,

    -- Autopay
    ROUND(AVG(ATT_IS_AUTOPAY), 4)                      AS AVG_AUTOPAY,
    ROUND(AVG(ATT_IS_AUTOPAY_L6M), 4)                  AS AVG_AUTOPAY_L6M,

    -- Credit score features
    ROUND(AVG(ATT2_ORIG_FICO), 2)                      AS AVG_ORIG_FICO,
    ROUND(AVG(ATT2_ORIG_VANTAGE), 2)                   AS AVG_ORIG_VANTAGE,
    ROUND(AVG(ATT2_FICO_CHANGE), 4)                    AS AVG_FICO_CHANGE,
    ROUND(AVG(ATT2_VANTAGE_CHANGE), 4)                 AS AVG_VANTAGE_CHANGE,

    -- Bureau features
    ROUND(AVG(ALL7517), 4)                             AS AVG_ALL7517,
    ROUND(AVG(ALL7518), 4)                             AS AVG_ALL7518,
    ROUND(AVG(ALL7519), 4)                             AS AVG_ALL7519,
    ROUND(AVG(ATT_PAST_DUE_TO_SCHD), 4)               AS AVG_PAST_DUE_SCHD,
    ROUND(AVG(ATT_GROSS_INCOME), 2)                    AS AVG_GROSS_INCOME,

    -- Seasoning
    ROUND(AVG(ATT_SEASONING), 2)                       AS AVG_SEASONING

FROM SPLIT
GROUP BY TIER_GROUP
ORDER BY TIER_GROUP;
```

---

## Part 2 — Target PSI Explanation

Yes — your reasoning is correct and it's actually the standard explanation for this situation:
```
Features out of distribution
        ↓
Model receives inputs it never saw during training
        ↓
Predictions are unreliable / in wrong range
        ↓
Score distribution shifts
        ↓
Target PSI breaches because decile/bucket 
assignment is based on shifted scores
