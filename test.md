-- ─────────────────────────────────────────────────────────────
-- Diagnostic: ATT_TOT_DQ_DAYS_CYC_RATIO_L3M and BCA8151
-- Why is PSI spiking specifically in Sep 2025 for Bucket 3?
-- ─────────────────────────────────────────────────────────────

-- STEP 1: Month by month distribution of both variables in Bucket 3
-- Compare Sep vs surrounding months to isolate the spike
SELECT
    DATE_TRUNC('MONTH', OBSERVATION_DATE)   AS MONTH,
    COUNT(*)                                 AS TOTAL_LOANS,

    -- ATT_TOT_DQ_DAYS_CYC_RATIO_L3M stats
    ROUND(AVG(ATT_TOT_DQ_DAYS_CYC_RATIO_L3M), 4)                        AS AVG_DQ_RATIO_L3M,
    ROUND(MEDIAN(ATT_TOT_DQ_DAYS_CYC_RATIO_L3M), 4)                     AS MEDIAN_DQ_RATIO_L3M,
    ROUND(COUNT_IF(ATT_TOT_DQ_DAYS_CYC_RATIO_L3M = 0) 
          / COUNT(*) * 100, 2)                                           AS PCT_ZERO_DQ_RATIO_L3M,
    ROUND(COUNT_IF(ATT_TOT_DQ_DAYS_CYC_RATIO_L3M IS NULL) 
          / COUNT(*) * 100, 2)                                           AS PCT_NULL_DQ_RATIO_L3M,
    ROUND(COUNT_IF(ATT_TOT_DQ_DAYS_CYC_RATIO_L3M > 0.5) 
          / COUNT(*) * 100, 2)                                           AS PCT_GT50PCT_DQ_RATIO_L3M,
    ROUND(COUNT_IF(ATT_TOT_DQ_DAYS_CYC_RATIO_L3M = 1) 
          / COUNT(*) * 100, 2)                                           AS PCT_FULL_DQ_RATIO_L3M,

    -- BCA8151 stats
    ROUND(AVG(BCA8151), 4)                                               AS AVG_BCA8151,
    ROUND(MEDIAN(BCA8151), 4)                                            AS MEDIAN_BCA8151,
    ROUND(COUNT_IF(BCA8151 = 0) 
          / COUNT(*) * 100, 2)                                           AS PCT_ZERO_BCA8151,
    ROUND(COUNT_IF(BCA8151 IS NULL) 
          / COUNT(*) * 100, 2)                                           AS PCT_NULL_BCA8151,
    ROUND(COUNT_IF(BCA8151 > 100) 
          / COUNT(*) * 100, 2)                                           AS PCT_GT100_BCA8151

FROM TDM_RISK_MGMT_HUB_CLONE_PL5.MODELED.PL_GEN3_COL_FEATURES_2025
WHERE OBSERVATION_DATE BETWEEN '2025-01-01' AND '2025-11-30'
  AND DQ_BUCKET = 'Bucket 3'
GROUP BY 1
ORDER BY 1;


-- ─────────────────────────────────────────────────────────────
-- STEP 2: What is different about Sep Bucket 3 accounts?
-- Check their DQ history — are these accounts that just entered B3
-- or have been in B3 for multiple cycles?
-- ─────────────────────────────────────────────────────────────
SELECT
    DATE_TRUNC('MONTH', OBSERVATION_DATE)   AS MONTH,
    COUNT(*)                                 AS TOTAL_LOANS,

    -- DQ history context
    ROUND(AVG(ATT_SEASONING), 1)             AS AVG_SEASONING,
    ROUND(AVG(ATT_DQ2UP_CNT_12M), 2)        AS AVG_DQ2UP_12M,
    ROUND(AVG(ATT_DQCNT_6M), 2)             AS AVG_DQCNT_6M,
    ROUND(AVG(ATT_CONS_NODQ3UP_CNT), 2)     AS AVG_CONS_NODQ3UP,

    -- How long have they been in B3+?
    ROUND(COUNT_IF(ATT_CONS_NODQ3UP_CNT = 0)
          / COUNT(*) * 100, 2)               AS PCT_STUCK_IN_B3,
    -- Fresh rollin vs chronic
    ROUND(COUNT_IF(ATT3_ROLLIN_B2_CNT_12M > 0)
          / COUNT(*) * 100, 2)               AS PCT_RECENT_ROLLIN,

    -- The key ratio itself
    ROUND(AVG(ATT_TOT_DQ_DAYS_CYC_RATIO_L3M), 4) AS AVG_DQ_RATIO_L3M,
    ROUND(AVG(BCA8151), 4)                   AS AVG_BCA8151

FROM TDM_RISK_MGMT_HUB_CLONE_PL5.MODELED.PL_GEN3_COL_FEATURES_2025
WHERE OBSERVATION_DATE BETWEEN '2025-01-01' AND '2025-11-30'
  AND DQ_BUCKET = 'Bucket 3'
GROUP BY 1
ORDER BY 1;


-- ─────────────────────────────────────────────────────────────
-- STEP 3: Isolate Sep 2025 Bucket 3 accounts
-- Are they a different type of account vs Aug and Oct?
-- Check origination cohort and seasoning
-- ─────────────────────────────────────────────────────────────
SELECT
    DATE_TRUNC('MONTH', OBSERVATION_DATE)   AS MONTH,
    -- Origination cohort bucketing
    CASE
        WHEN ATT_SEASONING <= 6   THEN '1. New (<=6m)'
        WHEN ATT_SEASONING <= 12  THEN '2. Early (7-12m)'
        WHEN ATT_SEASONING <= 24  THEN '3. Mid (13-24m)'
        WHEN ATT_SEASONING <= 36  THEN '4. Mature (25-36m)'
        ELSE                           '5. Seasoned (>36m)'
    END                                      AS SEASONING_BUCKET,
    COUNT(*)                                 AS TOTAL_LOANS,
    ROUND(AVG(ATT_TOT_DQ_DAYS_CYC_RATIO_L3M), 4) AS AVG_DQ_RATIO_L3M,
    ROUND(AVG(BCA8151), 4)                   AS AVG_BCA8151

FROM TDM_RISK_MGMT_HUB_CLONE_PL5.MODELED.PL_GEN3_COL_FEATURES_2025
WHERE OBSERVATION_DATE BETWEEN '2025-08-01' AND '2025-10-30'
  AND DQ_BUCKET = 'Bucket 3'
GROUP BY 1, 2
ORDER BY 1, 2;
