-- ─────────────────────────────────────────────────────────────
-- Confirm BCC7482 distribution shift hypothesis
-- Compare training period vs current scoring population
-- Split by ATT_MAIN (prime vs non-prime) to confirm connection
-- ─────────────────────────────────────────────────────────────

-- STEP 1: BCC7482 distribution by ATT_MAIN in current scoring population
-- If hypothesis is correct, ATT_MAIN=0 should have higher BCC7482 values
SELECT
    'CURRENT SCORING (Q4 2025)'         AS PERIOD,
    ATT_MAIN,
    COUNT(*)                             AS TOTAL_LOANS,

    -- Raw distribution
    AVG(BCC7482)                         AS AVG_BCC7482,
    MEDIAN(BCC7482)                      AS MEDIAN_BCC7482,
    PERCENTILE_CONT(0.25) WITHIN GROUP 
        (ORDER BY BCC7482)               AS P25_BCC7482,
    PERCENTILE_CONT(0.75) WITHIN GROUP 
        (ORDER BY BCC7482)               AS P75_BCC7482,

    -- How many accounts hit the >=70 threshold
    COUNT_IF(BCC7482 >= 70)              AS COUNT_GTE70,
    ROUND(COUNT_IF(BCC7482 >= 70) 
          / COUNT(*) * 100, 2)           AS PCT_GTE70,

    -- Null rate
    COUNT_IF(BCC7482 IS NULL)            AS NULL_COUNT,
    ROUND(COUNT_IF(BCC7482 IS NULL) 
          / COUNT(*) * 100, 2)           AS NULL_PCT

FROM TDM_RISK_MGMT_HUB_CLONE_PL5.MODELED.PL_GEN3_COL_FEATURES_2025
WHERE OBSERVATION_DATE BETWEEN '2025-09-01' AND '2025-11-30'
  AND DQ_BUCKET = 'Bucket 1'
GROUP BY ATT_MAIN
ORDER BY ATT_MAIN DESC;


-- ─────────────────────────────────────────────────────────────
-- STEP 2: Compare same distribution in training period
-- Use earlier observation dates that align with model training window
-- ─────────────────────────────────────────────────────────────
SELECT
    'TRAINING PERIOD'                    AS PERIOD,
    ATT_MAIN,
    COUNT(*)                             AS TOTAL_LOANS,

    AVG(BCC7482)                         AS AVG_BCC7482,
    MEDIAN(BCC7482)                      AS MEDIAN_BCC7482,
    PERCENTILE_CONT(0.25) WITHIN GROUP 
        (ORDER BY BCC7482)               AS P25_BCC7482,
    PERCENTILE_CONT(0.75) WITHIN GROUP 
        (ORDER BY BCC7482)               AS P75_BCC7482,

    COUNT_IF(BCC7482 >= 70)              AS COUNT_GTE70,
    ROUND(COUNT_IF(BCC7482 >= 70) 
          / COUNT(*) * 100, 2)           AS PCT_GTE70,

    COUNT_IF(BCC7482 IS NULL)            AS NULL_COUNT,
    ROUND(COUNT_IF(BCC7482 IS NULL) 
          / COUNT(*) * 100, 2)           AS NULL_PCT

FROM TDM_RISK_MGMT_HUB_CLONE_PL5.MODELED.PL_GEN3_COL_FEATURES_2025
WHERE OBSERVATION_DATE BETWEEN '2022-01-01' AND '2023-12-31' -- training window
  AND DQ_BUCKET = 'Bucket 1'
GROUP BY ATT_MAIN
ORDER BY ATT_MAIN DESC;


-- ─────────────────────────────────────────────────────────────
-- STEP 3: Month by month BCC7482 trend for prime only
-- To confirm it's consistent across all months (structural)
-- not a one-off spike
-- ─────────────────────────────────────────────────────────────
SELECT
    DATE_TRUNC('MONTH', OBSERVATION_DATE) AS MONTH,
    ATT_MAIN,
    COUNT(*)                               AS TOTAL_LOANS,
    AVG(BCC7482)                           AS AVG_BCC7482,
    ROUND(COUNT_IF(BCC7482 >= 70) 
          / COUNT(*) * 100, 2)             AS PCT_GTE70,
    ROUND(COUNT_IF(BCC7482 IS NULL) 
          / COUNT(*) * 100, 2)             AS NULL_PCT

FROM TDM_RISK_MGMT_HUB_CLONE_PL5.MODELED.PL_GEN3_COL_FEATURES_2025
WHERE OBSERVATION_DATE BETWEEN '2022-01-01' AND '2025-11-30'
  AND DQ_BUCKET = 'Bucket 1'
GROUP BY 1, 2
ORDER BY 1, 2 DESC;
