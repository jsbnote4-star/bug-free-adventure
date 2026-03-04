-- What % of training-era population was non-main in Bucket 1?
-- And how different is their BCC7482 vs main?
SELECT
    ATT_MAIN,
    COUNT(*)                                    AS TOTAL_LOANS,
    ROUND(COUNT(*) / SUM(COUNT(*)) OVER()
          * 100, 2)                             AS PCT_OF_POPULATION,
    AVG(BCC7482)                                AS AVG_BCC7482,
    ROUND(COUNT_IF(BCC7482 >= 70)
          / COUNT(*) * 100, 2)                  AS PCT_GTE70

FROM TDM_RISK_MGMT_HUB_CLONE_PL5.MODELED.PL_GEN3_COL_FEATURES_2025
WHERE OBSERVATION_DATE BETWEEN '2025-01-01' AND '2025-11-30'
  AND DQ_BUCKET = 'Bucket 1'
GROUP BY 1
ORDER BY 1 DESC;
