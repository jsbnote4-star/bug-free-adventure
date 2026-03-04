-- Quick diagnostic: how many Experian batches exist for mismatch loans?
SELECT
    a.USER_ID,
    COUNT(DISTINCT a.experian_batch_id)   AS num_batches,
    MIN(DATEADD(MONTH, 1, DATE_FROM_PARTS(
        YEAR(TO_DATE(a.REPORTING_DATE_ID::STRING,'YYYYMMDD')),
        MONTH(TO_DATE(a.REPORTING_DATE_ID::STRING,'YYYYMMDD')), 5
    )))                                    AS earliest_available,
    MAX(DATEADD(MONTH, 1, DATE_FROM_PARTS(
        YEAR(TO_DATE(a.REPORTING_DATE_ID::STRING,'YYYYMMDD')),
        MONTH(TO_DATE(a.REPORTING_DATE_ID::STRING,'YYYYMMDD')), 5
    )))                                    AS latest_available

FROM TDM_RISK_MGMT_HUB.SUMMARIZED.RR_EXPERIAN_PREMIER_ATTRIBUTES_VIEW_A_FACT a

WHERE a.USER_ID IN (
    -- PL_USER_IDs for the 7 mismatch loans
    SELECT PL_USER_ID 
    FROM TDM_RISK_MGMT_HUB_CLONE_PL5.MODELED.PROD_NOV_SAMPLE
    WHERE BUSINESS_LOAN_ID IN (1646484, 1181069, 1385935, 
                                2306809, 1503659, 1628721, 2553891)
)
GROUP BY a.USER_ID
ORDER BY num_batches DESC;
