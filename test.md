-- CHECK 1: Are there still duplicate loan + obs date combinations after the fix?
-- If QUALIFY worked correctly this should return ZERO rows

SELECT
    BUSINESS_LOAN_ID,
    BUSINESS_APPLICATION_ID,
    PL_USER_ID,
    OBSERVATION_DATE,
    COUNT(*)            AS ROW_COUNT,
    COUNT(DISTINCT BCA8370) AS DISTINCT_BCA8370_VALUES,  -- bureau attr should be 1
    COUNT(DISTINCT ALL5012) AS DISTINCT_ALL5012_VALUES   -- bureau attr should be 1

FROM TDM_RISK_MGMT_HUB_CLONE_PL5.MODELED.PL_GEN3_COL_FEATURES_2025

WHERE OBSERVATION_DATE BETWEEN '2025-11-01' AND '2025-11-30'

GROUP BY 1, 2, 3, 4
HAVING COUNT(*) > 1   -- only show loans that still have duplicates

ORDER BY ROW_COUNT DESC
LIMIT 20;


-- CHECK 2: Specifically check the 7 mismatch loans
-- These are the ones we KNOW had duplicates before the fix

SELECT
    BUSINESS_LOAN_ID,
    BUSINESS_APPLICATION_ID,
    PL_USER_ID,
    OBSERVATION_DATE,
    COUNT(*)               AS ROW_COUNT,
    MIN(BCA8370)           AS BCA8370_MIN,
    MAX(BCA8370)           AS BCA8370_MAX,
    MIN(ALL5012)           AS ALL5012_MIN,
    MAX(ALL5012)           AS ALL5012_MAX

FROM TDM_RISK_MGMT_HUB_CLONE_PL5.MODELED.PL_GEN3_COL_FEATURES_2025

WHERE BUSINESS_LOAN_ID IN (
    '1646484','1181069','1385935',
    '2306809','1503659','1628721','2553891'
)
AND OBSERVATION_DATE BETWEEN '2025-11-01' AND '2025-11-30'

GROUP BY 1, 2, 3, 4
ORDER BY BUSINESS_LOAN_ID, OBSERVATION_DATE;


-- CHECK 3: How many Experian batches per user are being used
-- after the fix — should be exactly 1 per loan per obs date

SELECT
    COUNT(*)                                    AS TOTAL_ROWS,
    COUNT(DISTINCT BUSINESS_LOAN_ID 
          || OBSERVATION_DATE::STRING)          AS UNIQUE_LOAN_OBS_COMBOS,
    -- These two should be EQUAL if fix worked
    IFF(COUNT(*) = COUNT(DISTINCT BUSINESS_LOAN_ID 
                         || OBSERVATION_DATE::STRING),
        'NO DUPLICATES ✅', 
        'DUPLICATES EXIST ❌')                  AS DEDUP_STATUS

FROM TDM_RISK_MGMT_HUB_CLONE_PL5.MODELED.PL_GEN3_COL_FEATURES_2025

WHERE OBSERVATION_DATE BETWEEN '2025-11-01' AND '2025-11-30';
