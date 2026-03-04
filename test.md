-- ─────────────────────────────────────────────────────────────
-- Full in-house feature comparison for 7 mismatch loans
-- Prod table vs Offline features table
-- ─────────────────────────────────────────────────────────────

WITH PROD AS (
    SELECT
        BUSINESS_LOAN_ID,
        BUSINESS_APPLICATION_ID,
        PL_USER_ID,
        CYCLE_START_DATE                                    AS OBSERVATION_DATE,
        SCORE                                               AS PROD_SCORE,

        -- In-house features available in prod table
        ATT_SEASONING,
        ATT_GROSS_INCOME,
        ATT_PAST_DUE_TO_SCHD,
        ATT_COBORROWER_IND,
        ATT_IS_AUTOPAY,
        ATT_IS_DUE_CHG,
        ATT_ORIG_GROSS_INT_RATE,
        ATT_ORIGINAL_PRIN,
        ATT_UBTI,
        ATT_PAST_DUE_TO_INCOME,
        ATT_REMAINING_BAL_RATIO,
        ATT_TOT_DQ_DAYS_CYC_RATIO_L1M,
        ATT_TOT_DQ_DAYS_CYC_RATIO_L3M,
        ATT_TOT_DQ_DAYS_CYC_RATIO_L6M,
        ATT_CONS_NODQCNT,
        ATT_CONS_NODQ2UP_CNT,
        ATT_CONS_NODQ3UP_CNT,
        ATT_CONS_NODQ4UP_CNT,
        ATT_CONS_NODQ5UP_CNT,
        ATT_DQCNT_3M,
        ATT_DQCNT_6M,
        ATT_DQCNT_12M,
        ATT_DQ2UP_CNT_3M,
        ATT_DQ2UP_CNT_6M,
        ATT_DQ2UP_CNT_12M,
        ATT_DQ4UP_CNT_3M,
        ATT_IS_DQ_L1M,
        ATT_IS_DQ2UP_L1M,
        ATT_IS_B1_WORST_DQ_L3M,
        ATT_IS_B1_WORST_DQ_L12M,
        ATT_IS_B2_WORST_DQ_L12M,
        ATT_IS_B3_L1M,
        ATT_IS_B4_WORST_DQ_L3M,
        WORST_DQ_L3M,
        WORST_DQ_L6M,
        ATT_NOAUTOPAY_CNT_3M,
        ATT_NOAUTOPAY_CNT_12M,
        ATT_IS_AUTOPAY_L6M,
        ATT_MS_LAST_NOAUTOPAY,
        ATT_REVERSED_CNT_3M,
        ATT_REVERSED_CNT_12M,
        ATT_IS_REVERSED_RENTRY,
        ATT3_IS_STRROLLIN_B4UP,
        ATT3_IS_STRROLLIN_B5,
        ATT3_ROLLIN_B1_CNT_12M,
        ATT3_ROLLIN_B2_CNT_12M,
        ATT3_ROLLIN_B3_CNT_12M,
        ATT3_DOWN_TOB0_CNT_12M,
        ATT_LAST_PAYMENT_AMOUNT_FIXED,
        ATT_PAST_DUE_AMT_FIXED,
        ATT_REL_REPAY_TERM_MON,
        ATT_MAIN,
        DQ_BUCKET

    FROM TDM_RISK_MGMT_HUB_CLONE_PL5.MODELED.PROD_NOV_SAMPLE
    WHERE BUSINESS_LOAN_ID IN (
        '1646484','1181069','1385935',
        '2306809','1503659','1628721','2553891'
    )
),

OFFLINE AS (
    SELECT
        BUSINESS_LOAN_ID,
        BUSINESS_APPLICATION_ID,
        PL_USER_ID,
        OBSERVATION_DATE,

        -- Same in-house features from offline table
        ATT_SEASONING,
        ATT_GROSS_INCOME,
        ATT_PAST_DUE_TO_SCHD,
        ATT_COBORROWER_IND,
        ATT_IS_AUTOPAY,
        ATT_IS_DUE_CHG,
        ATT_ORIG_GROSS_INT_RATE,
        ATT_ORIGINAL_PRIN,
        ATT_UBTI,
        ATT_PAST_DUE_TO_INCOME,
        ATT_REMAINING_BAL_RATIO,
        ATT_TOT_DQ_DAYS_CYC_RATIO_L1M,
        ATT_TOT_DQ_DAYS_CYC_RATIO_L3M,
        ATT_TOT_DQ_DAYS_CYC_RATIO_L6M,
        ATT_CONS_NODQCNT,
        ATT_CONS_NODQ2UP_CNT,
        ATT_CONS_NODQ3UP_CNT,
        ATT_CONS_NODQ4UP_CNT,
        ATT_CONS_NODQ5UP_CNT,
        ATT_DQCNT_3M,
        ATT_DQCNT_6M,
        ATT_DQCNT_12M,
        ATT_DQ2UP_CNT_3M,
        ATT_DQ2UP_CNT_6M,
        ATT_DQ2UP_CNT_12M,
        ATT_DQ4UP_CNT_3M,
        ATT_IS_DQ_L1M,
        ATT_IS_DQ2UP_L1M,
        ATT_IS_B1_WORST_DQ_L3M,
        ATT_IS_B1_WORST_DQ_L12M,
        ATT_IS_B2_WORST_DQ_L12M,
        ATT_IS_B3_L1M,
        ATT_IS_B4_WORST_DQ_L3M,
        WORST_DQ_L3M,
        WORST_DQ_L6M,
        ATT_NOAUTOPAY_CNT_3M,
        ATT_NOAUTOPAY_CNT_12M,
        ATT_IS_AUTOPAY_L6M,
        ATT_MS_LAST_NOAUTOPAY,
        ATT_REVERSED_CNT_3M,
        ATT_REVERSED_CNT_12M,
        ATT_IS_REVERSED_RENTRY,
        ATT3_IS_STRROLLIN_B4UP,
        ATT3_IS_STRROLLIN_B5,
        ATT3_ROLLIN_B1_CNT_12M,
        ATT3_ROLLIN_B2_CNT_12M,
        ATT3_ROLLIN_B3_CNT_12M,
        ATT3_DOWN_TOB0_CNT_12M,
        ATT_LAST_PAYMENT_AMOUNT_FIXED,
        ATT_PAST_DUE_AMT_FIXED,
        ATT_REL_REPAY_TERM_MON,
        ATT_MAIN,
        DQ_BUCKET

    FROM TDM_RISK_MGMT_HUB_CLONE_PL5.MODELED.PL_GEN3_COL_FEATURES_2025
    WHERE BUSINESS_LOAN_ID IN (
        '1646484','1181069','1385935',
        '2306809','1503659','1628721','2553891'
    )
    AND OBSERVATION_DATE BETWEEN '2025-11-01' AND '2025-11-30'
)

-- ─────────────────────────────────────────────────────────────
-- Side by side comparison with diff for every feature
-- ─────────────────────────────────────────────────────────────
SELECT
    p.BUSINESS_LOAN_ID,
    p.OBSERVATION_DATE,
    p.PROD_SCORE,
    p.DQ_BUCKET                                             AS PROD_DQ_BUCKET,
    o.DQ_BUCKET                                             AS OFFLINE_DQ_BUCKET,

    -- ATT_MAIN — tier based flag, key suspect
    p.ATT_MAIN                                              AS P_ATT_MAIN,
    o.ATT_MAIN                                              AS O_ATT_MAIN,
    p.ATT_MAIN - o.ATT_MAIN                                 AS DIFF_ATT_MAIN,

    -- Seasoning
    p.ATT_SEASONING                                         AS P_SEASONING,
    o.ATT_SEASONING                                         AS O_SEASONING,
    p.ATT_SEASONING - o.ATT_SEASONING                       AS DIFF_SEASONING,

    -- Coborrower — drives ATT3_SOFIDTI_REFRESH branch
    p.ATT_COBORROWER_IND                                    AS P_COBORROWER,
    o.ATT_COBORROWER_IND                                    AS O_COBORROWER,
    p.ATT_COBORROWER_IND - o.ATT_COBORROWER_IND             AS DIFF_COBORROWER,

    -- Income
    p.ATT_GROSS_INCOME                                      AS P_INCOME,
    o.ATT_GROSS_INCOME                                      AS O_INCOME,
    p.ATT_GROSS_INCOME - o.ATT_GROSS_INCOME                 AS DIFF_INCOME,

    -- Past due
    p.ATT_PAST_DUE_TO_SCHD                                  AS P_PAST_DUE_SCHD,
    o.ATT_PAST_DUE_TO_SCHD                                  AS O_PAST_DUE_SCHD,
    ROUND(p.ATT_PAST_DUE_TO_SCHD
          - o.ATT_PAST_DUE_TO_SCHD, 6)                      AS DIFF_PAST_DUE_SCHD,

    -- DQ ratio features
    ROUND(p.ATT_TOT_DQ_DAYS_CYC_RATIO_L1M
          - o.ATT_TOT_DQ_DAYS_CYC_RATIO_L1M, 6)             AS DIFF_DQ_RATIO_L1M,
    ROUND(p.ATT_TOT_DQ_DAYS_CYC_RATIO_L3M
          - o.ATT_TOT_DQ_DAYS_CYC_RATIO_L3M, 6)             AS DIFF_DQ_RATIO_L3M,

    -- DQ count features
    p.ATT_DQCNT_3M    - o.ATT_DQCNT_3M                     AS DIFF_DQCNT_3M,
    p.ATT_DQCNT_6M    - o.ATT_DQCNT_6M                     AS DIFF_DQCNT_6M,
    p.ATT_DQCNT_12M   - o.ATT_DQCNT_12M                    AS DIFF_DQCNT_12M,
    p.ATT_DQ2UP_CNT_3M  - o.ATT_DQ2UP_CNT_3M               AS DIFF_DQ2UP_3M,
    p.ATT_DQ2UP_CNT_6M  - o.ATT_DQ2UP_CNT_6M               AS DIFF_DQ2UP_6M,
    p.ATT_DQ2UP_CNT_12M - o.ATT_DQ2UP_CNT_12M              AS DIFF_DQ2UP_12M,

    -- Worst DQ
    p.WORST_DQ_L3M  - o.WORST_DQ_L3M                       AS DIFF_WORST_DQ_L3M,
    p.WORST_DQ_L6M  - o.WORST_DQ_L6M                       AS DIFF_WORST_DQ_L6M,

    -- Cons no DQ features
    p.ATT_CONS_NODQCNT    - o.ATT_CONS_NODQCNT              AS DIFF_CONS_NODQ,
    p.ATT_CONS_NODQ2UP_CNT - o.ATT_CONS_NODQ2UP_CNT        AS DIFF_CONS_NODQ2UP,
    p.ATT_CONS_NODQ3UP_CNT - o.ATT_CONS_NODQ3UP_CNT        AS DIFF_CONS_NODQ3UP,

    -- Autopay features
    p.ATT_IS_AUTOPAY      - o.ATT_IS_AUTOPAY                AS DIFF_AUTOPAY,
    p.ATT_NOAUTOPAY_CNT_12M - o.ATT_NOAUTOPAY_CNT_12M      AS DIFF_NOAUTOPAY_12M,
    p.ATT_MS_LAST_NOAUTOPAY - o.ATT_MS_LAST_NOAUTOPAY      AS DIFF_MS_LAST_NOAUTOPAY,

    -- Reversal features
    p.ATT_REVERSED_CNT_3M  - o.ATT_REVERSED_CNT_3M         AS DIFF_REVERSED_3M,
    p.ATT_REVERSED_CNT_12M - o.ATT_REVERSED_CNT_12M        AS DIFF_REVERSED_12M,

    -- Rollin features
    p.ATT3_ROLLIN_B1_CNT_12M - o.ATT3_ROLLIN_B1_CNT_12M   AS DIFF_ROLLIN_B1,
    p.ATT3_ROLLIN_B2_CNT_12M - o.ATT3_ROLLIN_B2_CNT_12M   AS DIFF_ROLLIN_B2,
    p.ATT3_ROLLIN_B3_CNT_12M - o.ATT3_ROLLIN_B3_CNT_12M   AS DIFF_ROLLIN_B3,
    p.ATT3_DOWN_TOB0_CNT_12M - o.ATT3_DOWN_TOB0_CNT_12M   AS DIFF_DOWN_TOB0,

    -- Financial ratios
    ROUND(p.ATT_UBTI
          - o.ATT_UBTI, 6)                                  AS DIFF_UBTI,
    ROUND(p.ATT_PAST_DUE_TO_INCOME
          - o.ATT_PAST_DUE_TO_INCOME, 6)                    AS DIFF_PAST_DUE_INCOME,
    ROUND(p.ATT_REMAINING_BAL_RATIO
          - o.ATT_REMAINING_BAL_RATIO, 6)                   AS DIFF_REM_BAL_RATIO,
    ROUND(p.ATT_REL_REPAY_TERM_MON
          - o.ATT_REL_REPAY_TERM_MON, 6)                    AS DIFF_REL_REPAY_TERM,
    p.ATT_PAST_DUE_AMT_FIXED - o.ATT_PAST_DUE_AMT_FIXED   AS DIFF_PAST_DUE_AMT,

    -- Overall mismatch flag — any diff > 0 across all features
    IFF(
        p.ATT_MAIN                  != o.ATT_MAIN
        OR p.ATT_COBORROWER_IND     != o.ATT_COBORROWER_IND
        OR p.ATT_SEASONING          != o.ATT_SEASONING
        OR p.ATT_GROSS_INCOME       != o.ATT_GROSS_INCOME
        OR p.ATT_DQCNT_3M           != o.ATT_DQCNT_3M
        OR p.ATT_DQCNT_12M          != o.ATT_DQCNT_12M
        OR p.ATT_DQ2UP_CNT_12M      != o.ATT_DQ2UP_CNT_12M
        OR p.WORST_DQ_L3M           != o.WORST_DQ_L3M
        OR p.ATT_CONS_NODQCNT       != o.ATT_CONS_NODQCNT
        OR p.ATT_REVERSED_CNT_12M   != o.ATT_REVERSED_CNT_12M
        OR p.ATT3_ROLLIN_B1_CNT_12M != o.ATT3_ROLLIN_B1_CNT_12M
        OR ABS(p.ATT_PAST_DUE_TO_SCHD
             - o.ATT_PAST_DUE_TO_SCHD) > 0.001,
        'INHOUSE FEATURES DIFFER ❌',
        'INHOUSE FEATURES MATCH ✅'
    )                                                        AS INHOUSE_STATUS

FROM PROD p

LEFT JOIN OFFLINE o
    ON  p.BUSINESS_LOAN_ID        = o.BUSINESS_LOAN_ID
    AND p.BUSINESS_APPLICATION_ID = o.BUSINESS_APPLICATION_ID
    AND p.PL_USER_ID              = o.PL_USER_ID
    AND p.OBSERVATION_DATE        = o.OBSERVATION_DATE

ORDER BY p.BUSINESS_LOAN_ID;
