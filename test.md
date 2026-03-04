-- ─────────────────────────────────────────────────────────────
-- PROD_NOV_SAMPLE — full columns, hardcoded to 7 mismatch loans
-- ─────────────────────────────────────────────────────────────

CREATE OR REPLACE TEMPORARY TABLE TDM_RISK_MGMT_HUB_CLONE_PL5.MODELED.PROD_NOV_SAMPLE AS

SELECT
    s.BUSINESS_LOAN_ID,
    s.BUSINESS_APPLICATION_ID,
    s.PL_USER_ID,
    s.ASOF_DATE,
    s.TIER,
    s.ATT_MAIN,
    s.DQ_BUCKET,
    s.CYCLE_START_DATE,
    s.ATT_COBORROWER_IND,
    s.ATT_ORIGINAL_PRIN,
    s.ATT2_ORIG_VANTAGE,
    s.ATT2_ORIG_FICO,

    -- ── In-house DQ features ──────────────────────────────
    s.ATT_IS_B1_WORST_DQ_L12M,
    s.ATT_TOT_DQ_DAYS_CYC_RATIO_L3M,
    s.ATT_CONS_NODQCNT,
    s.ATT_NOAUTOPAY_CNT_12M,
    s.ATT_PAST_DUE_TO_SCHD,
    s.ATT_IS_AUTOPAY,
    s.ATT_IS_B1_WORST_DQ_L3M,
    s.ATT3_ROLLIN_B1_CNT_12M,
    s.ATT_TOT_DQ_DAYS_CYC_RATIO_L1M,
    s.ATT_IS_AUTOPAY_L6M,
    s.ATT_ORIG_GROSS_INT_RATE,
    s.ATT_IS_REVERSED_RENTRY,
    s.ATT_DQCNT_3M,
    s.ATT_MS_LAST_NOAUTOPAY,
    s.ATT_UBTI,
    s.ATT_CONS_NODQ2UP_CNT,
    s.ATT_SEASONING,
    s.ATT_IS_DUE_CHG,
    s.ATT_DQ2UP_CNT_6M,
    s.WORST_DQ_L3M,
    s.ATT_DQ2UP_CNT_3M,
    s.ATT_GROSS_INCOME,
    s.ATT_CONS_NODQ3UP_CNT,
    s.ATT_IS_DQ2UP_L1M,
    s.WORST_DQ_L6M,
    s.ATT_DQCNT_6M,
    s.ATT_NOAUTOPAY_CNT_3M,
    s.ATT_IS_B2_WORST_DQ_L12M,
    s.ATT_REVERSED_CNT_3M,
    s.ATT_CONS_NODQ4UP_CNT,
    s.ATT_DQ2UP_CNT_12M,
    s.ATT_REVERSED_CNT_12M,
    s.ATT3_ROLLIN_B2_CNT_12M,
    s.ATT_DQ4UP_CNT_3M,
    s.ATT_PAST_DUE_TO_INCOME,
    s.ATT3_DOWN_TOB0_CNT_12M,
    s.ATT_IS_B3_L1M,
    s.ATT_REL_REPAY_TERM_MON,
    s.ATT_CONS_NODQ5UP_CNT,
    s.ATT_LAST_PAYMENT_AMOUNT_FIXED,
    s.ATT3_IS_STRROLLIN_B4UP,
    s.ATT_DQCNT_12M,
    s.ATT3_IS_STRROLLIN_B5,
    s.ATT_IS_B4_WORST_DQ_L3M,
    s.ATT_PAST_DUE_AMT_FIXED,
    s.ATT3_ROLLIN_B3_CNT_12M,
    s.ATT_TOT_DQ_DAYS_CYC_RATIO_L6M,
    s.ATT_REMAINING_BAL_RATIO,

    -- ── Experian date + score features ───────────────────
    s.EXP_AVA_DATE,
    s.VANTAGE_V3_SCORE,
    s.FICOCLV8_SCORE,

    -- ── Experian bureau attributes ────────────────────────
    s.ALL5820, s.MTF5820, s.ALX5839, s.MTX5839,
    s.ALL5320, s.BCX7110, s.PIL0438, s.FIP8320,
    s.ALL7519, s.ALL7517, s.IQF9540, s.ALL7518,
    s.ALL8552, s.BRC5620, s.REH7120, s.ALL5012,
    s.BCA8370, s.BCC7482, s.ALL2421, s.FIP0437,
    s.BCC3421, s.BCC7483, s.IQM9540, s.ALL2012,
    s.ALL8250, s.ALL8272, s.ALL0438, s.ALL8370,
    s.ALS0337, s.ILN4080, s.BCA8151, s.REV0318,
    s.ALL8154, s.ALL8171, s.BCC7708, s.AUA8370,
    s.STU5031, s.ALL8271, s.BCC8338, s.ALL2226,
    s.ALL4980, s.REV2126, s.REV5742,

    -- ── Exclusion flags ───────────────────────────────────
    s.NON_DELNQ_FLAG,
    s.LOW_PRINCIPAL_FLAG,
    s.MOB_LT3_FLAG,
    s.IS_DECEASED,
    s.IS_FRAUD_1,
    s.COMBINED_EXCLUSION_FLAG,

    -- ── Score + metadata ──────────────────────────────────
    s.SCORE                         AS PROD_SCORE,
    s.DAYS_DIFF,
    s.VERSION

FROM TDM_SERVICING.CLEANSED.PL_GEN3_RISK_SCORE s

WHERE s.BUSINESS_LOAN_ID IN (
    '1646484', '1181069', '1385935',
    '2306809', '1503659', '1628721', '2553891'
)
AND s.ASOF_DATE  >= '2025-11-02'
AND s.ASOF_DATE  <  '2025-12-01'
AND s.DQ_BUCKET  IN ('Bucket 1', 'Bucket 2', 'Bucket 3', 'Bucket 4+')
AND s.SCORE      >  0
AND s.DAYS_DIFF  <= 10

QUALIFY ROW_NUMBER() OVER (
    PARTITION BY s.BUSINESS_LOAN_ID,
                 s.BUSINESS_APPLICATION_ID,
                 s.PL_USER_ID,
                 LEFT(CAST(s.ASOF_DATE AS STRING), 7)
    ORDER BY s.DAYS_DIFF ASC   -- freshest score per cycle
) = 1

ORDER BY s.BUSINESS_LOAN_ID;
