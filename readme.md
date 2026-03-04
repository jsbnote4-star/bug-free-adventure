-- ============================================================
-- PL Gen3 Collection — Production vs Offline Validation
-- Purpose : Use PL_GEN3_RISK_SCORE (prod) as source of truth
--           and diff it against your offline feature/target/score tables
-- Scope   : November 2025 (ASOF_DATE Nov 2nd onwards)
-- ============================================================


-- ─────────────────────────────────────────────────────────────
-- STEP 1 : Pull a sample of accounts from the production table
--          for November.  Pick ~10 accounts spread across buckets.
-- ─────────────────────────────────────────────────────────────

CREATE OR REPLACE TEMPORARY TABLE PROD_NOV_SAMPLE AS

SELECT
    s.BUSINESS_LOAN_ID,
    s.BUSINESS_APPLICATION_ID,
    s.PL_USER_ID,
    s.ASOF_DATE                            AS PROD_ASOF_DATE,
    s.CYCLE_START_DATE                     AS PROD_CYCLE_START,
    -- OBSERVATION_DATE in offline = DATEADD(DAY,1, DUE_DATE_FIX2)
    -- DUE_DATE_FIX2 = CYCLE_START_DATE in prod → so offline OBS = CYCLE_START_DATE + 1
    DATEADD(DAY, 1, s.CYCLE_START_DATE)    AS EXPECTED_OFFLINE_OBS_DATE,
    s.ASOF_DATE                            AS EXPECTED_TARGET_OBS_DATE,
    s.DQ_BUCKET                            AS PROD_DQ_BUCKET,
    s.SCORE                                AS PROD_SCORE,
    s.VERSION                              AS PROD_VERSION,
    s.DAYS_DIFF                            AS PROD_DAYS_DIFF,
    s.ATT_SEASONING                        AS PROD_ATT_SEASONING,
    s.ATT_GROSS_INCOME                     AS PROD_ATT_GROSS_INCOME,
    s.ATT_PAST_DUE_TO_SCHD                 AS PROD_ATT_PAST_DUE_TO_SCHD,
    s.COMBINED_EXCLUSION_FLAG              AS PROD_EXCL_FLAG,
    s.NON_DELNQ_FLAG                       AS PROD_NON_DELNQ_FLAG,
    s.EXP_AVA_DATE                         AS PROD_EXP_AVA_DATE

FROM TDM_SERVICING.CLEANSED.PL_GEN3_RISK_SCORE s

WHERE s.ASOF_DATE >= '2025-11-02'
  AND s.ASOF_DATE <  '2025-12-01'
  AND s.DQ_BUCKET IN ('Bucket 1','Bucket 2','Bucket 3','Bucket 4+')
  AND s.SCORE > 0                          -- valid scores only, no error codes
  AND s.DAYS_DIFF <= 10                    -- freshest score per cycle
  AND s.COMBINED_EXCLUSION_FLAG = 0

QUALIFY ROW_NUMBER() OVER (
    PARTITION BY s.BUSINESS_LOAN_ID,
                 s.BUSINESS_APPLICATION_ID,
                 s.PL_USER_ID,
                 LEFT(CAST(s.ASOF_DATE AS STRING), 7)  -- one row per loan per month
    ORDER BY s.DAYS_DIFF ASC
) = 1

ORDER BY s.DQ_BUCKET, RANDOM()
LIMIT 20;   -- grab 20, you can filter to any 2–5 for manual spot-check


-- ─────────────────────────────────────────────────────────────
-- STEP 2 : FEATURE VALIDATION
--          Compare a handful of key features between prod and offline
-- ─────────────────────────────────────────────────────────────

SELECT
    p.BUSINESS_LOAN_ID,
    p.BUSINESS_APPLICATION_ID,
    p.PL_USER_ID,

    -- ── Date alignment check ──────────────────────────────────
    p.PROD_ASOF_DATE,
    p.PROD_CYCLE_START,
    p.EXPECTED_OFFLINE_OBS_DATE,
    f.OBSERVATION_DATE                     AS OFFLINE_OBS_DATE,
    IFF(p.PROD_ASOF_DATE = f.OBSERVATION_DATE,
        'MATCH', 'MISMATCH')               AS OBS_DATE_MATCH,

    -- ── Bucket check ─────────────────────────────────────────
    p.PROD_DQ_BUCKET,
    f.DQ_BUCKET                            AS OFFLINE_DQ_BUCKET,
    IFF(p.PROD_DQ_BUCKET = f.DQ_BUCKET,
        'MATCH', 'MISMATCH')               AS BUCKET_MATCH,

    -- ── Key feature diffs ────────────────────────────────────
    p.PROD_ATT_SEASONING,
    f.ATT_SEASONING                        AS OFFLINE_ATT_SEASONING,
    ROUND(p.PROD_ATT_SEASONING
          - f.ATT_SEASONING, 4)            AS SEASONING_DIFF,

    p.PROD_ATT_GROSS_INCOME,
    f.ATT_GROSS_INCOME                     AS OFFLINE_ATT_GROSS_INCOME,
    ROUND(p.PROD_ATT_GROSS_INCOME
          - f.ATT_GROSS_INCOME, 2)         AS INCOME_DIFF,

    p.PROD_ATT_PAST_DUE_TO_SCHD,
    f.ATT_PAST_DUE_TO_SCHD                 AS OFFLINE_ATT_PAST_DUE,
    ROUND(p.PROD_ATT_PAST_DUE_TO_SCHD
          - f.ATT_PAST_DUE_TO_SCHD, 4)    AS PAST_DUE_DIFF,

    -- ── Experian flag check ───────────────────────────────────
    p.PROD_EXP_AVA_DATE,
    f.EXP_AVA_DATE                         AS OFFLINE_EXP_AVA_DATE,
    IFF(p.PROD_EXP_AVA_DATE = f.EXP_AVA_DATE,
        'MATCH', 'MISMATCH')               AS EXP_DATE_MATCH

FROM PROD_NOV_SAMPLE p

LEFT JOIN TDM_SANDBOX.SANDBOX.PL_GEN3_COL_FEATURES_2025 f
    ON  p.BUSINESS_LOAN_ID        = f.BUSINESS_LOAN_ID
    AND p.BUSINESS_APPLICATION_ID = f.BUSINESS_APPLICATION_ID
    AND p.PL_USER_ID              = f.PL_USER_ID
    AND p.PROD_ASOF_DATE          = f.OBSERVATION_DATE   -- prod ASOF = offline OBS

ORDER BY p.PROD_DQ_BUCKET, p.BUSINESS_LOAN_ID;


-- ─────────────────────────────────────────────────────────────
-- STEP 3 : SCORE VALIDATION
--          Compare production score vs what your pipeline produced
-- ─────────────────────────────────────────────────────────────

SELECT
    p.BUSINESS_LOAN_ID,
    p.BUSINESS_APPLICATION_ID,
    p.PL_USER_ID,
    p.PROD_ASOF_DATE,
    p.PROD_DQ_BUCKET,

    -- ── Score comparison ─────────────────────────────────────
    p.PROD_SCORE,
    f.SCORE                                AS OFFLINE_SCORE,
    ROUND(p.PROD_SCORE - f.SCORE, 6)       AS SCORE_DIFF,
    IFF(ABS(p.PROD_SCORE - f.SCORE) < 0.0001,
        'MATCH', 'MISMATCH')               AS SCORE_MATCH,

    -- ── Error/exclusion flag alignment ───────────────────────
    p.PROD_EXCL_FLAG,
    f.COMBINED_EXCLUSION_FLAG              AS OFFLINE_EXCL_FLAG,
    IFF(p.PROD_EXCL_FLAG = f.COMBINED_EXCLUSION_FLAG,
        'MATCH', 'MISMATCH')               AS EXCL_FLAG_MATCH,

    p.PROD_VERSION,
    f.VERSION                              AS OFFLINE_VERSION

FROM PROD_NOV_SAMPLE p

-- Join your offline scored parquet — load it into Snowflake first,
-- or replace with your Snowflake table name if already uploaded
LEFT JOIN TDM_SANDBOX.SANDBOX.PL_GEN3_SCORED_2025 f
    ON  p.BUSINESS_LOAN_ID        = f.BUSINESS_LOAN_ID
    AND p.BUSINESS_APPLICATION_ID = f.BUSINESS_APPLICATION_ID
    AND p.PL_USER_ID              = f.PL_USER_ID
    AND p.PROD_ASOF_DATE          = f.OBSERVATION_DATE

ORDER BY SCORE_MATCH DESC, p.PROD_DQ_BUCKET;


-- ─────────────────────────────────────────────────────────────
-- STEP 4 : TARGET VALIDATION
--          Compare production-joined targets vs offline targets
-- ─────────────────────────────────────────────────────────────

SELECT
    p.BUSINESS_LOAN_ID,
    p.BUSINESS_APPLICATION_ID,
    p.PL_USER_ID,
    p.PROD_ASOF_DATE,
    p.PROD_DQ_BUCKET,

    -- ── V4 targets ───────────────────────────────────────────
    t.B2_2M_V4                             AS TARGET_B2_2M_V4,
    t.B4_2M_V4                             AS TARGET_B4_2M_V4,
    t.B3_6M_V4                             AS TARGET_B3_6M_V4,
    t.CO_3M_V4                             AS TARGET_CO_3M_V4,
    t.CO_6M_V4                             AS TARGET_CO_6M_V4,

    -- ── V3 targets (for reference) ───────────────────────────
    t.B2_2M_V3                             AS TARGET_B2_2M_V3,
    t.CO_3M_V3                             AS TARGET_CO_3M_V3,

    -- ── Target availability check ────────────────────────────
    IFF(t.BUSINESS_LOAN_ID IS NULL,
        'NO TARGET ROW', 'HAS TARGET')     AS TARGET_JOIN_STATUS,

    -- ── For Bucket 1 accounts: did bad event happen? ─────────
    CASE
        WHEN p.PROD_DQ_BUCKET = 'Bucket 1' THEN t.B2_2M_V4
        WHEN p.PROD_DQ_BUCKET = 'Bucket 2' THEN t.B4_2M_V4
        WHEN p.PROD_DQ_BUCKET = 'Bucket 3' THEN t.B3_6M_V4
        WHEN p.PROD_DQ_BUCKET = 'Bucket 4+' THEN t.CO_3M_V4
    END                                    AS PRIMARY_TARGET_FOR_BUCKET,

    p.PROD_SCORE

FROM PROD_NOV_SAMPLE p

LEFT JOIN TDM_SANDBOX.SANDBOX.PL_PORT_COL_TARGET_2025 t
    ON  p.BUSINESS_LOAN_ID        = t.BUSINESS_LOAN_ID
    AND p.BUSINESS_APPLICATION_ID = t.BUSINESS_APPLICATION_ID
    AND p.PL_USER_ID              = t.USER_ID
    AND p.PROD_ASOF_DATE          = t.OBSERVATION_DATE

ORDER BY p.PROD_DQ_BUCKET, p.BUSINESS_LOAN_ID;


-- ─────────────────────────────────────────────────────────────
-- STEP 5 : ACCOUNT-LEVEL SPOT CHECK
--          Full side-by-side for 2-3 specific loan IDs
--          Replace the loan IDs below with ones from your sample
-- ─────────────────────────────────────────────────────────────

WITH PROD AS (
    SELECT
        BUSINESS_LOAN_ID, BUSINESS_APPLICATION_ID, PL_USER_ID,
        ASOF_DATE, DQ_BUCKET, SCORE AS PROD_SCORE,
        ATT_SEASONING, ATT_GROSS_INCOME, ATT_PAST_DUE_TO_SCHD,
        COMBINED_EXCLUSION_FLAG, DAYS_DIFF
    FROM TDM_SERVICING.CLEANSED.PL_GEN3_RISK_SCORE
    WHERE ASOF_DATE >= '2025-11-02'
      AND DAYS_DIFF <= 10
      AND SCORE > 0
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY BUSINESS_LOAN_ID, BUSINESS_APPLICATION_ID, PL_USER_ID,
                     LEFT(CAST(ASOF_DATE AS STRING), 7)
        ORDER BY DAYS_DIFF ASC
    ) = 1
),

OFFLINE_FEAT AS (
    SELECT
        BUSINESS_LOAN_ID, BUSINESS_APPLICATION_ID, PL_USER_ID,
        OBSERVATION_DATE, DQ_BUCKET, ATT_SEASONING,
        ATT_GROSS_INCOME, ATT_PAST_DUE_TO_SCHD
    FROM TDM_SANDBOX.SANDBOX.PL_GEN3_COL_FEATURES_2025
),

OFFLINE_TARGET AS (
    SELECT
        BUSINESS_LOAN_ID, BUSINESS_APPLICATION_ID, USER_ID,
        OBSERVATION_DATE,
        B2_2M_V4, B4_2M_V4, B3_6M_V4, CO_3M_V4, CO_6M_V4,
        B2_2M_V3, CO_3M_V3
    FROM TDM_SANDBOX.SANDBOX.PL_PORT_COL_TARGET_2025
),

OFFLINE_SCORED AS (
    -- your offline scored parquet uploaded to Snowflake
    SELECT
        BUSINESS_LOAN_ID, BUSINESS_APPLICATION_ID, PL_USER_ID,
        OBSERVATION_DATE, SCORE AS OFFLINE_SCORE
    FROM TDM_SANDBOX.SANDBOX.PL_GEN3_SCORED_2025
)

SELECT
    p.BUSINESS_LOAN_ID,
    p.BUSINESS_APPLICATION_ID,
    p.ASOF_DATE,
    p.DQ_BUCKET,

    -- ── Features ─────────────────────────────────────────────
    p.ATT_SEASONING          AS F_PROD,
    f.ATT_SEASONING          AS F_OFFLINE,
    p.ATT_SEASONING - f.ATT_SEASONING  AS F_DIFF,

    -- ── Scores ───────────────────────────────────────────────
    p.PROD_SCORE,
    s.OFFLINE_SCORE,
    ROUND(p.PROD_SCORE - s.OFFLINE_SCORE, 6)  AS SCORE_DIFF,

    -- ── Targets (from offline target table) ──────────────────
    t.B2_2M_V4,
    t.B4_2M_V4,
    t.B3_6M_V4,
    t.CO_3M_V4,

    -- ── Overall status ───────────────────────────────────────
    CASE
        WHEN f.BUSINESS_LOAN_ID IS NULL       THEN 'MISSING FROM OFFLINE FEATURES'
        WHEN s.BUSINESS_LOAN_ID IS NULL       THEN 'MISSING FROM OFFLINE SCORES'
        WHEN t.BUSINESS_LOAN_ID IS NULL       THEN 'MISSING FROM OFFLINE TARGETS'
        WHEN ABS(p.PROD_SCORE - s.OFFLINE_SCORE) > 0.001 THEN 'SCORE MISMATCH'
        WHEN p.DQ_BUCKET != f.DQ_BUCKET       THEN 'BUCKET MISMATCH'
        ELSE 'ALL GOOD'
    END                      AS VALIDATION_STATUS

FROM PROD p

LEFT JOIN OFFLINE_FEAT f
    ON  p.BUSINESS_LOAN_ID        = f.BUSINESS_LOAN_ID
    AND p.BUSINESS_APPLICATION_ID = f.BUSINESS_APPLICATION_ID
    AND p.PL_USER_ID              = f.PL_USER_ID
    AND p.ASOF_DATE               = f.OBSERVATION_DATE

LEFT JOIN OFFLINE_SCORED s
    ON  p.BUSINESS_LOAN_ID        = s.BUSINESS_LOAN_ID
    AND p.BUSINESS_APPLICATION_ID = s.BUSINESS_APPLICATION_ID
    AND p.PL_USER_ID              = s.PL_USER_ID
    AND p.ASOF_DATE               = s.OBSERVATION_DATE

LEFT JOIN OFFLINE_TARGET t
    ON  p.BUSINESS_LOAN_ID        = t.BUSINESS_LOAN_ID
    AND p.BUSINESS_APPLICATION_ID = t.BUSINESS_APPLICATION_ID
    AND p.PL_USER_ID              = t.USER_ID
    AND p.ASOF_DATE               = t.OBSERVATION_DATE

-- ── Replace with actual loan IDs from your Step 1 sample ─────
WHERE p.BUSINESS_LOAN_ID IN (
    '<LOAN_ID_1>',
    '<LOAN_ID_2>',
    '<LOAN_ID_3>'
)

ORDER BY p.BUSINESS_LOAN_ID, p.ASOF_DATE;
