-- ─────────────────────────────────────────────────────────────
-- Bureau feature comparison: Prod vs Offline for 7 mismatch loans
-- ─────────────────────────────────────────────────────────────

WITH PROD AS (
    SELECT
        BUSINESS_LOAN_ID,
        BUSINESS_APPLICATION_ID,
        PL_USER_ID,
        CYCLE_START_DATE            AS OBSERVATION_DATE,
        PROD_SCORE,
        EXP_AVA_DATE,

        -- Bureau attributes from prod
        VANTAGE_V3_SCORE, FICOCLV8_SCORE,
        ALL5820, MTF5820, ALX5839, MTX5839,
        ALL5320, BCX7110, PIL0438, FIP8320,
        ALL7519, ALL7517, IQF9540, ALL7518,
        ALL8552, BRC5620, REH7120, ALL5012,
        BCA8370, BCC7482, ALL2421, FIP0437,
        BCC3421, BCC7483, IQM9540, ALL2012,
        ALL8250, ALL8272, ALL0438, ALL8370,
        ALS0337, ILN4080, BCA8151, REV0318,
        ALL8154, ALL8171, BCC7708, AUA8370,
        STU5031, ALL8271, BCC8338, ALL2226,
        ALL4980, REV2126, REV5742

    FROM TDM_RISK_MGMT_HUB_CLONE_PL5.MODELED.PROD_NOV_SAMPLE
),

OFFLINE AS (
    SELECT
        BUSINESS_LOAN_ID,
        BUSINESS_APPLICATION_ID,
        PL_USER_ID,
        OBSERVATION_DATE,

        -- Bureau attributes from offline features table
        VANTAGE_V3_SCORE, FICOCLV8_SCORE,
        ALL5820, MTF5820, ALX5839, MTX5839,
        ALL5320, BCX7110, PIL0438, FIP8320,
        ALL7519, ALL7517, IQF9540, ALL7518,
        ALL8552, BRC5620, REH7120, ALL5012,
        BCA8370, BCC7482, ALL2421, FIP0437,
        BCC3421, BCC7483, IQM9540, ALL2012,
        ALL8250, ALL8272, ALL0438, ALL8370,
        ALS0337, ILN4080, BCA8151, REV0318,
        ALL8154, ALL8171, BCC7708, AUA8370,
        STU5031, ALL8271, BCC8338, ALL2226,
        ALL4980, REV2126, REV5742

    FROM TDM_RISK_MGMT_HUB_CLONE_PL5.MODELED.PL_GEN3_COL_FEATURES_2025
    WHERE BUSINESS_LOAN_ID IN (
        '1646484','1181069','1385935',
        '2306809','1503659','1628721','2553891'
    )
    AND OBSERVATION_DATE BETWEEN '2025-11-01' AND '2025-11-30'
)

SELECT
    p.BUSINESS_LOAN_ID,
    p.OBSERVATION_DATE,
    p.PROD_SCORE,
    p.EXP_AVA_DATE                          AS PROD_EXP_AVA_DATE,

    -- Vantage and FICO scores
    p.VANTAGE_V3_SCORE                      AS P_VANTAGE,
    o.VANTAGE_V3_SCORE                      AS O_VANTAGE,
    p.VANTAGE_V3_SCORE - o.VANTAGE_V3_SCORE AS DIFF_VANTAGE,

    p.FICOCLV8_SCORE                        AS P_FICO,
    o.FICOCLV8_SCORE                        AS O_FICO,
    p.FICOCLV8_SCORE - o.FICOCLV8_SCORE     AS DIFF_FICO,

    -- Key bureau attrs most likely to differ
    p.BCA8370   - o.BCA8370                 AS DIFF_BCA8370,
    p.ALL5012   - o.ALL5012                 AS DIFF_ALL5012,
    p.BCX7110   - o.BCX7110                 AS DIFF_BCX7110,
    p.IQF9540   - o.IQF9540                 AS DIFF_IQF9540,
    p.ALL8370   - o.ALL8370                 AS DIFF_ALL8370,
    p.ALL2012   - o.ALL2012                 AS DIFF_ALL2012,
    p.ALL8250   - o.ALL8250                 AS DIFF_ALL8250,
    p.ALL8272   - o.ALL8272                 AS DIFF_ALL8272,
    p.BCC7482   - o.BCC7482                 AS DIFF_BCC7482,
    p.ALL7518   - o.ALL7518                 AS DIFF_ALL7518,
    p.ALL7517   - o.ALL7517                 AS DIFF_ALL7517,
    p.ALL7519   - o.ALL7519                 AS DIFF_ALL7519,
    p.ALL5320   - o.ALL5320                 AS DIFF_ALL5320,
    p.BRC5620   - o.BRC5620                 AS DIFF_BRC5620,
    p.PIL0438   - o.PIL0438                 AS DIFF_PIL0438,
    p.REH7120   - o.REH7120                 AS DIFF_REH7120,
    p.ALL8552   - o.ALL8552                 AS DIFF_ALL8552,
    p.FIP0437   - o.FIP0437                 AS DIFF_FIP0437,
    p.BCC3421   - o.BCC3421                 AS DIFF_BCC3421,
    p.BCC7483   - o.BCC7483                 AS DIFF_BCC7483,
    p.IQM9540   - o.IQM9540                 AS DIFF_IQM9540,
    p.ALL0438   - o.ALL0438                 AS DIFF_ALL0438,
    p.ALS0337   - o.ALS0337                 AS DIFF_ALS0337,
    p.ILN4080   - o.ILN4080                 AS DIFF_ILN4080,
    p.BCA8151   - o.BCA8151                 AS DIFF_BCA8151,
    p.REV0318   - o.REV0318                 AS DIFF_REV0318,
    p.ALL8154   - o.ALL8154                 AS DIFF_ALL8154,
    p.ALL8171   - o.ALL8171                 AS DIFF_ALL8171,
    p.BCC7708   - o.BCC7708                 AS DIFF_BCC7708,
    p.AUA8370   - o.AUA8370                 AS DIFF_AUA8370,
    p.STU5031   - o.STU5031                 AS DIFF_STU5031,
    p.ALL8271   - o.ALL8271                 AS DIFF_ALL8271,
    p.BCC8338   - o.BCC8338                 AS DIFF_BCC8338,
    p.ALL2226   - o.ALL2226                 AS DIFF_ALL2226,
    p.ALL4980   - o.ALL4980                 AS DIFF_ALL4980,
    p.REV2126   - o.REV2126                 AS DIFF_REV2126,
    p.REV5742   - o.REV5742                 AS DIFF_REV5742,
    p.ALL5820   - o.ALL5820                 AS DIFF_ALL5820,
    p.MTF5820   - o.MTF5820                 AS DIFF_MTF5820,
    p.ALX5839   - o.ALX5839                 AS DIFF_ALX5839,
    p.MTX5839   - o.MTX5839                 AS DIFF_MTX5839,
    p.FIP8320   - o.FIP8320                 AS DIFF_FIP8320,

    -- Quick summary flag
    IFF(
        p.VANTAGE_V3_SCORE  = o.VANTAGE_V3_SCORE
        AND p.FICOCLV8_SCORE = o.FICOCLV8_SCORE
        AND p.BCA8370        = o.BCA8370
        AND p.ALL5012        = o.ALL5012
        AND p.BCX7110        = o.BCX7110
        AND p.IQF9540        = o.IQF9540
        AND p.ALL8370        = o.ALL8370
        AND p.ALL5320        = o.ALL5320,
        'BUREAU MATCH ✅',
        'BUREAU DIFFER ❌'
    )                                       AS BUREAU_STATUS

FROM PROD p
LEFT JOIN OFFLINE o
    ON  p.BUSINESS_LOAN_ID        = o.BUSINESS_LOAN_ID
    AND p.BUSINESS_APPLICATION_ID = o.BUSINESS_APPLICATION_ID
    AND p.PL_USER_ID              = o.PL_USER_ID
    AND p.OBSERVATION_DATE        = o.OBSERVATION_DATE

ORDER BY p.BUSINESS_LOAN_ID;
