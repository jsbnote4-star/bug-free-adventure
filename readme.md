import pandas as pd
import numpy as np

# ─────────────────────────────────────────────────────────────
# STEP 1: Load your offline scored parquet (already done — just df)
# ─────────────────────────────────────────────────────────────
# df is already loaded, e.g.:
# df = pd.read_parquet("pl_collection_scored_data_28oct25.parquet")

# Quick check — see what columns and date format you have
print("Offline df shape:", df.shape)
print("Offline df columns:", df.columns.tolist())
print("Offline df dtypes:\n", df[['BUSINESS_LOAN_ID', 'BUSINESS_APPLICATION_ID', 
                                    'PL_USER_ID', 'OBSERVATION_DATE', 'SCORE']].dtypes)
print("\nSample dates:", df['OBSERVATION_DATE'].head(3).tolist())


# ─────────────────────────────────────────────────────────────
# STEP 2: Load the prod sample from Snowflake
#         (the same 20 accounts from PROD_NOV_SAMPLE)
# ─────────────────────────────────────────────────────────────
import snowflake.connector
from snowflake.connector.pandas_tools import pd_read_chunks

# Reuse your existing connection
# conn = snowflake.connector.connect(...)

prod_query = """
    SELECT
        BUSINESS_LOAN_ID,
        BUSINESS_APPLICATION_ID,
        PL_USER_ID,
        ASOF_DATE           AS PROD_ASOF_DATE,
        CYCLE_START_DATE    AS PROD_CYCLE_START,
        SCORE               AS PROD_SCORE,
        DQ_BUCKET           AS PROD_DQ_BUCKET,
        DAYS_DIFF
    FROM TDM_RISK_MGMT_HUB_CLONE_PL5.MODELED.PROD_NOV_SAMPLE
"""

df_prod = pd.read_sql(prod_query, conn)
print("\nProd sample shape:", df_prod.shape)
print("Prod sample dtypes:\n", df_prod[['BUSINESS_LOAN_ID', 'PROD_CYCLE_START', 'PROD_SCORE']].dtypes)


# ─────────────────────────────────────────────────────────────
# STEP 3: Align date types before joining
#         OBSERVATION_DATE and PROD_CYCLE_START must be same type
# ─────────────────────────────────────────────────────────────

# Normalize both to date only (no timestamp component)
df['OBSERVATION_DATE']         = pd.to_datetime(df['OBSERVATION_DATE']).dt.date
df_prod['PROD_CYCLE_START']    = pd.to_datetime(df_prod['PROD_CYCLE_START']).dt.date

# Normalize key columns to same type
for col in ['BUSINESS_LOAN_ID', 'BUSINESS_APPLICATION_ID', 'PL_USER_ID']:
    df[col]      = df[col].astype(str).str.strip()
    df_prod[col] = df_prod[col].astype(str).str.strip()


# ─────────────────────────────────────────────────────────────
# STEP 4: Merge prod sample with offline scores
#         Join key: LOAN_ID + APP_ID + USER_ID + CYCLE_START = OBSERVATION_DATE
# ─────────────────────────────────────────────────────────────

df_validation = df_prod.merge(
    df[['BUSINESS_LOAN_ID', 'BUSINESS_APPLICATION_ID', 'PL_USER_ID',
        'OBSERVATION_DATE', 'SCORE']].rename(columns={'SCORE': 'OFFLINE_SCORE'}),
    left_on  = ['BUSINESS_LOAN_ID', 'BUSINESS_APPLICATION_ID', 'PL_USER_ID', 'PROD_CYCLE_START'],
    right_on = ['BUSINESS_LOAN_ID', 'BUSINESS_APPLICATION_ID', 'PL_USER_ID', 'OBSERVATION_DATE'],
    how      = 'left'
)


# ─────────────────────────────────────────────────────────────
# STEP 5: Compute score diff and match flag
# ─────────────────────────────────────────────────────────────

df_validation['SCORE_DIFF']  = (df_validation['PROD_SCORE'] - df_validation['OFFLINE_SCORE']).round(6)
df_validation['ABS_DIFF']    = df_validation['SCORE_DIFF'].abs()
df_validation['SCORE_MATCH'] = np.where(df_validation['ABS_DIFF'] < 0.0001, 'MATCH', 'MISMATCH')
df_validation['JOIN_STATUS'] = np.where(df_validation['OFFLINE_SCORE'].isna(), 
                                         'NOT FOUND IN OFFLINE', 'FOUND')

# ─────────────────────────────────────────────────────────────
# STEP 6: Display results
# ─────────────────────────────────────────────────────────────

display_cols = [
    'BUSINESS_LOAN_ID', 'BUSINESS_APPLICATION_ID', 'PL_USER_ID',
    'PROD_CYCLE_START', 'PROD_DQ_BUCKET',
    'PROD_SCORE', 'OFFLINE_SCORE',
    'SCORE_DIFF', 'SCORE_MATCH', 'JOIN_STATUS'
]

print("\n===== SCORE VALIDATION RESULTS =====")
print(df_validation[display_cols].to_string(index=False))

# ─────────────────────────────────────────────────────────────
# STEP 7: Summary stats
# ─────────────────────────────────────────────────────────────

total       = len(df_validation)
found       = (df_validation['JOIN_STATUS'] == 'FOUND').sum()
matched     = (df_validation['SCORE_MATCH'] == 'MATCH').sum()
mismatched  = (df_validation['SCORE_MATCH'] == 'MISMATCH').sum()
not_found   = (df_validation['JOIN_STATUS'] == 'NOT FOUND IN OFFLINE').sum()

print("\n===== SUMMARY =====")
print(f"Total prod accounts    : {total}")
print(f"Found in offline df    : {found}  ({found/total*100:.1f}%)")
print(f"  → Score MATCH        : {matched}")
print(f"  → Score MISMATCH     : {mismatched}")
print(f"Not found in offline   : {not_found}")

if mismatched > 0:
    print("\n--- Mismatched rows ---")
    print(df_validation[df_validation['SCORE_MATCH'] == 'MISMATCH'][display_cols].to_string(index=False))

if not_found > 0:
    print("\n--- Accounts not found in offline df ---")
    print(df_validation[df_validation['JOIN_STATUS'] == 'NOT FOUND IN OFFLINE'][display_cols].to_string(index=False))
