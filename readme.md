mismatch_ids = [1646484, 1181069, 1385935, 2306809, 1503659, 1628721, 2553891]

# ─────────────────────────────────────────────────────────────
# CHECK 1: Duplicate rows in offline parquet for these loans?
#           Most common culprit — merge picks wrong row silently
# ─────────────────────────────────────────────────────────────

dupes = df[df['BUSINESS_LOAN_ID'].astype(str).isin([str(x) for x in mismatch_ids])]

print("=== All offline rows for mismatched loans ===")
print(dupes[['BUSINESS_LOAN_ID', 'BUSINESS_APPLICATION_ID', 'PL_USER_ID',
             'OBSERVATION_DATE', 'SCORE']].sort_values(
             ['BUSINESS_LOAN_ID', 'OBSERVATION_DATE']).to_string(index=False))

print("\n=== Row count per loan in offline df ===")
print(dupes.groupby('BUSINESS_LOAN_ID')['OBSERVATION_DATE'].count())


# ─────────────────────────────────────────────────────────────
# CHECK 2: Compare prod CYCLE_START vs all offline obs dates
#           Maybe offline has the loan but on a different date
# ─────────────────────────────────────────────────────────────

print("\n=== Prod CYCLE_START vs offline OBSERVATION_DATE for mismatches ===")
prod_miss = df_prod[df_prod['BUSINESS_LOAN_ID'].astype(str).isin([str(x) for x in mismatch_ids])]

for _, row in prod_miss.iterrows():
    loan_id = str(row['BUSINESS_LOAN_ID'])
    offline_rows = df[df['BUSINESS_LOAN_ID'].astype(str) == loan_id]
    offline_dates = offline_rows['OBSERVATION_DATE'].tolist()
    offline_scores = offline_rows['SCORE'].tolist()
    print(f"\nLoan {loan_id}:")
    print(f"  PROD_CYCLE_START = {row['PROD_CYCLE_START']}  |  PROD_SCORE = {row['PROD_SCORE']:.6f}")
    print(f"  Offline obs dates: {offline_dates}")
    print(f"  Offline scores   : {[round(s,6) for s in offline_scores]}")


# ─────────────────────────────────────────────────────────────
# CHECK 3: Are these loans appearing in multiple DQ buckets
#           in the offline df? (wrong model routing)
# ─────────────────────────────────────────────────────────────

if 'DQ_BUCKET' in df.columns:
    print("\n=== DQ_BUCKET in offline df for mismatched loans ===")
    print(dupes[['BUSINESS_LOAN_ID', 'OBSERVATION_DATE', 
                 'DQ_BUCKET', 'SCORE']].to_string(index=False))
