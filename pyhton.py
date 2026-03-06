# Load benchmark
df_bench = pd.read_parquet('s3://sofi-data-science/mraj/pl_collection_b4_dr2_12jun25_processed.parquet')
df_bench.columns = df_bench.columns.str.upper()
benchmark_b4 = df_bench[df_bench['ATT_MAIN'] == 1]

# Load actual, preprocess, filter to same period as benchmark
df_raw = pd.read_parquet('s3://sofi-data-science/jbhagat/pl_collection_scored_data_Q4_2025_JB.parquet')
df_raw.columns = df_raw.columns.str.upper()
df_raw['month'] = pd.to_datetime(df_raw['OBSERVATION_DATE']).dt.to_period('M').astype(str)

# Filter actual to THE SAME window as benchmark (Apr-Sep 2024)
df_b4_same_period = df_raw[
    (df_raw['DQ_BUCKET'] == 'Bucket 4+') &
    (df_raw['NON_PRIME_FLAG'] == 0) &
    (df_raw['month'] >= '2024-04') &
    (df_raw['month'] <= '2024-09')
].copy()

# Preprocess
from preprocess import b4_preprocess
df_b4_processed = b4_preprocess(df_b4_same_period)

# Compare distributions
for var in ['ALL8154', 'ATT_TOT_DQ_DAYS_CYC_RATIO_L6M']:
    print(f"\n{var}")
    print(f"  Benchmark (OOT2, ATT_MAIN=1) : mean={benchmark_b4[var].mean():.4f}, "
          f"median={benchmark_b4[var].median():.4f}, n={len(benchmark_b4)}")
    print(f"  Actual (Apr-Sep 2024, prime)  : mean={df_b4_processed[var].mean():.4f}, "
          f"median={df_b4_processed[var].median():.4f}, n={len(df_b4_processed)}")
