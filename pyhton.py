# Load the raw scored parquet
df_scored = pd.read_parquet('s3://sofi-data-science/jbhagat/pl_collection_scored_data_Q4_2025_JB.parquet')
df_scored.columns = df_scored.columns.str.upper()

# Load benchmark (already preprocessed)
df_bench = pd.read_parquet('s3://sofi-data-science/mraj/pl_collection_b4_dr2_12jun25_processed.parquet')
df_bench.columns = df_bench.columns.str.upper()
benchmark_b4 = df_bench[df_bench['ATT_MAIN'] == 1]

# Check the two breaching features BEFORE any PSI-pipeline preprocessing
for var in ['ALL8154', 'ATT_TOT_DQ_DAYS_CYC_RATIO_L6M']:
    df_b4_raw = df_scored[df_scored['DQ_BUCKET'] == 'Bucket 4+']
    
    print(f"\n{var}")
    print(f"  Benchmark  — min: {benchmark_b4[var].min():.4f}, max: {benchmark_b4[var].max():.4f}, "
          f"mean: {benchmark_b4[var].mean():.4f}, nulls: {benchmark_b4[var].isna().sum()}")
    print(f"  Scored raw — min: {df_b4_raw[var].min():.4f}, max: {df_b4_raw[var].max():.4f}, "
          f"mean: {df_b4_raw[var].mean():.4f}, nulls: {df_b4_raw[var].isna().sum()}")
