"""
PSI Feature Distribution Diagnostic
====================================
Generates a detailed percentile + PSI table for any feature, 
comparing benchmark (all months) vs current (monitored months).

Output matches the format:
Bucket | Feature | Month | Dataset | Obs | Missing_Rate | P05 | P25 | Median | P75 | P95 | Mean | PSI
"""

import pandas as pd
import numpy as np
import warnings
warnings.filterwarnings('ignore')


# ============================================================================
# PSI HELPER (same logic as your pipeline)
# ============================================================================

def calculate_psi_continuous(expected_arr, actual_arr, buckets=10):
    """Calculate PSI between two arrays using quantile binning on expected."""
    expected_arr = expected_arr[~np.isnan(expected_arr)]
    actual_arr   = actual_arr[~np.isnan(actual_arr)]

    if len(expected_arr) == 0 or len(actual_arr) == 0:
        return np.nan

    breakpoints = np.unique(np.percentile(expected_arr, np.linspace(0, 100, buckets + 1)))

    if len(breakpoints) < 2:
        return np.nan

    e_counts = np.histogram(expected_arr, breakpoints)[0]
    a_counts = np.histogram(actual_arr,   breakpoints)[0]

    e_pct = e_counts / len(expected_arr)
    a_pct = a_counts / len(actual_arr)

    psi = 0.0
    for e, a in zip(e_pct, a_pct):
        e = e if e > 0 else 0.0001
        a = a if a > 0 else 0.0001
        psi += (a - e) * np.log(a / e)

    return psi


def calculate_psi_categorical(ref_series, new_series):
    """Calculate PSI for a categorical feature."""
    ref_counts = ref_series.value_counts(normalize=True)
    new_counts = new_series.value_counts(normalize=True)
    all_cats   = set(ref_counts.index) | set(new_counts.index)

    psi = 0.0
    for cat in all_cats:
        e = ref_counts.get(cat, 0)
        a = new_counts.get(cat, 0)
        e = e if e > 0 else 0.0001
        a = a if a > 0 else 0.0001
        psi += (a - e) * np.log(a / e)
    return psi


# ============================================================================
# CORE DIAGNOSTIC FUNCTION
# ============================================================================

def build_distribution_table(
    benchmark_df: pd.DataFrame,
    actual_df: pd.DataFrame,
    features: list,
    bucket_name: str,
    categorical_vars: list = None,
    date_col: str = 'month',
    benchmark_label: str = 'Benchmark',
    percentiles: list = [5, 25, 50, 75, 95]
) -> pd.DataFrame:
    """
    For each feature, produce one row per month of actual data
    plus one row per month of benchmark data, with percentile stats and PSI.

    PSI is always calculated: benchmark_overall vs that specific month's data.
    """
    if categorical_vars is None:
        categorical_vars = []

    rows = []
    p_labels = [f'P{p:02d}' for p in percentiles]

    def get_stats(series, ref_series=None, is_cat=False):
        """Return dict of stats for a series. PSI requires ref_series."""
        n     = len(series)
        n_mis = series.isna().sum()
        mis_r = n_mis / n if n > 0 else np.nan

        if is_cat:
            pcts = {lbl: np.nan for lbl in p_labels}
            mean_val = np.nan
        else:
            clean = series.dropna()
            pcts  = {f'P{p:02d}': np.percentile(clean, p) if len(clean) > 0 else np.nan
                     for p in percentiles}
            mean_val = clean.mean() if len(clean) > 0 else np.nan

        psi_val = np.nan
        if ref_series is not None:
            if is_cat:
                psi_val = calculate_psi_categorical(ref_series.dropna(), series.dropna())
            else:
                psi_val = calculate_psi_continuous(ref_series.dropna().values,
                                                   series.dropna().values)

        return {'Obs': n, 'Missing_Rate': mis_r, **pcts, 'Mean': mean_val, 'PSI': psi_val}

    for feat in features:
        if feat not in benchmark_df.columns:
            print(f"  WARNING: {feat} not in benchmark — skipping")
            continue
        if feat not in actual_df.columns:
            print(f"  WARNING: {feat} not in actual — skipping")
            continue

        is_cat = feat in categorical_vars
        bench_series = benchmark_df[feat]

        # ── Benchmark rows (one per month if date_col present, else one overall) ──
        if date_col in benchmark_df.columns:
            for bmonth in sorted(benchmark_df[date_col].unique()):
                bsub = benchmark_df[benchmark_df[date_col] == bmonth][feat]
                s = get_stats(bsub, ref_series=None, is_cat=is_cat)
                rows.append({
                    'Bucket': bucket_name.upper(),
                    'Feature': feat,
                    'Month': bmonth,
                    'Dataset': benchmark_label,
                    **s
                })
        else:
            s = get_stats(bench_series, ref_series=None, is_cat=is_cat)
            rows.append({
                'Bucket': bucket_name.upper(),
                'Feature': feat,
                'Month': 'ALL',
                'Dataset': benchmark_label,
                **s
            })

        # ── Current (actual) rows — one per month, PSI vs full benchmark ──
        actual_df_copy = actual_df.copy()
        actual_df_copy[date_col] = actual_df_copy[date_col].astype(str)

        for month in sorted(actual_df_copy[date_col].unique()):
            msub = actual_df_copy[actual_df_copy[date_col] == month][feat]
            s = get_stats(msub, ref_series=bench_series, is_cat=is_cat)
            rows.append({
                'Bucket': bucket_name.upper(),
                'Feature': feat,
                'Month': month,
                'Dataset': 'Current',
                **s
            })

    result = pd.DataFrame(rows)

    # Column ordering
    col_order = ['Bucket', 'Feature', 'Month', 'Dataset',
                 'Obs', 'Missing_Rate'] + p_labels + ['Mean', 'PSI']
    result = result[[c for c in col_order if c in result.columns]]

    return result


# ============================================================================
# CONVENIENCE WRAPPER — plug straight into your pipeline data
# ============================================================================

def run_distribution_diagnostic(
    benchmark_parquet_path: str,
    actual_parquet_path: str,
    bucket: str,                        # 'b1', 'b2', 'b3', 'b4'
    features: list,
    preprocess_fn,
    categorical_vars: list = None,
    output_csv: str = None,
    non_prime_flag_col: str = 'NON_PRIME_FLAG',
    dq_bucket_map: dict = None
) -> pd.DataFrame:
    """
    End-to-end wrapper:
      1. Load benchmark parquet, filter ATT_MAIN=1 + OOT2
      2. Load actual parquet, filter prime, preprocess bucket slice
      3. Build distribution table
      4. Optionally save to CSV
    """
    if dq_bucket_map is None:
        dq_bucket_map = {
            'b1': 'Bucket 1', 'b2': 'Bucket 2',
            'b3': 'Bucket 3', 'b4': 'Bucket 4+'
        }

    print(f"Loading benchmark: {benchmark_parquet_path}")
    bench = pd.read_parquet(benchmark_parquet_path)
    bench.columns = bench.columns.str.upper()

    if 'SAMPLE_TYPE' in bench.columns:
        bench = bench[bench['SAMPLE_TYPE'] == 'OOT2']
        print(f"  Filtered to OOT2: {len(bench):,} rows")

    bench = bench[bench['ATT_MAIN'] == 1]
    print(f"  Filtered to ATT_MAIN=1: {len(bench):,} rows")

    # Add month to benchmark if OBSERVATION_DATE present
    if 'OBSERVATION_DATE' in bench.columns:
        bench['month'] = pd.to_datetime(bench['OBSERVATION_DATE'],
                                        errors='coerce').dt.to_period('M').astype(str)

    print(f"\nLoading actual: {actual_parquet_path}")
    actual_raw = pd.read_parquet(actual_parquet_path)
    actual_raw.columns = actual_raw.columns.str.upper()

    if non_prime_flag_col in actual_raw.columns:
        actual_raw = actual_raw[actual_raw[non_prime_flag_col] == 0]
        print(f"  After prime filter: {len(actual_raw):,} rows")

    dq_label = dq_bucket_map[bucket.lower()]
    actual_bucket = actual_raw[actual_raw['DQ_BUCKET'] == dq_label].copy()
    print(f"  {dq_label} rows: {len(actual_bucket):,}")

    print(f"  Preprocessing {bucket.upper()}...")
    actual_processed = preprocess_fn(actual_bucket)
    actual_processed['month'] = pd.to_datetime(
        actual_bucket['OBSERVATION_DATE'], errors='coerce'
    ).dt.to_period('M').astype(str).values
    print(f"  Preprocessed: {len(actual_processed):,} rows")

    print(f"\nBuilding distribution table for {len(features)} features...")
    result = build_distribution_table(
        benchmark_df=bench,
        actual_df=actual_processed,
        features=features,
        bucket_name=bucket,
        categorical_vars=categorical_vars or [],
        date_col='month'
    )

    if output_csv:
        result.to_csv(output_csv, index=False)
        print(f"\nSaved to: {output_csv}")

    return result


# ============================================================================
# B4 DISTRIBUTION DIAGNOSTIC
# ============================================================================

if __name__ == "__main__":

    from preprocess import b4_preprocess

    B4_CATEGORICAL_VARS = [
        'ATT_DQ2UP_CNT_12M',
        'ATT_DQCNT_12M',
        'ATT3_ROLLIN_B1_CNT_12M',
        'ATT_CONS_NODQ5UP_CNT',
        'ATT_NOAUTOPAY_CNT_12M',
        'ATT3_ROLLIN_B3_CNT_12M',
        'ATT3_IS_STRROLLIN_B4UP',
        'ATT3_IS_STRROLLIN_B5',
        'ATT_IS_B4_WORST_DQ_L3M'
    ]

    B4_FEATURES = [
        'ATT_TOT_DQ_DAYS_CYC_RATIO_L3M', 'ATT_DQ2UP_CNT_12M',
        'ATT_TOT_DQ_DAYS_CYC_RATIO_L1M', 'ATT_PAST_DUE_TO_SCHD',
        'ATT3_IS_STRROLLIN_B4UP', 'ATT_DQCNT_12M',
        'ATT3_ROLLIN_B1_CNT_12M', 'ATT3_IS_STRROLLIN_B5',
        'ALL2226', 'ATT_IS_B4_WORST_DQ_L3M', 'ALL4980',
        'ATT_PAST_DUE_AMT_FIXED', 'ATT3_ROLLIN_B3_CNT_12M',
        'REV2126', 'ATT_TOT_DQ_DAYS_CYC_RATIO_L6M', 'ALL8154',
        'ATT_CONS_NODQ5UP_CNT', 'ATT_REMAINING_BAL_RATIO',
        'REV5742', 'ATT_NOAUTOPAY_CNT_12M'
    ]

    result = run_distribution_diagnostic(
        benchmark_parquet_path='s3://sofi-data-science/mraj/pl_collection_b4_dr2_12jun25_processed.parquet',
        actual_parquet_path='s3://sofi-data-science/jbhagat/pl_collection_scored_data_Q4_2025_JB.parquet',
        bucket='b4',
        features=B4_FEATURES,
        preprocess_fn=b4_preprocess,
        categorical_vars=B4_CATEGORICAL_VARS,
        output_csv='b4_distribution_diagnostic.csv'
    )

    print(result.to_string(index=False))
