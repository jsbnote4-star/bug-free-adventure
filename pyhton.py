"""
PSI Feature Distribution Diagnostic v2
=======================================
Generates a detailed percentile + PSI table for any feature,
comparing benchmark (all months) vs current (monitored months).

Output format:
Bucket | Feature | Month | Dataset | Obs | Missing_Rate | P05 | P25 | P50 | P75 | P95 | Mean | PSI

FIXES APPLIED (aligned with psi_pipeline_fixed_v2.py):

FIX 1 — calculate_psi_continuous:
    Removed np.unique(breakpoints) deduplication. Discrete-heavy features
    (e.g. ATT_TOT_DQ_DAYS_CYC_RATIO_L6M) were producing variable bin counts
    month-to-month causing wildly unstable PSI. Now always exactly 10 bins.

FIX 2 — calculate_psi_categorical:
    Null filling now always applied (was missing).
    Zero-proportion guard now applied to all categories including cross-dataset
    mismatches (was silently skipping psi += 0 when one side was zero).
"""

import pandas as pd
import numpy as np
import warnings
warnings.filterwarnings('ignore')


# ============================================================================
# PSI HELPER (same logic as your pipeline)
# ============================================================================

def calculate_psi_continuous(expected_arr, actual_arr, buckets=10):
    """
    Calculate PSI between two arrays using quantile binning on expected.

    FIX: Removed np.unique(breakpoints) — duplicate bin edges for discrete-heavy
    features were causing variable effective bin counts month-to-month, producing
    wildly unstable PSI values even when distributions were identical.
    Now passes raw quantile breakpoints directly to np.histogram, always giving
    exactly `buckets` bins — matching the original Matthew Burke methodology.
    """
    expected_arr = expected_arr[~np.isnan(expected_arr)]
    actual_arr   = actual_arr[~np.isnan(actual_arr)]

    if len(expected_arr) == 0 or len(actual_arr) == 0:
        return np.nan

    # NO np.unique — keep all breakpoints to guarantee exactly `buckets` bins
    breakpoints = np.array([
        np.percentile(expected_arr, b)
        for b in np.linspace(0, 100, buckets + 1)
    ])

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


def calculate_psi_categorical(ref_series, new_series, null_value='NULL', special_values=None):
    """
    Calculate PSI for a categorical feature.

    FIX 1: Null filling now always applied (was missing entirely in old version).
    FIX 2: Zero-proportion guard (0.0001) applied to all categories — previously
            categories present in one dataset but not the other were silently
            skipped with psi += 0, underestimating drift.
    """
    ref = ref_series.copy()
    new = new_series.copy()

    # Always fill nulls
    ref = ref.fillna(null_value)
    new = new.fillna(null_value)

    # Replace special sentinel values with null placeholder
    if special_values is not None:
        ref = ref.replace(special_values, null_value)
        new = new.replace(special_values, null_value)

    ref_counts = ref.value_counts(normalize=True)
    new_counts = new.value_counts(normalize=True)
    all_cats   = set(ref.unique()) | set(new.unique())

    psi = 0.0
    for cat in all_cats:
        e = ref_counts.get(cat, 0)
        a = new_counts.get(cat, 0)
        # Apply zero guard to both sides
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

    def get_stats(series, ref_series=None, is_cat=False, special_vals=None):
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
                psi_val = calculate_psi_categorical(
                    ref_series, series, special_values=special_vals
                )
            else:
                psi_val = calculate_psi_continuous(
                    ref_series.dropna().values, series.dropna().values
                )

        return {'Obs': n, 'Missing_Rate': mis_r, **pcts, 'Mean': mean_val, 'PSI': psi_val}

    SPECIAL_VALUES_CONFIG = {
        'sloppy_ind_d3': [60.0],
    }

    for feat in features:
        if feat not in benchmark_df.columns:
            print(f"  WARNING: {feat} not in benchmark — skipping")
            continue
        if feat not in actual_df.columns:
            print(f"  WARNING: {feat} not in actual — skipping")
            continue

        is_cat       = feat in categorical_vars
        special_vals = SPECIAL_VALUES_CONFIG.get(feat, None)
        bench_series = benchmark_df[feat]

        # ── Benchmark rows (one per month if date_col present, else one overall) ──
        if date_col in benchmark_df.columns:
            for bmonth in sorted(benchmark_df[date_col].unique()):
                bsub = benchmark_df[benchmark_df[date_col] == bmonth][feat]
                s = get_stats(bsub, ref_series=None, is_cat=is_cat, special_vals=special_vals)
                rows.append({
                    'Bucket': bucket_name.upper(),
                    'Feature': feat,
                    'Month': bmonth,
                    'Dataset': benchmark_label,
                    **s
                })
        else:
            s = get_stats(bench_series, ref_series=None, is_cat=is_cat, special_vals=special_vals)
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
            s = get_stats(msub, ref_series=bench_series, is_cat=is_cat, special_vals=special_vals)
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
