"""
PSI (Population Stability Index) Calculation Pipeline for Collection Buckets
CORRECTED VERSION - Matches Industry Standard (Code 3)
Handles variable-level PSI calculations AND target PSI for B1, B2, B3, and B4 buckets

KEY FEATURES:
1. Variable-level PSI for all features
2. Target PSI (comparing benchmark 'TARGET_PRED' to actual 'SCORE')
3. Monthly PSI values for target

FIXES APPLIED:
- benchmark.columns uppercased after loading (fixes TARGET_PRED not found)
- benchmark_col changed from 'TARGET_SCORE' -> 'TARGET_PRED'
- actual_col changed from 'score' -> 'SCORE' (main df is uppercased)
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Tuple
import warnings
warnings.filterwarnings('ignore')


# ============================================================================
# CONFIGURATION - CATEGORICAL VARIABLES BY BUCKET
# ============================================================================

CATEGORICAL_VARS_CONFIG = {
    'b1': [
        'ATT_NOAUTOPAY_CNT_12M',
        'ATT_CONS_NODQ2UP_CNT',
        'ATT3_ROLLIN_B1_CNT_12M',
        'ATT_CONS_NODQCNT',
        'ATT_CONS_NODQ3UP_CNT',
        'ATT_MS_LAST_NOAUTOPAY',
        'ATT_DQ2UP_CNT_6M',
        'WORST_DQ_L3M',
        'WORST_DQ_L6M',
        'ATT_DQ2UP_CNT_3M',
        'ATT_DQCNT_3M',
        'ATT_IS_B1_WORST_DQ_L12M',
        'ATT_IS_AUTOPAY',
        'ATT_IS_B1_WORST_DQ_L3M',
        'ATT_IS_REVERSED_RENTRY',
        'ATT_IS_DUE_CHG',
        'ATT_IS_AUTOPAY_L6M',
        'ATT_IS_DQ2UP_L1M',
        'ATT_MAIN'
    ],
    'b2': [
        'ATT_CONS_NODQ2UP_CNT',
        'ATT_DQ2UP_CNT_12M',
        'ATT_CONS_NODQ4UP_CNT',
        'ATT3_ROLLIN_B2_CNT_12M',
        'ATT_REVERSED_CNT_12M',
        'ATT_DQCNT_6M',
        'ATT3_DOWN_TOB0_CNT_12M',
        'ATT_NOAUTOPAY_CNT_3M',
        'ATT_DQ4UP_CNT_3M',
        'ATT_REVERSED_CNT_3M',
        'ATT_IS_DQ2UP_L1M',
        'ATT_IS_B2_WORST_DQ_L12M'
    ],
    'b3': [
        'ATT_DQ2UP_CNT_12M',
        'ATT_CONS_NODQ3UP_CNT',
        'ATT3_ROLLIN_B2_CNT_12M',
        'ATT_CONS_NODQ5UP_CNT',
        'ATT_REVERSED_CNT_12M',
        'ATT_DQCNT_6M',
        'ATT3_DOWN_TOB0_CNT_12M',
        'ATT_NOAUTOPAY_CNT_3M',
        'ATT_IS_B3_L1M'
    ],
    'b4': [
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
}

SPECIAL_VALUES_CONFIG = {
    'sloppy_ind_d3': [60.0],
}


# ============================================================================
# BUCKET ATTRIBUTE LISTS
# ============================================================================

b1_attr_list = [
    'ATT_IS_B1_WORST_DQ_L12M', 'ATT2_VANTAGE_CHANGE', 'ATT_NOAUTOPAY_CNT_12M', 
    'ATT_IS_AUTOPAY', 'ATT_IS_B1_WORST_DQ_L3M', 'ATT2_FICO_CHANGE', 
    'ATT_TOT_DQ_DAYS_CYC_RATIO_L1M', 'ATT_TOT_DQ_DAYS_CYC_RATIO_L3M', 
    'ATT_DQ2UP_CNT_3M', 'ATT_PAST_DUE_TO_SCHD', 'ALL5012', 'ATT_CONS_NODQ2UP_CNT', 
    'ALL8552', 'ATT_IS_REVERSED_RENTRY', 'ATT3_SOFIDTI_REFRESH', 
    'ATT3_ROLLIN_B1_CNT_12M', 'REH7120', 'ATT_ORIG_GROSS_INT_RATE', 'BCX7110', 
    'WORST_DQ_L3M', 'ATT_MS_LAST_NOAUTOPAY', 'PIL0438', 'ATT_GROSS_INCOME', 
    'ATT_DQ2UP_CNT_6M', 'ATT_IS_DUE_CHG', 'ATT_CONS_NODQCNT', 'ATT_CONS_NODQ3UP_CNT', 
    'ATT_IS_AUTOPAY_L6M', 'ATT_DQCNT_3M', 'BCA8370', 'BRC5620', 'BCC7482', 
    'ALL7518', 'ATT_SEASONING', 'ALL2421', 'ATT_IS_DQ2UP_L1M', 'ALL7517', 
    'ATT_MAIN', 'WORST_DQ_L6M', 'IQF9540', 'BCC7483', 'IQM9540', 'ALL5320', 
    'FIP0437', 'ALL7519', 'FIP8320', 'BCC3421', 'ATT2_ORIG_VANTAGE', 
    'ATT2_ORIG_FICO', 'ATT_UBTI'
]

b2_attr_list = [
    'ATT_DQCNT_6M', 'ATT_PAST_DUE_TO_SCHD', 'ATT_TOT_DQ_DAYS_CYC_RATIO_L1M', 
    'ATT_IS_DQ2UP_L1M', 'ATT3_DOWN_TOB0_CNT_12M', 'ATT3_SOFIDTI_REFRESH', 
    'ATT_CONS_NODQ2UP_CNT', 'ATT_DQ2UP_CNT_12M', 'ATT_NOAUTOPAY_CNT_3M', 
    'ATT_CONS_NODQ4UP_CNT', 'ALL8370', 'ATT_DQ4UP_CNT_3M', 'ATT_IS_B2_WORST_DQ_L12M', 
    'BCA8370', 'ALL8272', 'ATT3_ROLLIN_B2_CNT_12M', 'ALL2012', 'ATT_REVERSED_CNT_12M', 
    'ATT_REVERSED_CNT_3M', 'ATT_PAST_DUE_TO_INCOME', 'ATT_UBTI', 'ALL0438', 
    'ALS0337', 'ALL8250'
]

b3_attr_list = [
    'ATT_DQ2UP_CNT_12M', 'ATT_PAST_DUE_TO_SCHD', 'ATT_DQCNT_6M', 
    'ATT_CONS_NODQ3UP_CNT', 'ATT_IS_B3_L1M', 'ATT_TOT_DQ_DAYS_CYC_RATIO_L3M', 
    'ATT3_ROLLIN_B2_CNT_12M', 'ALL8171', 'ALL2012', 'ALL8370', 'ATT_REVERSED_CNT_12M', 
    'ATT_NOAUTOPAY_CNT_3M', 'ALL8154', 'ATT_TOT_DQ_DAYS_CYC_RATIO_L1M', 'BCA8370', 
    'ATT_PAST_DUE_TO_INCOME', 'ALL8271', 'ATT3_DOWN_TOB0_CNT_12M', 
    'ATT_CONS_NODQ5UP_CNT', 'BCA8151', 'ATT_LAST_PAYMENT_AMOUNT_FIXED', 'REV0318', 
    'BCC7708', 'ATT_UBTI', 'ATT3_SOFIDTI_REFRESH', 'ALL8250', 'ATT_REL_REPAY_TERM_MON', 
    'ILN4080', 'BCC8338', 'STU5031', 'AUA8370'
]

b4_attr_list = [
    'ATT_TOT_DQ_DAYS_CYC_RATIO_L3M', 'ATT_DQ2UP_CNT_12M', 'ATT_TOT_DQ_DAYS_CYC_RATIO_L1M', 
    'ATT_PAST_DUE_TO_SCHD', 'ATT3_IS_STRROLLIN_B4UP', 'ATT_DQCNT_12M', 
    'ATT3_ROLLIN_B1_CNT_12M', 'ATT3_IS_STRROLLIN_B5', 'ALL2226', 
    'ATT_IS_B4_WORST_DQ_L3M', 'ALL4980', 'ATT_PAST_DUE_AMT_FIXED', 
    'ATT3_ROLLIN_B3_CNT_12M', 'REV2126', 'ATT_TOT_DQ_DAYS_CYC_RATIO_L6M', 
    'ALL8154', 'ATT_CONS_NODQ5UP_CNT', 'ATT_REMAINING_BAL_RATIO', 'REV5742', 
    'ATT_NOAUTOPAY_CNT_12M'
]

BUCKET_ATTR_MAPPING = {
    'b1': b1_attr_list,
    'b2': b2_attr_list,
    'b3': b3_attr_list,
    'b4': b4_attr_list
}


# ============================================================================
# UTILITY FUNCTIONS
# ============================================================================

def extract_month_from_data(df, date_column_candidates=None):
    if date_column_candidates is None:
        date_column_candidates = ['observation_date', 'OBSERVATION_DATE', 
                                 'obs_date', 'OBS_DATE', 'date', 'DATE']
    
    for col in date_column_candidates:
        if col in df.columns:
            try:
                return pd.to_datetime(df[col]).dt.to_period('M').astype(str)
            except Exception as e:
                print(f"Warning: Could not convert {col} to month: {str(e)}")
                continue
    
    raise ValueError(f"No valid date column found. Tried: {date_column_candidates}")


# ============================================================================
# PSI CALCULATION FUNCTIONS
# ============================================================================

def calculate_psi_continuous(expected, actual, score_dev, score_act, buckettype='quantiles', buckets=10, round_decimals=4):
    expected_data = expected[~expected[score_dev].isna()][score_dev].copy()
    actual_data = actual[~actual[score_act].isna()][score_act].copy()
    
    if len(expected_data) == 0 or len(actual_data) == 0:
        return np.nan
    
    if round_decimals is not None:
        expected_data = expected_data.round(round_decimals)
        actual_data = actual_data.round(round_decimals)
    
    def psi_with_details(expected_array, actual_array, buckets):
        def scale_range(input_arr, min_val, max_val):
            input_arr = input_arr.copy()
            input_arr += -(np.min(input_arr))
            if np.max(input_arr) > 0:
                input_arr /= np.max(input_arr) / (max_val - min_val)
            input_arr += min_val
            return input_arr
        
        breakpoints = np.arange(0, buckets + 1) / buckets * 100
        
        if buckettype == 'bins':
            breakpoints = scale_range(breakpoints, np.min(expected_array), np.max(expected_array))
        elif buckettype == 'quantiles':
            breakpoints = np.stack([np.percentile(expected_array, b) for b in breakpoints])
        
        breakpoints_unique = np.unique(breakpoints)
        n_bins = len(breakpoints_unique) - 1
        
        expected_percents = np.histogram(expected_array, breakpoints_unique)[0] / len(expected_array)
        actual_percents = np.histogram(actual_array, breakpoints_unique)[0] / len(actual_array)
        
        contribution_data = []
        
        for i in range(n_bins):
            e_perc = expected_percents[i] if expected_percents[i] > 0 else 0.0001
            a_perc = actual_percents[i] if actual_percents[i] > 0 else 0.0001
            
            bin_width = breakpoints_unique[i+1] - breakpoints_unique[i]
            if bin_width < 0.001:
                continue
            
            diff = a_perc - e_perc
            ln_ratio = np.log(a_perc / e_perc)
            psi_contrib = diff * ln_ratio
            
            contribution_data.append({
                'Bin': f'Bin{len(contribution_data)+1}',
                'Range': f"[{breakpoints_unique[i]:.4f}, {breakpoints_unique[i+1]:.4f})",
                'Benchmark_%': e_perc * 100,
                'Current_%': a_perc * 100,
                'A_B': diff * 100,
                'ln_A_B': ln_ratio,
                'PSI': psi_contrib
            })
        
        psi_value = sum([item['PSI'] for item in contribution_data])
        return psi_value, pd.DataFrame(contribution_data)
    
    psi_val, _ = psi_with_details(expected_data, actual_data, buckets)
    return psi_val


def calculate_psi_categorical(data_ref, data_new, feature, null_value='NULL', special_values=None):
    ref = data_ref.copy()
    new = data_new.copy()
    
    ref[feature] = ref[feature].fillna(null_value)
    new[feature] = new[feature].fillna(null_value)
    
    ref_counts = ref[feature].value_counts(normalize=True)
    new_counts = new[feature].value_counts(normalize=True)
    
    all_categories = sorted(set(ref[feature].unique()) | set(new[feature].unique()))
    
    contribution_data = []
    
    for category in all_categories:
        ref_prop = ref_counts.get(category, 0)
        new_prop = new_counts.get(category, 0)
        
        ref_prop_adj = ref_prop if ref_prop > 0 else 0.0001
        new_prop_adj = new_prop if new_prop > 0 else 0.0001
        
        diff = new_prop - ref_prop
        ln_ratio = np.log(new_prop_adj / ref_prop_adj)
        psi_contrib = diff * ln_ratio
        
        contribution_data.append({
            'Category': str(category),
            'Benchmark_%': ref_prop * 100,
            'Current_%': new_prop * 100,
            'A_B': diff * 100,
            'ln_A_B': ln_ratio,
            'PSI': psi_contrib
        })
    
    psi_value = sum([item['PSI'] for item in contribution_data])
    return psi_value


def calculate_var_psi_bucket(dev, act, features, date_col, yyyymm, 
                             bucket_name, categorical_vars=None):
    if categorical_vars is None:
        categorical_vars = []
    
    mon = act[act[date_col] == yyyymm].copy()
    
    if len(mon) == 0:
        print(f"Warning: No data found for month {yyyymm} in {bucket_name}")
        return pd.DataFrame({'var': features, 'psi': [np.nan] * len(features)})
    
    results = []
    
    for var in features:
        try:
            if var not in dev.columns or var not in mon.columns:
                print(f"Warning: Variable {var} not found in datasets for {bucket_name}")
                results.append({'var': var, 'psi': np.nan})
                continue
            
            is_categorical = var in categorical_vars
            special_vals = SPECIAL_VALUES_CONFIG.get(var, None)
            
            if is_categorical:
                psi_val = calculate_psi_categorical(dev, mon, var, special_values=special_vals)
            else:
                psi_val = calculate_psi_continuous(dev, mon, var, var)
            
            results.append({'var': var, 'psi': psi_val})
            
        except Exception as e:
            print(f"Error calculating PSI for {var} in {bucket_name} for month {yyyymm}: {str(e)}")
            results.append({'var': var, 'psi': np.nan})
    
    return pd.DataFrame(results)


def calculate_psi_all_months(dev, act, features, date_col, bucket_name, categorical_vars=None):
    act = act.copy()
    act[date_col] = act[date_col].astype(str)
    
    all_months = sorted(act[date_col].unique())
    
    print(f"\n{bucket_name.upper()} - Processing {len(all_months)} months: {all_months[0]} to {all_months[-1]}")
    
    final_psi_df = calculate_var_psi_bucket(
        dev, act, features, date_col, all_months[0], bucket_name, categorical_vars
    ).rename(columns={'psi': all_months[0]})
    
    for month in all_months[1:]:
        psi_result = calculate_var_psi_bucket(
            dev, act, features, date_col, month, bucket_name, categorical_vars
        ).rename(columns={'psi': month})
        
        final_psi_df = final_psi_df.merge(psi_result, on='var', how='outer')
    
    final_psi_df = final_psi_df.rename(columns={'var': 'variable'})
    
    print(f"{bucket_name.upper()} - Completed PSI calculation for {len(features)} variables")
    
    return final_psi_df


# ============================================================================
# TARGET PSI CALCULATION FUNCTIONS
# ============================================================================

def calculate_target_psi_monthly(benchmark_df, actual_df, date_col, bucket_name,
                                 benchmark_col='TARGET_PRED',   # FIX: was 'TARGET_SCORE'
                                 actual_col='SCORE'):
    
    print(f"\n{bucket_name.upper()} - Calculating Target PSI...")
    
    if benchmark_col not in benchmark_df.columns:
        print(f"Warning: '{benchmark_col}' not found in benchmark data for {bucket_name}")
        print(f"  Available columns: {[c for c in benchmark_df.columns if 'score' in c.lower() or 'pred' in c.lower()]}")
        return pd.DataFrame({'month': [], 'target_psi': []})
    
    if actual_col not in actual_df.columns:
        print(f"Warning: '{actual_col}' not found in actual data for {bucket_name}")
        return pd.DataFrame({'month': [], 'target_psi': []})
    
    actual_df = actual_df.copy()
    actual_df[date_col] = actual_df[date_col].astype(str)
    
    all_months = sorted(actual_df[date_col].unique())
    
    print(f"  Processing {len(all_months)} months: {all_months[0]} to {all_months[-1]}")
    
    results = []
    
    for month in all_months:
        month_data = actual_df[actual_df[date_col] == month].copy()
        
        if len(month_data) == 0:
            print(f"  Warning: No data for month {month}")
            results.append({'month': month, 'target_psi': np.nan})
            continue
        
        try:
            psi_val = calculate_psi_continuous(
                expected=benchmark_df,
                actual=month_data,
                score_dev=benchmark_col,
                score_act=actual_col,
                buckettype='quantiles',
                buckets=10,
                round_decimals=4
            )
            
            results.append({'month': month, 'target_psi': psi_val})
            print(f"  {month}: PSI = {psi_val:.4f}" if not np.isnan(psi_val) else f"  {month}: PSI = N/A")
            
        except Exception as e:
            print(f"  Error calculating target PSI for month {month}: {str(e)}")
            results.append({'month': month, 'target_psi': np.nan})
    
    result_df = pd.DataFrame(results)
    print(f"{bucket_name.upper()} - Completed Target PSI calculation")
    
    return result_df


# ============================================================================
# MAIN PIPELINE FUNCTION
# ============================================================================

def run_psi_pipeline(data_path: str, benchmark_paths: Dict[str, str],
                    output_file: Optional[str] = None,
                    preprocess_funcs: Optional[Dict] = None,
                    calculate_target_psi: bool = True):
    
    print("="*80)
    print("PSI CALCULATION PIPELINE - WITH TARGET PSI")
    print("="*80)
    
    # Load main data
    print("\nLoading main data...")
    df = pd.read_parquet(data_path)
    print(f"Loaded {len(df):,} rows")

    # Uppercase all columns so everything is consistent
    df.columns = df.columns.str.upper()

    # Date filter
    df['OBSERVATION_DATE'] = pd.to_datetime(df['OBSERVATION_DATE'], errors='coerce')
    df = df[df['OBSERVATION_DATE'] >= pd.to_datetime('2024-01-01')]
    
    # Score filter
    print("Applying SCORE >= 0 filter...")
    df = df[df['SCORE'] >= 0]
    print(f"After score filter: {len(df):,} rows")

    # Non-prime filter (exclude Pagaya TIER=20)
    if 'NON_PRIME_FLAG' in df.columns:
        df = df[df['NON_PRIME_FLAG'] == 0]
        print(f"After non-prime filter: {len(df):,} rows")
    
    # Split by bucket
    print("\nSplitting data by bucket...")
    buckets_data = {
        'b1': df[df['DQ_BUCKET'] == 'Bucket 1'],
        'b2': df[df['DQ_BUCKET'] == 'Bucket 2'],
        'b3': df[df['DQ_BUCKET'] == 'Bucket 3'],
        'b4': df[df['DQ_BUCKET'] == 'Bucket 4+']
    }
    
    for bucket, data in buckets_data.items():
        print(f"  {bucket.upper()}: {len(data):,} rows")
    
    # Preprocess
    processed_data = {}
    if preprocess_funcs is not None:
        print("\nPreprocessing bucket data...")
        for bucket, data in buckets_data.items():
            print(f"  Processing {bucket.upper()}...")
            # Save SCORE + month BEFORE preprocess — preprocess only keeps model feature
            # columns and drops everything else (SCORE, OBSERVATION_DATE, etc.)
            month_series = pd.to_datetime(data['OBSERVATION_DATE'], errors='coerce').dt.to_period('M').astype(str)
            score_series  = data['SCORE'].values
            processed_data[bucket] = preprocess_funcs[bucket](data)
            processed_data[bucket]['month'] = month_series.values
            processed_data[bucket]['SCORE'] = score_series
    else:
        print("\nNo preprocessing functions provided - assuming data is preprocessed...")
        for bucket, data in buckets_data.items():
            processed_data[bucket] = data.copy()
            if 'month' not in processed_data[bucket].columns:
                processed_data[bucket]['month'] = extract_month_from_data(processed_data[bucket])
    
    # Calculate PSI for each bucket
    print("\n" + "="*80)
    print("CALCULATING PSI BY BUCKET")
    print("="*80)
    
    all_results = {}
    
    for bucket in ['b1', 'b2', 'b3', 'b4']:
        print(f"\n{'-'*80}")
        print(f"Processing {bucket.upper()}")
        print(f"{'-'*80}")
        
        if bucket not in benchmark_paths:
            print(f"Warning: No benchmark path provided for {bucket.upper()}, skipping...")
            continue
        
        print(f"Loading benchmark data from {benchmark_paths[bucket]}...")
        benchmark = pd.read_parquet(benchmark_paths[bucket])

        # FIX 1: Uppercase benchmark columns to match main data
        benchmark.columns = benchmark.columns.str.upper()

        # Filter to OOT2 only (B2/B3/B4 have mixed sample types; B1 is already all OOT2)
        if 'SAMPLE_TYPE' in benchmark.columns:
            benchmark = benchmark[benchmark['SAMPLE_TYPE'] == 'OOT2']
            print(f"Filtered to OOT2: {len(benchmark):,} rows")
        
        benchmark_processed = benchmark.copy()

        print(f"Before bechmark Main filter: {len(benchmark_processed):,} rows")
        benchmark_processed = benchmark_processed[benchmark_processed['ATT_MAIN']==1]
        print(f"After bechmark Main filter: {len(benchmark_processed):,} rows")
        
        # --- 1. Variable PSI ---
        features = BUCKET_ATTR_MAPPING[bucket]
        categorical_vars = CATEGORICAL_VARS_CONFIG.get(bucket, [])
        
        print(f"\n1. VARIABLE PSI")
        print(f"   Features to analyze: {len(features)}")
        print(f"   Categorical variables: {len(categorical_vars)}")
        
        variable_psi_df = calculate_psi_all_months(
            dev=benchmark_processed,
            act=processed_data[bucket],
            features=features,
            date_col='month',
            bucket_name=bucket,
            categorical_vars=categorical_vars
        )
        
        # --- 2. Target PSI ---
        target_psi_df = None
        if calculate_target_psi:
            print(f"\n2. TARGET PSI")
            target_psi_df = calculate_target_psi_monthly(
                benchmark_df=benchmark_processed,
                actual_df=processed_data[bucket],
                date_col='month',
                bucket_name=bucket,
                benchmark_col='TARGET_PRED',  # FIX 2: was 'TARGET_SCORE'
                actual_col='SCORE'             # FIX 3: was 'score' (df is now uppercased)
            )
        
        all_results[bucket] = {
            'variable_psi': variable_psi_df,
            'target_psi': target_psi_df
        }
    
    # Save to Excel
    if output_file:
        print("\n" + "="*80)
        print("SAVING RESULTS")
        print("="*80)
        
        with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
            for bucket, results in all_results.items():
                # Variable PSI sheet
                var_sheet_name = f"{bucket.upper()}_Variable_PSI"
                results['variable_psi'].to_excel(writer, sheet_name=var_sheet_name, index=False)
                print(f"Saved {bucket.upper()} Variable PSI -> {var_sheet_name}")
                
                # Target PSI sheet
                if results['target_psi'] is not None and not results['target_psi'].empty:
                    target_sheet_name = f"{bucket.upper()}_Target_PSI"
                    results['target_psi'].to_excel(writer, sheet_name=target_sheet_name, index=False)
                    print(f"Saved {bucket.upper()} Target PSI -> {target_sheet_name}")
                else:
                    print(f"WARNING: {bucket.upper()} Target PSI is empty - check column names above")
        
        print(f"\nAll results saved to: {output_file}")
    
    print("\n" + "="*80)
    print("PSI PIPELINE COMPLETED")
    print("="*80)
    
    return all_results


# ============================================================================
# EXAMPLE USAGE
# ============================================================================

if __name__ == "__main__":

    # from preprocess import b1_preprocess, b2_preprocess, b3_preprocess, b4_preprocess

    DATA_PATH = 's3://sofi-data-science/jbhagat/pl_collection_scored_data_Q4_2025_JB.parquet'
    
    BENCHMARK_PATHS = {
        'b1': 's3://sofi-data-science/mraj/pl_collection_b1_dr2_12jun25_processed.parquet',
        'b2': 's3://sofi-data-science/mraj/pl_collection_b2_dr2_12jun25_processed.parquet',
        'b3': 's3://sofi-data-science/mraj/pl_collection_b3_dr2_12jun25_processed.parquet',
        'b4': 's3://sofi-data-science/mraj/pl_collection_b4_dr2_12jun25_processed.parquet'
    }
    
    PREPROCESS_FUNCS = {
        'b1': b1_preprocess,
        'b2': b2_preprocess,
        'b3': b3_preprocess,
        'b4': b4_preprocess
    }
    
    OUTPUT_FILE = f'PSI_Results_with_Target_{pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")}.xlsx'
    
    results = run_psi_pipeline(
        data_path=DATA_PATH,
        benchmark_paths=BENCHMARK_PATHS,
        output_file=OUTPUT_FILE,
        preprocess_funcs=PREPROCESS_FUNCS,
        calculate_target_psi=True
    )
    
    print("\nPSI calculation complete!")
    
    # Verify target PSI worked
    for bucket in ['b1', 'b2', 'b3', 'b4']:
        if bucket in results and results[bucket]['target_psi'] is not None:
            tpsi = results[bucket]['target_psi']
            if not tpsi.empty:
                print(f"\n{bucket.upper()} Target PSI:")
                print(tpsi.to_string(index=False))
            else:
                print(f"\n{bucket.upper()} Target PSI: EMPTY - check column name warnings above")



  # DEBUG 

# df_bench = pd.read_parquet('s3://sofi-data-science/mraj/pl_collection_b4_dr2_12jun25_processed.parquet')
# benchmark_b4 = df_bench[df_bench['ATT_MAIN']==1]

# df = pd.read_parquet('s3://sofi-data-science/jbhagat/pl_collection_scored_data_Q4_2025_JB.parquet')

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns

def compare_b4_distributions(df_bench, df_actual, features):
    """
    Generate side-by-side distribution plots and forensic stats 
    for the breaching B4 variables.
    """
    for var in features:
        print(f"\n{'='*60}")
        print(f"DISTRIBUTION ANALYSIS: {var}")
        print(f"{'='*60}")
        
        # 1. Preparation
        bench_var = df_bench[var].dropna()
        actual_var = df_actual[var].dropna()
        
        # 2. Plotting
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 6))
        
        # Histogram Comparison
        sns.histplot(bench_var, color="blue", label="Benchmark (Expected)", 
                     kde=True, stat="probability", bins=50, ax=ax1, alpha=0.4)
        sns.histplot(actual_var, color="red", label="Sep 2025 (Current)", 
                     kde=True, stat="probability", bins=50, ax=ax1, alpha=0.4)
        ax1.set_title(f"Density Comparison: {var}")
        ax1.legend()
        
        # Boxplot Comparison (to see outliers and mean shifts)
        combined_data = pd.DataFrame({
            'Value': np.concatenate([bench_var, actual_var]),
            'Sample': ['Benchmark']*len(bench_var) + ['Sep 2025']*len(actual_var)
        })
        sns.boxplot(x='Sample', y='Value', data=combined_data, ax=ax2, palette=['blue', 'red'])
        ax2.set_title(f"Range & Outlier Shift: {var}")
        
        plt.tight_layout()
        plt.show()
        
        # 3. Quantile Comparison Table
        quantiles = [0, 0.1, 0.25, 0.5, 0.75, 0.9, 1]
        q_table = pd.DataFrame({
            'Quantile': quantiles,
            'Benchmark': [bench_var.quantile(q) for q in quantiles],
            'Sep 2025': [actual_var.quantile(q) for q in quantiles]
        })
        q_table['Difference'] = q_table['Sep 2025'] - q_table['Benchmark']
        
        print("\nQuantile Comparison:")
        print(q_table.to_string(index=False))
        
        # 4. Mode and Special Value Check
        print(f"\nMost Frequent Values (Modes):")
        print(f"Benchmark Mode: {bench_var.mode().iloc[0] if not bench_var.empty else 'N/A'}")
        print(f"Sep 2025 Mode:  {actual_var.mode().iloc[0] if not actual_var.empty else 'N/A'}")

# --- EXECUTION ---
# Filter your data for B4 only
# Create a YYYY-MM string
df['month'] = pd.to_datetime(df['OBSERVATION_DATE']).dt.to_period('M').astype(str)
df_b4_sep = df[(df['DQ_BUCKET'] == 'Bucket 4+') & (df['month'] == '2025-09')]
# benchmark_b4 should already be loaded from your benchmark paths

breaching_features = ['ALL8154', 'ATT_TOT_DQ_DAYS_CYC_RATIO_L6M']
compare_b4_distributions(benchmark_b4, df_b4_sep, breaching_features)
