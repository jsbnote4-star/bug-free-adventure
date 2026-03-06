ALL8154
  Benchmark  — min: 0.0000, max: 23.0000, mean: 14.6189, nulls: 0
  Scored raw — min: 0.0000, max: 9999.0000, mean: 5110.6965, nulls: 96

ATT_TOT_DQ_DAYS_CYC_RATIO_L6M
  Benchmark  — min: 0.0617, max: 1.0350, mean: 0.6818, nulls: 0
  Scored raw — min: 0.0000, max: 1.0350, mean: 0.6829, nulls: 44


preprocess code :

import pandas as pd
import numpy as np

b1_attr_list = ['ATT_IS_B1_WORST_DQ_L12M', 'ATT2_VANTAGE_CHANGE', 'ATT_NOAUTOPAY_CNT_12M', 'ATT_IS_AUTOPAY', 'ATT_IS_B1_WORST_DQ_L3M', 'ATT2_FICO_CHANGE', 'ATT_TOT_DQ_DAYS_CYC_RATIO_L1M', 'ATT_TOT_DQ_DAYS_CYC_RATIO_L3M', 'ATT_DQ2UP_CNT_3M', 'ATT_PAST_DUE_TO_SCHD', 'ALL5012', 'ATT_CONS_NODQ2UP_CNT', 'ALL8552', 'ATT_IS_REVERSED_RENTRY', 'ATT3_SOFIDTI_REFRESH', 'ATT3_ROLLIN_B1_CNT_12M', 'REH7120', 'ATT_ORIG_GROSS_INT_RATE', 'BCX7110', 'WORST_DQ_L3M', 'ATT_MS_LAST_NOAUTOPAY', 'PIL0438', 'ATT_GROSS_INCOME', 'ATT_DQ2UP_CNT_6M', 'ATT_IS_DUE_CHG', 'ATT_CONS_NODQCNT', 'ATT_CONS_NODQ3UP_CNT', 'ATT_IS_AUTOPAY_L6M', 'ATT_DQCNT_3M', 'BCA8370', 'BRC5620', 'BCC7482', 'ALL7518', 'ATT_SEASONING', 'ALL2421', 'ATT_IS_DQ2UP_L1M', 'ALL7517', 'ATT_MAIN', 'WORST_DQ_L6M', 'IQF9540', 'BCC7483', 'IQM9540', 'ALL5320', 'FIP0437', 'ALL7519', 'FIP8320', 'BCC3421', 'ATT2_ORIG_VANTAGE', 'ATT2_ORIG_FICO', 'ATT_UBTI']

b2_attr_list = ['ATT_DQCNT_6M', 'ATT_PAST_DUE_TO_SCHD', 'ATT_TOT_DQ_DAYS_CYC_RATIO_L1M', 'ATT_IS_DQ2UP_L1M', 'ATT3_DOWN_TOB0_CNT_12M', 'ATT3_SOFIDTI_REFRESH', 'ATT_CONS_NODQ2UP_CNT', 'ATT_DQ2UP_CNT_12M', 'ATT_NOAUTOPAY_CNT_3M', 'ATT_CONS_NODQ4UP_CNT', 'ALL8370', 'ATT_DQ4UP_CNT_3M', 'ATT_IS_B2_WORST_DQ_L12M', 'BCA8370', 'ALL8272', 'ATT3_ROLLIN_B2_CNT_12M', 'ALL2012', 'ATT_REVERSED_CNT_12M', 'ATT_REVERSED_CNT_3M', 'ATT_PAST_DUE_TO_INCOME', 'ATT_UBTI', 'ALL0438', 'ALS0337', 'ALL8250']

b3_attr_list = ['ATT_DQ2UP_CNT_12M', 'ATT_PAST_DUE_TO_SCHD', 'ATT_DQCNT_6M', 'ATT_CONS_NODQ3UP_CNT', 'ATT_IS_B3_L1M', 'ATT_TOT_DQ_DAYS_CYC_RATIO_L3M', 'ATT3_ROLLIN_B2_CNT_12M', 'ALL8171', 'ALL2012', 'ALL8370', 'ATT_REVERSED_CNT_12M', 'ATT_NOAUTOPAY_CNT_3M', 'ALL8154', 'ATT_TOT_DQ_DAYS_CYC_RATIO_L1M', 'BCA8370', 'ATT_PAST_DUE_TO_INCOME', 'ALL8271', 'ATT3_DOWN_TOB0_CNT_12M', 'ATT_CONS_NODQ5UP_CNT', 'BCA8151', 'ATT_LAST_PAYMENT_AMOUNT_FIXED', 'REV0318', 'BCC7708', 'ATT_UBTI', 'ATT3_SOFIDTI_REFRESH', 'ALL8250', 'ATT_REL_REPAY_TERM_MON', 'ILN4080', 'BCC8338', 'STU5031', 'AUA8370']

b4_attr_list = ['ATT_TOT_DQ_DAYS_CYC_RATIO_L3M', 'ATT_DQ2UP_CNT_12M', 'ATT_TOT_DQ_DAYS_CYC_RATIO_L1M', 'ATT_PAST_DUE_TO_SCHD', 'ATT3_IS_STRROLLIN_B4UP', 'ATT_DQCNT_12M', 'ATT3_ROLLIN_B1_CNT_12M', 'ATT3_IS_STRROLLIN_B5', 'ALL2226', 'ATT_IS_B4_WORST_DQ_L3M', 'ALL4980', 'ATT_PAST_DUE_AMT_FIXED', 'ATT3_ROLLIN_B3_CNT_12M', 'REV2126', 'ATT_TOT_DQ_DAYS_CYC_RATIO_L6M', 'ALL8154', 'ATT_CONS_NODQ5UP_CNT', 'ATT_REMAINING_BAL_RATIO', 'REV5742', 'ATT_NOAUTOPAY_CNT_12M']

exclusion_list = ['IS_FRAUD_1', 'IS_DECEASED', 'LOW_PRINCIPAL_FLAG', 'MOB_LT3_FLAG', 'MISSING_EXPERIAN_ATTR', 'MISSING_INTERNAL_ATTR']

def b1_preprocess(data):
    
    imputer = {'ATT_IS_B1_WORST_DQ_L12M': 0,
             # 'ATT2_VANTAGE_CHANGE': -95.5,
             'ATT_NOAUTOPAY_CNT_12M': 0,
             'ATT_IS_AUTOPAY': 0,
             'ATT_IS_B1_WORST_DQ_L3M': 0,
             # 'ATT2_FICO_CHANGE': -80.5,
             'ATT_TOT_DQ_DAYS_CYC_RATIO_L1M': 0,
             'ATT_TOT_DQ_DAYS_CYC_RATIO_L3M': 0,
             'ATT_DQ2UP_CNT_3M': 0,
             'ATT_PAST_DUE_TO_SCHD': 0,
             'ALL5012': 129.5,
             'ATT_CONS_NODQ2UP_CNT': 0,
             'ALL8552': 8.5,
             'ATT_IS_REVERSED_RENTRY': 0,
             # 'ATT3_SOFIDTI_REFRESH': 0,
             'ATT3_ROLLIN_B1_CNT_12M': 0,
             'REH7120': 103.5,
             'ATT_ORIG_GROSS_INT_RATE': 0,
             'BCX7110': 79.0,
             'WORST_DQ_L3M': 0,
             'ATT_MS_LAST_NOAUTOPAY': 0,
             'PIL0438': 1.5,
             'ATT_GROSS_INCOME': 0,
             'ATT_DQ2UP_CNT_6M': 0,
             'ATT_IS_DUE_CHG': 0,
             'ATT_CONS_NODQCNT': 0,
             'ATT_CONS_NODQ3UP_CNT': 0,
             'ATT_IS_AUTOPAY_L6M': 0,
             'ATT_DQCNT_3M': 0,
             'BCA8370': 56.0,
             'BRC5620': 605.5,
             'BCC7482': 79.0,
             'ALL7518': 50.5,
             'ATT_SEASONING': 0,
             'ALL2421': 1.5,
             'ATT_IS_DQ2UP_L1M': 0,
             'ALL7517': 34.0,
             'ATT_MAIN': 0,
             'WORST_DQ_L6M': 0,
             'IQF9540': 767.0,
             'BCC7483': 31.0,
             'IQM9540': 243.0,
             'ALL5320': 81257.5,
             'FIP0437': 1.5,
             'ALL7519': 68.0,
             'FIP8320': 24.0,
             'BCC3421': 3.5,
             'ATT2_ORIG_VANTAGE': 466.0,
             'ATT2_ORIG_FICO': 0,
             'ATT_UBTI': 0,
              'ALL5820': 0,
              'MTF5820': 0,
              'ALX5839': 0,
              'MTX5839': 0,
             'ATT_COBORROWER_IND': 0,
             'ATT_GROSS_INCOME': 0,
              'VANTAGE_V3_SCORE': 0,
              'ATT2_ORIG_VANTAGE': 0,
              'FICOCLV8_SCORE': 0,
              'ATT2_ORIG_FICO': 0}

    val_max = {'ALL2421': 90.0,
              'ALL5012': 999999990.0,
              'ALL5320': 999999990.0,
              'ALL7517': 100.0,
              'ALL7518': 100.0,
              'ALL7519': 100.0,
              'ALL8552': 9990.0,
              'BCA8370': 9990.0,
              'BCC3421': 90.0,
              'BCC7482': 100.0,
              'BCC7483': 100.0,
              'BCX7110': 990.0,
              'BRC5620': 999999990.0,
              'FIP0437': 90.0,
              'FIP8320': 9990.0,
              'IQF9540': 9990.0,
              'IQM9540': 9990.0,
              'PIL0438': 90.0,
              'REH7120': 990.0,
              'ALL5820': 999999996,
              'MTF5820': 999999996,
              'ALX5839': 999999996,
              'MTX5839': 999999996,
              'VANTAGE_V3_SCORE': 850,
              'ATT2_ORIG_VANTAGE': 850,
              'FICOCLV8_SCORE': 850,
              'ATT2_ORIG_FICO': 850}
    
    vars_months = ['ATT_CONS_NODQCNT'
                ,'ATT_CONS_NODQ2UP_CNT'
                ,'BCA8370'
                ,'ATT_CONS_NODQ3UP_CNT']


    req_attr = list(imputer.keys())
    internal_attr = [attr for attr in req_attr if attr.startswith('ATT') and (attr not in ['ATT2_ORIG_VANTAGE', 'ATT2_ORIG_FICO'])]
    experian_attr = [attr for attr in req_attr if attr not in internal_attr]

    data['MISSING_EXPERIAN_ATTR'] = data[experian_attr].isnull().all(axis=1)
    data['MISSING_INTERNAL_ATTR'] = data[internal_attr].isnull().any(axis=1)

    try:
        df = data[req_attr+exclusion_list]
    except:
        print("Attributes do not match")
        raise ValueError("Attributes do not match")

    for col in df.columns:
        df[col] = df[col].astype(float)

    for col, max_val in val_max.items():
        df.loc[df[col] >= max_val, col] = np.nan

    for f,v in imputer.items():
        df[f] = df[f].fillna(v)

    for f in vars_months:
        df[f] = np.where(df[f] >= 240, 240, df[f])
        

    df['ATT3_SOFIDTI_REFRESH'] = np.where(df['ATT_COBORROWER_IND'] == 0, (df['ALL5820'] - df['MTF5820'])*12/df['ATT_GROSS_INCOME'], (df['ALX5839'] - df['MTX5839'])*12/df['ATT_GROSS_INCOME'])

    df['ATT3_SOFIDTI_REFRESH'] = df['ATT3_SOFIDTI_REFRESH'].fillna(0)

    for col in ['VANTAGE_V3_SCORE', 'ATT2_ORIG_VANTAGE', 'FICOCLV8_SCORE', 'ATT2_ORIG_FICO']:
        df[col] = df[col].apply(lambda x: np.nan if x<300 or x>850 else x)

    df['ATT2_VANTAGE_CHANGE'] = df['VANTAGE_V3_SCORE'] - df['ATT2_ORIG_VANTAGE']
    df['ATT2_FICO_CHANGE'] = df['FICOCLV8_SCORE'] - df['ATT2_ORIG_FICO']

    for col in ['ATT2_VANTAGE_CHANGE', 'ATT2_FICO_CHANGE']:
        df[col] = df[col].apply(lambda x: 0 if x>0 else x)

    df['ATT2_VANTAGE_CHANGE'] = df.apply(lambda x: -95.5 if pd.isna(x['VANTAGE_V3_SCORE']) or pd.isna(x['ATT2_ORIG_VANTAGE']) else x['ATT2_VANTAGE_CHANGE'], axis=1)
    df['ATT2_FICO_CHANGE'] = df.apply(lambda x: -80.5 if pd.isna(x['FICOCLV8_SCORE']) or pd.isna(x['ATT2_ORIG_FICO']) else x['ATT2_FICO_CHANGE'], axis=1)

    return df[b1_attr_list+exclusion_list]

    
    
def b2_preprocess(data, attr_list=b2_attr_list):
    
    imputer = {'ATT_DQCNT_6M': 0,
             'ATT_PAST_DUE_TO_SCHD': 0,
             'ATT_TOT_DQ_DAYS_CYC_RATIO_L1M': 0,
             'ATT_IS_DQ2UP_L1M': 0,
             'ATT3_DOWN_TOB0_CNT_12M': 0,
             # 'ATT3_SOFIDTI_REFRESH': 0,
             'ATT_CONS_NODQ2UP_CNT': 0,
             'ATT_DQ2UP_CNT_12M': 0,
             'ATT_NOAUTOPAY_CNT_3M': 0,
             'ATT_CONS_NODQ4UP_CNT': 0,
             'ALL8370': 66.5,
             'ATT_DQ4UP_CNT_3M': 0,
             'ATT_IS_B2_WORST_DQ_L12M': 0,
             'BCA8370': 133.0,
             'ALL8272': 1.0,
             'ATT3_ROLLIN_B2_CNT_12M': 0,
             'ALL2012': 0.0,
             'ATT_REVERSED_CNT_12M': 0,
             'ATT_REVERSED_CNT_3M': 0,
             'ATT_PAST_DUE_TO_INCOME': 0,
             'ATT_UBTI': 0,
             'ALL0438': 2.5,
             'ALS0337': 1.5,
             'ALL8250': 0.0,
              'ALL5820': 0,
              'MTF5820': 0,
              'ALX5839': 0,
              'MTX5839': 0,
             'ATT_COBORROWER_IND': 0,
             'ATT_GROSS_INCOME': 0
              }
    
    val_max = {'ALL0438': 90.0,
              'ALL2012': 90.0,
              'ALL8250': 9990.0,
              'ALL8272': 90.0,
              'ALL8370': 9990.0,
              'ALS0337': 90.0,
              'BCA8370': 9990.0,
              'ALL5820': 999999996,
              'MTF5820': 999999996,
              'ALX5839': 999999996,
              'MTX5839': 999999996,}
    
    vars_months = ['ALL8250',
                 'ALL8272',
                 'ATT_CONS_NODQ4UP_CNT',
                 'ALL8370',
                 'BCA8370',
                 'ATT_CONS_NODQ2UP_CNT']
    
    req_attr = list(imputer.keys())
    internal_attr = [attr for attr in req_attr if attr.startswith('ATT')]
    experian_attr = [attr for attr in req_attr if attr not in internal_attr]

    data['MISSING_EXPERIAN_ATTR'] = data[experian_attr].isnull().all(axis=1)
    data['MISSING_INTERNAL_ATTR'] = data[internal_attr].isnull().any(axis=1)

    try:
        df = data[req_attr+exclusion_list]
    except:
        print("Attributes do not match")
        raise ValueError("Attributes do not match")

    for col in df.columns:
        df[col] = df[col].astype(float)

    for col, max_val in val_max.items():
        df.loc[df[col] >= max_val, col] = np.nan

    for f,v in imputer.items():
        df[f] = df[f].fillna(v)

    for f in vars_months:
        df[f] = np.where(df[f] >= 240, 240, df[f])
    
    df['ATT3_SOFIDTI_REFRESH'] = np.where(df['ATT_COBORROWER_IND'] == 0, (df['ALL5820'] - df['MTF5820'])*12/df['ATT_GROSS_INCOME'], (df['ALX5839'] - df['MTX5839'])*12/df['ATT_GROSS_INCOME'])
    df['ATT3_SOFIDTI_REFRESH'] = df['ATT3_SOFIDTI_REFRESH'].fillna(0)
    
    return df[b2_attr_list+exclusion_list]

    
def b3_preprocess(data):
    
    imputer = {'ATT_DQ2UP_CNT_12M': 0,
             'ATT_PAST_DUE_TO_SCHD': 0,
             'ATT_DQCNT_6M': 0,
             'ATT_CONS_NODQ3UP_CNT': 0,
             'ATT_IS_B3_L1M': 0,
             'ATT_TOT_DQ_DAYS_CYC_RATIO_L3M': 0,
             'ATT3_ROLLIN_B2_CNT_12M': 0,
             'ALL8171': 80.0,
             'ALL2012': 0.0,
             'ALL8370': 22.0,
             'ATT_REVERSED_CNT_12M': 0,
             'ATT_NOAUTOPAY_CNT_3M': 0,
             # 'ATT2_VANTAGE_CHANGE': 0.0,
             'ALL8154': 23.0,
             'ATT_TOT_DQ_DAYS_CYC_RATIO_L1M': 0,
             'ATT_PAST_DUE_AMT_FIXED': 0,
             'ATT_REVERSED_CNT_3M': 0,
             'BCA8370': 128.0,
             'ATT_PAST_DUE_TO_INCOME': 0,
             'ALL8271': 1.0,
             'ATT3_DOWN_TOB0_CNT_12M': 0,
             'ATT_CONS_NODQ5UP_CNT': 0,
             'REV5742': 339692.0,
             'BCA8151': 0.0,
             'ATT_LAST_PAYMENT_AMOUNT_FIXED': 0,
             'REV0318': 5.5,
             'BCC7708': 100.0,
             'ATT_UBTI': 0,
             # 'ATT3_SOFIDTI_REFRESH': 0,
             'ALL8250': 1.0,
             'ATT_REL_REPAY_TERM_MON': -0.0417,
             'ILN4080': 0.0,
             'BCC8338': 25.0,
             'STU5031': 0.0,
             'AUA8370': 47.0,
              'ALL5820': 0,
              'MTF5820': 0,
              'ALX5839': 0,
              'MTX5839': 0,
             'ATT_COBORROWER_IND': 0,
             'ATT_GROSS_INCOME': 0,
              'VANTAGE_V3_SCORE': 0,
              'ATT2_ORIG_VANTAGE': 0,
               }
    
    val_max = {'ALL2012': 90.0,
              'ALL8154': 9990.0,
              'ALL8171': 90.0,
              'ALL8250': 9990.0,
              'ALL8271': 90.0,
              'ALL8370': 9990.0,
              'AUA8370': 9990.0,
              'BCA8151': 9990.0,
              'BCA8370': 9990.0,
              'BCC7708': 100.0,
              'BCC8338': 25.0,
              'ILN4080': 990.0,
              'REV0318': 90.0,
              'STU5031': 999999990.0,
              'ALL5820': 999999996,
              'MTF5820': 999999996,
              'ALX5839': 999999996,
              'MTX5839': 999999996,
              'VANTAGE_V3_SCORE': 850,
              'ATT2_ORIG_VANTAGE': 850}
    
    vars_months = ['ATT_CONS_NODQ3UP_CNT',
                 'BCA8151',
                 'ALL8370',
                 'BCA8370',
                 'AUA8370',
                 'ALL8271',
                 'ALL8250']
    
    req_attr = list(imputer.keys())
    internal_attr = [attr for attr in req_attr if attr.startswith('ATT') and (attr not in ['ATT2_ORIG_VANTAGE'])]
    experian_attr = [attr for attr in req_attr if attr not in internal_attr]

    data['MISSING_EXPERIAN_ATTR'] = data[experian_attr].isnull().all(axis=1)
    data['MISSING_INTERNAL_ATTR'] = data[internal_attr].isnull().any(axis=1)

    try:
        df = data[req_attr+exclusion_list]
    except:
        print("Attributes do not match")
        raise ValueError("Attributes do not match")

    for col in df.columns:
        df[col] = df[col].astype(float)

    for col, max_val in val_max.items():
        df.loc[df[col] >= max_val, col] = np.nan

    for f,v in imputer.items():
        df[f] = df[f].fillna(v)

    for f in vars_months:
        df[f] = np.where(df[f] >= 240, 240, df[f])
    
    df['ATT3_SOFIDTI_REFRESH'] = np.where(df['ATT_COBORROWER_IND'] == 0, (df['ALL5820'] - df['MTF5820'])*12/df['ATT_GROSS_INCOME'], (df['ALX5839'] - df['MTX5839'])*12/df['ATT_GROSS_INCOME'])
    df['ATT3_SOFIDTI_REFRESH'] = df['ATT3_SOFIDTI_REFRESH'].fillna(0)
    
    for col in ['VANTAGE_V3_SCORE', 'ATT2_ORIG_VANTAGE']:
        df[col] = df[col].apply(lambda x: np.nan if x<300 or x>850 else x)

    df['ATT2_VANTAGE_CHANGE'] = df['VANTAGE_V3_SCORE'] - df['ATT2_ORIG_VANTAGE']

    df['ATT2_VANTAGE_CHANGE'] = df['ATT2_VANTAGE_CHANGE'].apply(lambda x: 0 if x>0 else x)
    
    df['ATT2_VANTAGE_CHANGE'] = df.apply(lambda x: -95.5 if pd.isna(x['VANTAGE_V3_SCORE']) or pd.isna(x['ATT2_ORIG_VANTAGE']) else x['ATT2_VANTAGE_CHANGE'], axis=1)

    return df[b3_attr_list+exclusion_list]

    
def b4_preprocess(data):
    
    imputer = {'ATT_TOT_DQ_DAYS_CYC_RATIO_L3M': 0,
             'ATT_DQ2UP_CNT_12M': 0,
             'ATT_TOT_DQ_DAYS_CYC_RATIO_L1M': 0,
             'ATT_PAST_DUE_TO_SCHD': 0,
             'ATT3_IS_STRROLLIN_B4UP': 0,
             'ATT_DQCNT_12M': 0,
             'ATT3_ROLLIN_B1_CNT_12M': 0,
             'ATT3_IS_STRROLLIN_B5': 0,
             'ALL2226': 11.5,
             'ATT_IS_B4_WORST_DQ_L3M': 0,
             'ALL4980': 0.0,
             'ATT_PAST_DUE_AMT_FIXED': 0,
             'ATT3_ROLLIN_B3_CNT_12M': 0,
             'REV2126': 1.5,
             'ATT_TOT_DQ_DAYS_CYC_RATIO_L6M': 0,
             'ALL8154': 23.0,
             'ATT_CONS_NODQ5UP_CNT': 0,
             'ATT_REMAINING_BAL_RATIO': 0,
             'REV5742': 150.0,
             'ATT_NOAUTOPAY_CNT_12M': 0}

    val_max = {'ALL2226': 90.0,
              'ALL4980': 90.0,
              'ALL8154': 9990.0,
              'REV2126': 90.0,
              'REV5742': 999999990.0}

    vars_months = ['ATT_CONS_NODQ5UP_CNT']

    req_attr = list(imputer.keys())
    internal_attr = [attr for attr in req_attr if attr.startswith('ATT')]
    experian_attr = [attr for attr in req_attr if attr not in internal_attr]

    data['MISSING_EXPERIAN_ATTR'] = data[experian_attr].isnull().all(axis=1)
    data['MISSING_INTERNAL_ATTR'] = data[internal_attr].isnull().any(axis=1)

    try:
        df = data[req_attr+exclusion_list]
    except:
        print("Attributes do not match")
        raise ValueError("Attributes do not match")

    for col in df.columns:
        df[col] = df[col].astype(float)

    for col, max_val in val_max.items():
        df.loc[df[col] >= max_val, col] = np.nan

    for f,v in imputer.items():
        df[f] = df[f].fillna(v)

    for f in vars_months:
        df[f] = np.where(df[f] >= 240, 240, df[f])

    return df[b4_attr_list+exclusion_list]
