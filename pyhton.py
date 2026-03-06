def calculate_psi(expected, actual, score_dev, score_act, buckettype='quantiles', buckets=10, axis=0):
    '''Calculate the PSI (population stability index) across all variables
    Args:
       expected: numpy matrix of original values
       actual: numpy matrix of new values, same size as expected
       buckettype: type of strategy for creating buckets, bins splits into even splits, quantiles splits into quantile buckets
       buckets: number of quantiles to use in bucketing variables
       axis: axis by which variables are defined, 0 for vertical, 1 for horizontal
    Returns:
       psi_values: ndarray of psi values for each variable
    Author:
       Matthew Burke
       github.com/mwburke
       worksofchart.com
    '''
    
    expected_data = expected[~expected[score_dev].isna()][score_dev]
    actual_data = actual[~actual[score_act].isna()][score_act]

    def psi(expected_array, actual_array, buckets):
        '''Calculate the PSI for a single variable
        Args:
           expected_array: numpy array of original values
           actual_array: numpy array of new values, same size as expected
           buckets: number of percentile ranges to bucket the values into
        Returns:
           psi_value: calculated PSI value
        '''

        def scale_range (input, min, max):
            input += -(np.min(input))
            input /= np.max(input) / (max - min)
            input += min
            return input


        breakpoints = np.arange(0, buckets + 1) / (buckets) * 100

        if buckettype == 'bins':
            breakpoints = scale_range(breakpoints, np.min(expected_array), np.max(expected_array))
        elif buckettype == 'quantiles':
            breakpoints = np.stack([np.percentile(expected_array, b) for b in breakpoints])



        expected_percents = np.histogram(expected_array, breakpoints)[0] / len(expected_array)
        actual_percents = np.histogram(actual_array, breakpoints)[0] / len(actual_array)

        def sub_psi(e_perc, a_perc):
            '''Calculate the actual PSI value from comparing the values.
               Update the actual value to a very small number if equal to zero
            '''
            if a_perc == 0:
                a_perc = 0.0001
            if e_perc == 0:
                e_perc = 0.0001

            value = (e_perc - a_perc) * np.log(e_perc / a_perc)
            return(value)
        

        psi_value = np.sum(sub_psi(expected_percents[i], actual_percents[i]) for i in range(0, len(expected_percents)))

        return psi_value

    if len(expected_data.shape) == 1:
        psi_values = np.empty(len(expected_data.shape))
    else:
        psi_values = np.empty(expected_data.shape[axis])

    for i in range(0, len(psi_values)):
        if len(psi_values) == 1:
            psi_values = psi(expected_data, actual_data, buckets)
        elif axis == 0:
            psi_values[i] = psi(expected_data[:,i], actual_data[:,i], buckets)
        elif axis == 1:
            psi_values[i] = psi(expected_data[i,:], actual_data[i,:], buckets)

    return(psi_values)

def calculate_psi_by_month(dev, act, score_dev, score_act, date):
    _ = {}
    _['volume'] = act.groupby(date).apply(lambda x: len(x[~x[score_act].isna()])).to_dict()
    _['psi'] = act.groupby(date).apply(lambda x: calculate_psi(dev, x, score_dev, score_act)).to_dict()
    return pd.DataFrame(_)

def calculate_var_psi_old(dev, act, features, date, yyyymm):
    data = pd.DataFrame()
    mon = act[act[date]==yyyymm]
    for var in features:
        temp = pd.DataFrame()
        temp['var'] = [var]
        temp['psi'] = calculate_psi(dev, mon, var, var)
        data = pd.concat([data, temp], ignore_index=True)
    return data

def calculate_psi_categorical(data_ref, data_new, feature, null_value='NULL', special_values=None):
    """
    Calculates the Population Stability Index (PSI) for a categorical feature, handling nulls and special values.
    Args:
        data_ref (pd.DataFrame): Reference dataset.
        data_new (pd.DataFrame): New dataset to compare against.
        feature (str): Name of the categorical feature.
        null_value (str, optional): Value to represent nulls in the data. Defaults to 'NULL'.
        special_values (list, optional): List of special values to handle separately. Defaults to None.
    Returns:
        float: The PSI value for the given feature.
    """
    # Handle nulls and special values
    ref = data_ref.copy()
    new = data_new.copy()
    if special_values != None:
        ref[feature] = ref[feature].fillna(null_value).replace(special_values, null_value)
        new[feature] = new[feature].fillna(null_value).replace(special_values, null_value)
    # Calculate category proportions in reference data
    ref_counts = ref[feature].value_counts(normalize=True)
    # Calculate category proportions in new data
    new_counts = new[feature].value_counts(normalize=True)
    # Calculate PSI
    psi = 0
    for category in set(ref[feature].unique()) | set(new[feature].unique()):
        ref_prop = ref_counts.get(category, 0)
        new_prop = new_counts.get(category, 0)
        psi += (new_prop - ref_prop) * np.log(new_prop / ref_prop) if (ref_prop > 0 and new_prop > 0) else 0
    return  psi

def calculate_var_psi(dev, act, features, date, yyyymm):
    data = pd.DataFrame()
    mon = act[act[date]==yyyymm]
    for var in features:
        if var == 'sloppy_ind_d3':
            temp = pd.DataFrame()
            temp['var'] = [var]
            temp['psi'] = calculate_psi_categorical(dev, mon, var,special_values = 60.0 )
            data = data._append(temp,ignore_index=True)
            
        elif var == 'ATT_IS_AUTOPAY':
            temp = pd.DataFrame()
            temp['var'] = [var]
            temp['psi'] = calculate_psi_categorical(dev, mon, var)
            data = data._append(temp,ignore_index=True)
        elif var == 'IQF9417':
            temp = pd.DataFrame()
            temp['var'] = [var]
            temp['psi'] = calculate_psi_categorical(dev, mon, var)
            data = data._append(temp,ignore_index=True)
        elif var == 'ATT_CONS_NODQ2UP_CNT':
            temp = pd.DataFrame()
            temp['var'] = [var]
            temp['psi'] = calculate_psi_categorical(dev, mon, var)
            data = data._append(temp,ignore_index=True)
        elif var == 'ATT_CONS_NODQCNT':
            temp = pd.DataFrame()
            temp['var'] = [var]
            temp['psi'] = calculate_psi_categorical(dev, mon, var)
            data = data._append(temp,ignore_index=True)
        elif var == 'REV3517':
            temp = pd.DataFrame()
            temp['var'] = [var]
            temp['psi'] = calculate_psi_categorical(dev, mon, var)
            data = data._append(temp,ignore_index=True)
        else:
            temp = pd.DataFrame()
            temp['var'] = [var]
            temp['psi'] = calculate_psi(dev, mon, var, var)
            data = data._append(temp,ignore_index=True)
    return data
