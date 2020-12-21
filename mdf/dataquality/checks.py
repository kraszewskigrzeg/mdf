import pandas as pd

"""
All functions supported by pandas.
Sprak not yet supported.

"""

from ..utils import validate_pandas_df, validate_check_results


def Aggregated(config, df1, df2=None):
    """
Config example:
{
    'column': 'column_name',
    'function': 'function_name',
    'groupby': [
        'dataset_id',
        # ... dataset_id should be always specified 
        # as it is a constatnt value for whole dataset
        'group_column_name_1', 
        'gorup_column_anme_2'
    ],
    'percent': 0.1,
    'absolute': 15,
}
    """
    df1 = validate_pandas_df(config,df1)
    if df2 is not None: 
        df2 = validate_pandas_df(config,df2)
        
    dat1 = df1 \
           .groupby(config['groupby'], as_index=False) \
           .agg({
               config['column']: config['function']
           })

    dat2 = df2 \
           .groupby(config['groupby'], as_index=False) \
           .agg({
               config['column']: config['function']
           })

    dat = dat1.merge(dat2, on=config['groupby'], suffixes=('_now','_then'), how='left')
    dat['diff'] = dat[config['column']+'_then'] - dat[config['column']+'_now']
    dat['ratio'] = ( (dat[config['column']+'_then'] - dat[config['column']+'_now']) 
                     / dat[config['column']+'_then'] )

    return validate_check_results(config, dat)


def CrossField(config, df1, df2=None):
    
    df1 = validate_pandas_df(config, df1)
    df1['ref'] = df1[config['reference_column']]

    dat = df1 \
           .groupby(config['groupby'], as_index=False) \
           .agg({
               config['column']: config['function'],
               'ref': config['reference_function'],
           })
    dat['diff'] = dat[config['column']] - dat['ref']
    dat['ratio'] = ( (dat[config['column']] - dat['ref']) 
                     / dat['ref'] )
    
    return validate_check_results(config, dat)


def Filtered(config, df1, df2=None):
    count_all = len(df1)
    count_filtered = len(df1.query(f"{config['filter_string']}"))
    dat = pd.DataFrame({
        'count_all': count_all,
        'count_filtered': count_filtered
    }, index =[0] )
    dat['diff'] = dat['count_all'] - dat['count_filtered']
    dat['ratio'] = dat['count_filtered'] / dat['count_all'] 
    
    return validate_check_results(config, dat)
    
    
def Added(config, df1, df2=None):
    values_now = df1[config['columns']].values
    values_then = df2[config['columns']].values
    set_values_now = {tuple(row) for row in values_now}
    set_values_then = {tuple(row) for row in values_then}
    added = set_values_now - set_values_then
    return  pd.DataFrame({'added': list(added)})


def Removed(config, df1, df2=None):
    values_now = df1[config['columns']].values
    values_then = df2[config['columns']].values
    set_values_now = {tuple(row) for row in values_now}
    set_values_then = {tuple(row) for row in values_then}
    removed = set_values_then - set_values_now
    return  pd.DataFrame({'removed': list(removed)})


def Shared(config, df1, df2=None):
    values_now = df1[config['columns']].values
    values_then = df2[config['columns']].values
    set_values_now = {tuple(row) for row in values_now}
    set_values_then = {tuple(row) for row in values_then}
    
    diff = len(set_values_then) - len(set_values_then.intersection(set_values_now)) 
    ratio =  len(set_values_then.intersection(set_values_now)) / len(set_values_then)
    dat = pd.DataFrame({
        'shared': ','.join(config['columns']),
        'diff': diff,
        'ratio': ratio,
    }, index=[0])
    
    return validate_check_results(config, dat)



checksdict = {
    'Aggregated': Aggregated, 
    'CrossField': CrossField, 
    'Filtered': Filtered, 
    'Added': Added, 
    'Shared': Shared, 
    'Removed': Removed   
}