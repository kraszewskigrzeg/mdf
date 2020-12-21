def validate_pandas_df(config, df):
    require_numeric_functions = [
        'mean', 'sum', 'size', 'first', 'last','std','var'
    ]
    
    if config['to_replace']:
        df[config['column']] = df[config['column']]\
                                .str.replace(config['to_replace'],'')
    
    if config['function'] in require_numeric_functions:
        df = df.astype({config['column']: float})
        
    return df


def validate_check_results(config, df):
    if config['percent'] and config['absolute']:
        df = df[(abs(df['ratio'])>=config['percent']) & 
                (abs(df['diff'])>=config['absolute'])]
        
    elif config['percent'] and not config['absolute']:
        df = df[abs(df['ratio'])>=config['percent']]
    
    elif not config['percent'] and config['absolute']:
        df = df[abs(df['diff'])>=config['absolute']]
    
    return df