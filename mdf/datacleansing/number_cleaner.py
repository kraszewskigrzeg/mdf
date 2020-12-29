import re
import logging

try:
    from price_parser import Price
except:
    logging.warning("could not import price_parser only regex engine will be available")


class NumberCleaner:
    
    """
    config = {
        'strings': {
            ...
        },
        
        'numbers': {
             'some_almost_number_column': {
             # '$123,78 .-' -> 123.78
                'engine': 'price',
                    # regex / price
                    # if regexp sep will be used to replace this value to '.'
                
                'regex': '[0-9,]+',            
                    # Will use this pattern to get value
                                              
                'sep': ',',
                    # If specified and price engine will be passeed to parser as separator tip 
                    # if regexp engine this will be changed to '.'
                    
                'replace': ' ',
                
                'on_error': None,
                    # value that will be returned on error
            },
            
            'some_oher_column': {
            # '21,38 m2' -> 21.38
                'engine': 'regex',
                'regex': '[0-9,]+',
                'sep': ',',
            },
            
            'some_oher_column': {
            # '$123,78 .-' -> 123.78
                'engine': 'price',
                'sep': ',',
            }
        }
    }
    """
    
    def __init__(self, data, config):
        self.data = data
        self.config = config
    
    @staticmethod
    def _price_engine(value, regexp=None, sep=None):
        p = Price.fromstring(value, decimal_separator=sep)
        return p.amount_float
    
    @staticmethod
    def _regex_engine(value, regexp, sep=None):
        res = re.findall(regexp, value)
        v = res[0]
        
        if sep and res:
            v = v.replace(sep, '.')
        return float(v)
    
    def _cleanse(self):
        obj = self.config
        engines = {
            'regex': self._regex_engine,
            'price': self._price_engine,
        }
        
        try:
            val = self.data.replace(' ', '')
            
            result = engines[obj.get('engine', 'regex')](
                value=val, 
                regexp=obj.get('regex', None), 
                sep=obj.get('sep', None)
            )
        except:
            result = obj.get('on_error', None)
        
        return result
    
    def clean(self):
        return self._cleanse()
    

class NumberDataCleaner:
    def __init__(self, data, config):
        self.data = data
        self.config = config

    
    def clean_all_numbers(self):
        result = self.data
        for n, c in self.config['numbers'].items():
            
            def clean_numbers(value):
                if value:
                    p = NumberCleaner(value, c)
                    return p.clean()
                else:
                    return None
                
            result[n] = result[n].apply(clean_numbers)

        return result
