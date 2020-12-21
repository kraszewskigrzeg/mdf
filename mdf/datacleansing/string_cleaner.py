from collections import defaultdict
import re


class StringCleaner:
    
    """
    Config defines how to clean choosen column.
    {
        'on_error': None,
        
        'strip': True,
        
        'remove': [
            '|',
            '(',
            ')'
        ],
        
        'replace': {
            ',': '.',
            '|': ' OR ',            
        },
        
        'categorize': {
            'label1': [
                'regexp1',
                'regexp2'
            ],
            
            'label2': [
                'regexp3',
                'regexp4'
            ],
        }
    }
    """
    
    def __init__(self, data, config):
        self.data = data
        self.config = config
    
    def _replace(self, val):
        c = self.config.get('replace', None)
        result = val
        if c:
            for n,v in c.items():
                result = result.replace(n,v)
            
            return result
        return result
    
    def _remove(self, val):
        c = self.config.get('remove', None)
        result = val
        if c:
            for n in c:
                result = result.replace(n,'')
            return result
        return result
    
    ##################################################################################################
    ###  Categorizer 
    ################################################################################################## 
    
    def _categorize(self, val):
        c = self.config.get('categorize', None)
        
        if not c:
            return val
        
        results = defaultdict(int)
        for label, regexps in c.items():
            for r in regexps:
                r=r.upper()
                if re.findall(r, val.upper()):
                    results[label] += 1
        return max(results, key=results.get) if results else None
    
    def _cleanse(self):       
        res = self.data
        
        strip = self.config.get('strip', None)
        capitalize = self.config.get('capitalize', None)
        upper = self.config.get('upper', None)
        lower = self.config.get('lower', None)
        
        if strip:
            print('stripping')
            res = res.strip()
        
        res = self._remove(res)
        res = self._replace(res)
        res = self._categorize(res)
        
        if capitalize:
            res = res.capitalize()
            
        if upper:
            res = res.upper()
            
        if lower:
            res = res.lower()
            
        return res
        
    def clean(self):
        return self._cleanse()
    
    
class StringDataCleaner:    
    def __init__(self, data, config):
        self.data = data
        self.config = config

    def clean_all_strings(self):
        result = self.data
        for n, c in self.config['strings'].items():

            def clean_strings(value):
                if value:
                    p = StringCleaner(value.upper(), c)
                    return p.clean()
                else:
                    return None
            
            result[n] = result[n].apply(clean_strings)

        return result