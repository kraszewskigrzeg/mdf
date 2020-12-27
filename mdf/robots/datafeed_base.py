import pandas as pd
from ..models import DataSet

class DataFeedBase:
    
    def __init__(self, mdf, datafeed_id):
        self.mdf = mdf
        self.df = mdf.datafeeds.get(datafeed_id = datafeed_id)[0]
        
    def execute(self):
        """
        Mdf will call this method to collect data hence it must be implemented       
        """
        raise NotImplementedError("This function needs to return data in form [{'col1': 'val1'}, {'col1': 'val2'}]")
    
    @staticmethod
    def add_dataset_id(dataset_id, data):
        data['dataset_id'] = dataset_id
        return data
    
    def write(self):
        ds = DataSet(datafeed_id=self.df.datafeed_id)
        self.mdf.s.add(ds)
        self.mdf.s.commit()
        table_name = self.df.project.project_name
        print(f""" 
EXECUTING DATAFEED: {self.df}
dataset_id: {ds.dataset_id}
table_name: {table_name}
        """)
        try:
            data = pd.DataFrame(self.execute())
            data = self.add_dataset_id(ds.dataset_id, data)
            data.to_sql(name=table_name,
                        schema='data', 
                        con=self.mdf.engine, 
                        if_exists='append',
                        index=False)
            self.mdf.s.commit()
            return True
        except Exception as e:
            self.mdf.s.rollback()
            raise e
            
        