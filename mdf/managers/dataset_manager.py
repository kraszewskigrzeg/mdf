from .manager import Manager
from ..models import DataSet, DqChecksResults
from ..dataquality import Added, Aggregated, CrossField, Filtered, Removed, Shared
from ..datacleansing.string_cleaner import StringDataCleaner
from ..datacleansing.number_cleaner import NumberDataCleaner


class DataSetManager(Manager):
    
    def __init__(self, session, mdf, auto_save=True):
        super().__init__(session, auto_save)
        self._mdf = mdf

    def get(self, **kwargs):
        """
        Get DataSets according to specified filters
        """
        if kwargs:
            dat = self.s.query(DataSet).filter_by(**kwargs).all()
        else:
            dat = self.s.query(DataSet).all()
        self.save()
        return dat
    
    def _get_prev_id_on_success(self, dataset_id) -> int:
        last_success = self._mdf.sql(f""" 
        SELECT dataset_id_last_success 
        FROM workflow.last_dataset_on_success_v 
        WHERE dataset_id_now = {dataset_id}""")
        if not last_success.empty:
            return last_success['dataset_id_last_success'][0] 
        else:
            raise Exception(f"DataSet ID: {dataset_id} not found!")
    
    #############################################################################
    ### DQ checks Execution
    #############################################################################
    
    def _in_progress(self, dataset_id):
        ds = self.get(dataset_id=dataset_id)[0]
        ds.dataset_quality_status = 'In Progress'
        self.save([ds])
    
    def _validate_results(self, dataset_id, results):
        status = 'S'
        if results:
            # here some more fancy logic can be implemented!
            
            status = 'F'
        
        ds = self.get(dataset_id=dataset_id)[0]
        ds.dataset_quality_status = status
        self.save([ds])
        return status
    
    def execute_quality_scripts(self, dataset_id, prev_dataset_id, config):
        """
        Execute quality checks manualy.
        """
        checksdict = {
            'Added': Added, 
            'Aggregated': Aggregated, 
            'CrossField': CrossField, 
            'Filtered': Filtered, 
            'Removed': Removed, 
            'Shared': Shared,
        }
        
        data_now = self.get_data(dataset_id)
        if dataset_id == prev_dataset_id:
            data_then = data_now
        else:
            data_then = self.get_data(prev_dataset_id)
    
        results = {}
        for name, check in config.items():
            res = checksdict[check['type']](config=check, 
                                        df1=data_now, 
                                        df2=data_then)
            if not res.empty:
                results[name] = res.to_dict(orient='records')
        
        return results
    
    def load_results(self, dataset_id, results):
        r = DqChecksResults(dataset_id=dataset_id, issues=results)
        self.save([r])
    
    #############################################################################
    ### data enhancement
    #############################################################################
    
    def execute_enhancemnet_sripts(self, dataset_id, config):
        data_raw = self.get_data(dataset_id)
        sdc = StringDataCleaner(data_raw, config)
        data_string_cleaned = sdc.clean_all_strings()
        ndc = NumberDataCleaner(data_string_cleaned, config)
        data = ndc.clean_all_numbers()
        return data
    
    def load_to_clean(self, data):
        assert hasattr(data, 'dataset_id'), "You are trying to insert data with no dataset_id!"
        dataset_ids = data.dataset_id.unique()
        assert len(dataset_ids) == 1, "You are trying to insert data with multiple or None dataset_ids!"
        dataset_id = int(data.dataset_id.unique()[0])
        table_name = self.s.query(DataSet).get(dataset_id).datafeed.project.project_name + '_clean'
        ds = self.get(dataset_id = dataset_id)[0]
        try:
            data.to_sql(name=table_name,
                        schema='data', 
                        con=self._mdf.engine, 
                        if_exists='append',
                        index=False)
            ds.dataset_enhancement_status = 'S'
        except Exception as e:
            ds.dataset_enhancement_status = 'F'
            self.save()
            raise e
            
        self.save()
        

    #############################################################################
    ### data assigned to one dataset_id acquisition
    #############################################################################
    
    def get_data(self, dataset_id):
        """
        Get data assigned to specified dataset_id
        """
        
        table = self.s.query(DataSet).get(dataset_id).datafeed.project.project_name
        
        q = f"""
        SELECT 
            *
        FROM data."{table}"
        WHERE dataset_id = {dataset_id}
        """ 
        return self._mdf.sql(q)
    