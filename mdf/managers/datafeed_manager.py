from .manager import Manager
from ..models import DataFeed


class DataFeedManager(Manager):
        
    def get(self,**kwargs):
        if kwargs:
            dat = self.s.query(DataFeed).filter_by(**kwargs).all()
        else:
            dat = self.s.query(DataFeed).all()
        self.save()
        return dat
    
    def create(self,
               project_id,
               datafeed_name,
               datafeed_execution_script=None,
               datafeed_quality_rules:dict=None,
               datafeed_enhancement_rules=None,
               datafeed_is_running=None,
               datafeed_last_executed=None,
               datafeed_execution_count=None):
        
        new_df = DataFeed(
            project_id=project_id,
            datafeed_name = datafeed_name,
            datafeed_execution_script = datafeed_execution_script,
            datafeed_quality_rules = datafeed_quality_rules,
            datafeed_enhancement_rules = datafeed_enhancement_rules,
            datafeed_is_running = datafeed_is_running,
            datafeed_last_executed = datafeed_last_executed,
            datafeed_execution_count = datafeed_execution_count
        )
        
        self.s.add(new_df)
        
        if self.auto_save:
            self.save()
