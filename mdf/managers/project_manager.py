from .manager import Manager
from ..models import Project


class ProjectManager(Manager):
        
    def get(self, **kwargs):
        if kwargs:
            dat = self.s.query(Project).filter_by(**kwargs).all()
        else:
            dat = self.s.query(Project).all()
        self.save()
        return dat
    
    def create(self,
               name,
               description, 
               table_raw_config: dict, 
               auto_save=True):
        
        """
        table_raw: str must be like const.<your_table_name> 
        table_clean: str must be like nconst.<your_table_name> 
        as those tables will be created in different schemas 
        """
        
        new_proj = Project(
                project_name=name,
                project_description=description,
                project_table_raw_config=table_raw_config,
        )
        
        self.s.add(new_proj)
        
        if self.auto_save:
            self.save()