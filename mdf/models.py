from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, ForeignKey, Boolean
from sqlalchemy.types import JSON, DateTime
from sqlalchemy.orm import relationship
from datetime import datetime
from IPython.display import display, Markdown
import importlib.util
import os

Base = declarative_base()

class Project(Base):
    __tablename__ = 'projects'
    __table_args__ = {'schema': 'workflow', 'extend_existing': True}
    
    project_id = Column(Integer, primary_key=True)
    project_name = Column(String, nullable=False, unique=True)
    project_description = Column(String)
    project_table_raw_config = Column(JSON, nullable=False)
    created = Column(DateTime, nullable=False, default=datetime.now())
    last_updated = Column(DateTime, nullable=False, default=datetime.now())
    
    def __repr__(self):
        return f"<Project Id: {self.project_id} Name: {self.project_name} Created: {self.created}>"
    
    def print_description(self):
        try:
            display(Markdown(self.project_description))
        except:
            print(self.project_description)

            
class DataSet(Base):
    __tablename__ = 'datasets'
    __table_args__ = {'schema': 'workflow', 'extend_existing': True}
    
    dataset_id = Column(Integer, primary_key=True)
    datafeed_id = Column(Integer, ForeignKey('workflow.datafeeds.datafeed_id'))
    
    datafeed = relationship("DataFeed", back_populates="datasets")
    dataset_quality_status = Column(String, nullable=False, default='P')
    dataset_enhancement_status = Column(String, nullable=False, default='P')
    dataset_quality_pushed = Column(Boolean, default=False)
    created = Column(DateTime, nullable=False, default=datetime.now())
    last_updated = Column(DateTime, nullable=False, default=datetime.now())
    
    def __repr__(self):
        return f"<Dataset Id: {self.dataset_id} QualityStatus: {self.dataset_quality_status} EnchanceStatus: {self.dataset_enhancement_status}>"

    
class DataFeed(Base):
    __tablename__ = 'datafeeds'
    __table_args__ = {'schema': 'workflow', 'extend_existing': True}
    
    datafeed_id = Column(Integer, primary_key=True)
    project_id = Column(Integer, ForeignKey('workflow.projects.project_id'))
    project = relationship("Project", back_populates="datafeeds")
    
    datafeed_name = Column(String, nullable=False, unique=True)
    datafeed_execution_script = Column(String)
    datafeed_quality_rules = Column(JSON)
    datafeed_enhancement_rules = Column(JSON)
    datafeed_is_running = Column(Boolean, default=False)
    datafeed_last_executed = Column(DateTime, default=datetime.now())
    datafeed_execution_count = Column(Integer, default=0)
    created = Column(DateTime, nullable=False, default=datetime.now())
    last_updated = Column(DateTime, nullable=False, default=datetime.now())
    
    def __repr__(self):
        return f"<DataFeed: {self.datafeed_id}, {self.datafeed_name} IsRunning: {self.datafeed_is_running} LastExecuted: {self.datafeed_last_executed}>"
    
    def get_datafeed_class(self):
        spec = importlib.util.spec_from_file_location("df", self.datafeed_execution_script)
        try:
            module = importlib.util.module_from_spec(spec)
        except AttributeError as e:
            raise Exception(f'DataFeed Code not found in {self.datafeed_execution_script}')
        spec.loader.exec_module(module)
        return getattr(module, self.datafeed_name)
    
    def get_datafeed_object(self, mdf):
        try:
            Klass = self.get_datafeed_class()
            self.datafeed_is_running = True
        except AttributeError as e:
            raise Exception(f'DataFeed Code not found in {self.datafeed_execution_script}')
        return Klass(mdf, self.datafeed_id)
    
    def get_datafeed_code(self):
        with open(self.datafeed_execution_script, 'r') as file:
            code = file.read()
        return code
    
    def save_datafeed_code(self, code: str):
        '''
        This method will overwrite the content of the file!
        '''
        confirmation = input('This method will overwrite the content of the file! Do you want to continue [Y] ?')
        if confirmation.upper() =='Y':
            msg = 'Path to execution sript of this datafeed is missing!'
            assert self.datafeed_execution_script is not None, msg
            
            code_path = os.path.dirname(os.path.realpath(self.datafeed_execution_script))
            if not os.path.exists(code_path):
                os.makedirs(code_path)
                
            with open(self.datafeed_execution_script, 'w') as file:
                file.write(code)
    
    def execute(self, mdf, should_write=False, **kwargs):
        
        try:
            Klass = self.get_datafeed_class()
            self.datafeed_is_running = True
        except AttributeError as e:
            raise Exception(f'DataFeed Code not found in {self.datafeed_execution_script}')
        df = Klass(mdf, self.datafeed_id)
        if should_write:
            df.write()
            self.datafeed_execution_count += 1
            self.datafeed_is_running = False
        else:
            return df.execute(limit=kwargs.get('limit', -1))

        
class DqChecksResults(Base):
    __tablename__ = 'dq_checks_results'
    __table_args__ = {'schema': 'workflow', 'extend_existing': True}
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    dataset_id = Column(Integer, ForeignKey('workflow.datasets.dataset_id'))
    checks_date = Column(DateTime, nullable=False, default=datetime.now())
    issues = Column(JSON)
    dataset = relationship("DataSet", back_populates="dq_checks_results")
    

Project.datafeeds = relationship('DataFeed', order_by=DataFeed.datafeed_id, back_populates='project')
    
DataFeed.datasets = relationship('DataSet', order_by=DataSet.dataset_id, back_populates='datafeed')

DataSet.dq_checks_results = relationship('DqChecksResults', order_by=DqChecksResults.id, back_populates='dataset')