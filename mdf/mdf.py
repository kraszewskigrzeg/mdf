from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from .models import Base, DataFeed, Project, DataSet
from .managers import ProjectManager, DataFeedManager, DataSetManager
import os
import datetime
import logging 
import pandas as pd

class MicroDataFactory:
    """
    Works for Postgres only for now
    """
    def __init__(self, host, port, db_name, user, password, auto_save=True):
        self.host = host
        self.port = port
        self.db_name = db_name
        self.user = user
        self._password = password

        self.auto_save = auto_save
        con_str = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db_name}"
        self._set_up_connection(con_str)
        Base.metadata.create_all(self.engine)
        self.projects = ProjectManager(self.s, self.auto_save)
        self.datafeeds = DataFeedManager(self.s, self.auto_save)
        self.datasets = DataSetManager(self.s, self, self.auto_save)

    def _set_up_connection(self, con_str):
        self.engine = create_engine(con_str)
        Session = sessionmaker(bind=self.engine)
        self.s = Session()
    
    def get_spark_context(self, app_name):
        from .spark_utils import DataFactorySparkSession
        
        return DataFactorySparkSession(
            app_name, 
            self.user, 
            self._password,
            self.host, 
            self.db_name, 
            self.port
        )

    def sql(self, query, limit=None):
        r = self.engine.execute(query)
        logging.info(f"Executed: {query} at {datetime.datetime.now()}")
        columns = r.keys()
        if "SELECT" in query.strip()[:7].upper():
            if limit is not None:
                data = r.fetchmany(limit)
            else:
                data = r.fetchall()
            return pd.DataFrame(data=data, columns=columns)
                        
    def __del__(self):
        if self.auto_save:
            self.projects.save()
        del self
        
    @property
    def size(self):
        return (len(self.projects),
                len(self.datafeeds),
                len(self.datasets))
    
    def __repr__(self):
        return f"""
DataFactory:
 > Projects: {len(self.projects)},
 > DataFeeds: {len(self.datafeeds)},
 > DataSets: {len(self.datasets)}.
        """
