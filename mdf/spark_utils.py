from pyspark.sql import SparkSession

class DataFactorySparkSession:
    
    def __init__(self, 
                 name, 
                 user, 
                 password, 
                 host,
                 db_name,
                 port='5432'):
        
        self._user = user
        self._password = password
        self.host = host
        self.db_name = db_name
        self.port = port
        self._spark = SparkSession \
                     .builder \
                     .appName(name) \
                     .config("spark.driver.memory","2g")\
                     .config("spark.executor.memory","2g")\
                     .config("spark.master", "local[4]")\
                     .getOrCreate()

    def __getattr__(self, name):
        return getattr(self._spark, name)
        
    def sql(self, sql):
        df = self._spark.read \
                        .format("jdbc") \
                        .option("url", f"jdbc:postgresql://{self.host}:{self.port}/{self.db_name}") \
                        .option("query", sql) \
                        .option("user", self._user) \
                        .option("password", self._password) \
                        .option("driver", "org.postgresql.Driver") \
                        .load()
        return df
    
    def mdf_write(self, data, tablename):
        data.write \
            .format("jdbc") \
            .option("url", f"jdbc:postgresql://{self.host}:{self.port}/{self.db_name}") \
            .option("dbtable", f"data.{tablename}") \
            .option("user", self._user) \
            .option("password", self._password) \
            .option("driver", "org.postgresql.Driver").mode('overwrite').save()

    def __del__(self):
        self._spark.stop()
        del self