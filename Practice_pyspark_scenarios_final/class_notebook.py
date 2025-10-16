# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.window import Window

class DataValidation:

    def __init__(self,df):
        self.df = df

    def deduplicate(self,idCol,cdcCol):
        dedup = Window.partitionBy(idCol).orderBy(col(cdcCol).desc())
        df = self.df.withColumn("dedup",row_number().over(dedup))
        df=df.filter(col("dedup")==1).drop(col("dedup"))
        return df
    
    def removeNulls(self,nullCol):
        df = self.df.filter(col(nullCol).isNotNull())
        return df

# COMMAND ----------

from pyspark.sql.types import *

# COMMAND ----------

data = [
    (1,5000,"10-07-2025"),(2,6000,"15-07-2025"),(3,8000,"13-07-2025"),(1,9000,"12-07-2025"),(4,5000,None)
]

d_schema = StructType([
    StructField("id",IntegerType()),
    StructField("amount",DoubleType()),
    StructField("process_date",StringType())
])

df = spark.createDataFrame(data,d_schema).withColumn("process_date",to_date(col("process_date"),"dd-mm-yyyy"))

display(df)

# COMMAND ----------

dv = DataValidation(df)

df = dv.deduplicate("id","process_date")
df = dv.removeNulls("process_date")
display(df)
