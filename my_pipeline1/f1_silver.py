from config import *
from pyspark.sql.functions import *

@dlt.table(
    name = "circuits_silver"
)
def circuits_silver():
    raw_df = dlt.read("circuits_bronze")

    trimmed_df = raw_df.drop("url")

    renamed_df = trimmed_df.withColumnRenamed("circuitId","circuit_id").withColumnRenamed("circuitRef","circuit_ref").withColumnRenamed("lat","latitude").withColumnRenamed("lng","longitude").withColumnRenamed("alt","altitude")

    audited_df = renamed_df.withColumn("ingestion_time",current_timestamp())

    return audited_df
    
    
