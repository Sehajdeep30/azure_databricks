from config import *
from pyspark.sql.functions import *
from pyspark.sql.types import *

@dlt.table(
    name = "circuits_bronze"
)

def circuits_bronze():
  new_schema = StructType(fields=[StructField("circuitId",IntegerType(),False),
                            StructField("circuitRef",StringType(),True),
                            StructField("name",StringType(),True),
                            StructField("location",StringType(),True),
                            StructField("country",StringType(),True),
                            StructField("lat",DoubleType(),True),
                            StructField("lng",DoubleType(),True),
                            StructField("alt",IntegerType(),True),
                            StructField("url",StringType(),True)])
  
  df = spark.read.table("vsarthicat.f1_bronze.circuits")
  return df

  dlt.create_auto_cdc_flow(
  target = "vsarthicat.f1_bronze.circuits_scd2",
  source = "circuits_bronze",
  keys = ["circuitId"],
  sequence_by = "<sequence-column>",
  stored_as_scd_type = 2
)