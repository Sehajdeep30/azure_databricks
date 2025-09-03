import dlt
from pyspark.sql.types import StructField,IntegerType,StringType,StructType, DateType
loc = spark.conf.get("ingestion_loc")

@dlt.table(
    name = "orders_stg"
)
def orders_stg():
    schema = StructType([StructField("orderId",StringType(), False),
        StructField("customerId", StringType(), False),
        StructField("orderDate", DateType(), True),
        StructField("orderStatus", StringType(), True)])  
    
    df = spark.readStream\
        .format("cloudFiles")\
        .option("cloudFiles.format", "csv")\
        .option("cloudFiles.schemaLocation", f"{loc}/orders/")\
        .option("header", "true")\
        .schema(schema)\
        .load(f"{loc}/orders/raw/")
    return df
