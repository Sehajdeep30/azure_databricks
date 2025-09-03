import dlt
from pyspark.sql.types import StructField,IntegerType,StringType,StructType, DateType
loc = spark.conf.get("ingestion_loc")

@dlt.table(
    name = "customer_stg"
)
def customer_stg():
    schema = StructType([StructField("customerId",StringType(), False),
        StructField("signupDate", DateType(), True),
        StructField("deviceType", StringType(), True),
        StructField("location", StringType(), True)]) 
    
    df = spark.readStream\
        .format("cloudFiles")\
        .option("cloudFiles.format", "csv")\
        .option("cloudFiles.schemaLocation", f"{loc}/customers/")\
        .option("header", "true")\
        .schema(schema)\
        .load(f"{loc}/customers/raw/")
    return df
