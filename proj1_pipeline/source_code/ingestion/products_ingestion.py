import dlt
from pyspark.sql.types import StructField,IntegerType,StringType,StructType, DateType
loc = spark.conf.get("ingestion_loc")

@dlt.table(
    name = "products_stg"
)
def customer_stg():
    schema = StructType([StructField("productId",StringType(), False),
        StructField("productName", StringType(), True),
        StructField("category", StringType(), True),
        StructField("price", IntegerType(), True)]) 
    
    df = spark.readStream\
        .format("cloudFiles")\
        .option("cloudFiles.format", "csv")\
        .option("cloudFiles.schemaLocation", f"{loc}/products/")\
        .option("header", "true")\
        .schema(schema)\
        .load(f"{loc}/products/raw/")
    return df
