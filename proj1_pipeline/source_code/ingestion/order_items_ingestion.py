import dlt
from pyspark.sql.types import StructField,IntegerType,StringType,StructType, DateType
loc = spark.conf.get("ingestion_loc")

@dlt.table(
    name = "order_items_stg"
)
def order_items_stg():
    schema = StructType([StructField("itemId",StringType(), False),
        StructField("orderId", StringType(),False ),
        StructField("productId", StringType(), False),
        StructField("quantity", IntegerType(), True),
        StructField("pricePerUnit", IntegerType(), True)]) 
    
    df = spark.readStream\
        .format("cloudFiles")\
        .option("cloudFiles.format", "csv")\
        .option("cloudFiles.schemaLocation", f"{loc}/order_items/")\
        .option("header", "true")\
        .schema(schema)\
        .load(f"{loc}/order_items/raw/")
    return df
