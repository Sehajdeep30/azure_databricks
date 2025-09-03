import dlt
from pyspark.sql.functions import when, current_timestamp,col,to_date,lit
from pyspark.sql.types import StructField,IntegerType,StringType,StructType, DateType,TimestampType

schema = StructType([StructField("product_id",StringType(), True),
        StructField("product_name", StringType(), True),
        StructField("category", StringType(), True),
        StructField("price", IntegerType(), True),
        StructField("__START_AT",TimestampType(),True),
        StructField("__END_AT",TimestampType(),True)])

dlt.create_streaming_table(
  name = "products_silver",
  comment = "table to store products silver data in scd2 form",
  table_properties={
    "stage":"silver",
    "owner":"vsarthi"
  },
  cluster_by_auto = True,
  schema=schema
)

valid_pages = {"valid_price": "price IS NOT NULL AND price > 0",
               "valid_product": "product_id IS NOT NULL",
               }

@dlt.view(
  name = "products_silver_view"
)
@dlt.expect_all_or_drop(valid_pages)

def products_silver_view():
  timestamp = current_timestamp()

  df = dlt.readStream("products_stg")\
    .withColumnRenamed("productId", "product_id")\
    .withColumnRenamed("productName", "product_name")\
    .withColumn("data_timestamp",timestamp)\
    .dropDuplicates()
    
  null_managed_df = df.withColumn("product_name",
      when(df.product_name.isNotNull(), df.product_name)\
      .otherwise("unkown"))\
    .withColumn("category",
      when(df.category.isNotNull(),df.category)\
      .otherwise("unkown"))
  
  return null_managed_df

dlt.create_auto_cdc_flow(
  target = "products_silver",
  source = "products_silver_view",
  keys = ["product_id"],
  sequence_by = col("data_timestamp"),
  except_column_list = ["data_timestamp"],
  stored_as_scd_type = 2
)

