import dlt
from pyspark.sql.functions import when, current_timestamp,col
from pyspark.sql.types import StructField,IntegerType,StringType,StructType, DateType,TimestampType

schema = StructType([StructField("item_id",StringType(), False),
        StructField("order_id", StringType(),False ),
        StructField("product_id", StringType(), False),
        StructField("quantity", IntegerType(), True),
        StructField("price_per_unit", IntegerType(), True),
        StructField("__START_AT",TimestampType(),True),
        StructField("__END_AT",TimestampType(),True)])

dlt.create_streaming_table(
  name = "order_items_silver",
  comment = "table to store order items silver data in scd2 form",
  table_properties={
    "stage":"silver",
    "owner":"vsarthi"
  },
  cluster_by_auto = True,
  schema=schema
)

valid_pages = {"valid_ppu": "price_per_unit IS NOT NULL",
               "valid_order": "order_id IS NOT NULL AND product_id IS NOT NULL",
               "valid_quantity": "quantity IS NOT NULL AND quantity>0",
               }

@dlt.view(
  name = "order_items_silver_view"
)
@dlt.expect_all_or_drop(valid_pages)

def order_items_silver_view():
  
  timestamp = current_timestamp()
  
  df = dlt.readStream("order_items_stg")\
    .withColumnRenamed("itemId", "item_id")\
    .withColumnRenamed("orderId", "order_id")\
    .withColumnRenamed("productId", "product_id")\
    .withColumnRenamed("pricePerUnit", "price_per_unit")\
    .withColumn("data_timestamp",timestamp)\
    .dropDuplicates()

  return df

dlt.create_auto_cdc_flow(
  target = "order_items_silver",
  source = "order_items_silver_view",
  keys = ["item_id"],
  sequence_by = col("data_timestamp"),
  except_column_list = ["data_timestamp"],
  stored_as_scd_type = 2
)

