import dlt
from pyspark.sql.functions import when, current_timestamp,col,to_date,lit
from pyspark.sql.types import StructField,IntegerType,StringType,StructType, DateType,TimestampType

schema = StructType([StructField("order_id",StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("order_date", DateType(), True),
        StructField("order_status", StringType(), True),
        StructField("__START_AT",TimestampType(),True),
        StructField("__END_AT",TimestampType(),True)])

dlt.create_streaming_table(
  name = "order_silver",
  comment = "table to store orders silver data in scd2 form",
  table_properties={
    "stage":"silver",
    "owner":"vsarthi"
  },
  cluster_by_auto = True,
  schema=schema
)

valid_pages = {"valid_customer": "customer_id IS NOT NULL",
               "valid_order": "order_id IS NOT NULL",
               }

@dlt.view(
  name = "order_silver_view"
)
@dlt.expect_all_or_drop(valid_pages)

def order_items_silver_view():
  timestamp = current_timestamp()
  df = dlt.readStream("orders_stg")\
    .withColumnRenamed("orderId", "order_id")\
    .withColumnRenamed("customerId", "customer_id")\
    .withColumnRenamed("orderStatus", "order_status")\
    .withColumn("data_timestamp",timestamp)\
    .withColumn("order_date", to_date(col("orderDate")))\
    .drop("orderDate")\
    .dropDuplicates()
    
  null_managed_df = df.withColumn("order_date",
      when(df.order_date.isNotNull(), df.order_date)\
      .otherwise(lit("9999-12-31").cast("date")))\
    .withColumn("order_status",
      when(df.order_status.isNotNull(),df.order_status)\
      .otherwise("unkown"))
  
  return null_managed_df

dlt.create_auto_cdc_flow(
  target = "order_silver",
  source = "order_silver_view",
  keys = ["order_id"],
  sequence_by = col("data_timestamp"),
  except_column_list = ["data_timestamp"],
  stored_as_scd_type = 2
)

