import dlt
from pyspark.sql.functions import when, current_timestamp,col
from pyspark.sql.types import StructField,IntegerType,StringType,StructType, DateType,TimestampType

schema = StructType([StructField("customer_id",StringType(), False),
        StructField("signup_date", DateType(), True),
        StructField("device_type", StringType(), True),
        StructField("location", StringType(), True),
        StructField("__START_AT",TimestampType(),True),
        StructField("__END_AT",TimestampType(),True)])

dlt.create_streaming_table(
  name = "customer_silver",
  comment = "table to store customer silver data in scd2 form",
  table_properties={
    "stage":"silver",
    "owner":"vsarthi"
  },
  cluster_by_auto = True,
  schema=schema
)


@dlt.view(
  name = "customer_silver_view"
)
@dlt.expect_or_drop("valid_customer_id", "customer_id IS NOT NULL AND signup_date IS NOT NULL")
def silver_view():
  timestamp = current_timestamp()

  df = dlt.readStream("customer_stg")\
    .withColumnRenamed("customerId", "customer_id")\
    .withColumnRenamed("signupDate", "signup_date")\
    .withColumnRenamed("deviceType", "device_type")\
    .withColumn("data_timestamp",timestamp)\
    .dropDuplicates()

  null_managed_df = df.withColumn("device_type",
      when(df.device_type.isNotNull(), df.device_type)\
      .otherwise("unkown"))\
    .withColumn("location",
      when(df.location.isNotNull(),df.location)\
      .otherwise("unkown"))
  
  return null_managed_df

dlt.create_auto_cdc_flow(
  target = "customer_silver",
  source = "customer_silver_view",
  keys = ["customer_id"],
  sequence_by = col("data_timestamp"),
  except_column_list = ["data_timestamp"],
  stored_as_scd_type = 2
)

