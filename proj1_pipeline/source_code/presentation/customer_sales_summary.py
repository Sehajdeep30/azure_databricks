import dlt
from pyspark.sql.functions import col,sum,current_timestamp

# dlt.create_streaming_table(
#   name = "customer_sales_summary",
#   comment = "table to store order items silver data in scd2 form",
#   table_properties={
#     "stage":"silver",
#     "owner":"vsarthi"
#   },
#   cluster_by_auto = True,
# )

@dlt.table(
    name = "customer_sales_summary_view"
)
def customer_sales_summary_view():
    timestamp = current_timestamp()

    customers_df = spark.read.table("customer_silver").filter("__END_AT IS NULL").select("customer_id","location")
    orders_df = dlt.read("order_silver").filter("__END_AT IS NULL").select("customer_id","order_id")
    order_items_df = spark.read.table("order_items_silver").filter("__END_AT IS NULL").select("order_id","quantity","price_per_unit")

    joined_df = customers_df.join(orders_df, customers_df.customer_id == orders_df.customer_id, "inner").join(order_items_df, orders_df.order_id == order_items_df.order_id, "inner").select(customers_df.customer_id,customers_df.location,order_items_df.quantity,order_items_df.price_per_unit)

    summary_df = joined_df.groupBy("customer_id","location")\
        .agg(sum("quantity").alias("total_items_ordered"),
             sum(col("quantity")*col("price_per_unit")).alias("total_amount_spent"))
    
    output_df = summary_df.withColumn("data_timestamp",timestamp)
        
    return output_df

# dlt.create_auto_cdc_flow(
#   target = "customer_sales_summary",
#   source = "customer_sales_summary_view",
#   keys = ["customer_id"],
#   sequence_by = col("data_timestamp"),
#   except_column_list = ["data_timestamp"],
#   stored_as_scd_type = 2
# )

