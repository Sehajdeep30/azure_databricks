import dlt
from pyspark.sql.functions import col,sum,current_timestamp

@dlt.table(
    name = "products_sales_summary_view"
)
def products_sales_summary_view():
    timestamp = current_timestamp()

    products_df = spark.read.table("products_silver").filter("__END_AT IS NULL")

    order_items_df = spark.read.table("order_items_silver").filter("__END_AT IS NULL").select("product_id","quantity")

    joined_df = products_df.join(order_items_df, products_df.product_id == order_items_df.product_id, "inner").select(products_df.product_id,products_df.product_name,products_df.category,products_df.price,order_items_df.quantity)

    summary_df = joined_df.groupBy("product_id","product_name","category")\
        .agg(sum("quantity").alias("units_sold"),
             sum(col("quantity")*col("price")).alias("total_revenue"))
    
    output_df = summary_df.withColumn("data_timestamp",timestamp)
        
    return output_df



