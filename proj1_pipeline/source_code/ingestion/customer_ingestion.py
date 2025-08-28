import dlt
loc = spark.conf.get("ingestion_loc")
@dlt.table(
    name = "customer_stg"
)

def customer_stg():
    print(loc)
