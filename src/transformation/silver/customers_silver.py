import dlt
from pyspark.sql.functions import *

#Streaming view
@dlt.view(
    name="customers_silver_view"
)
def  customers_silver_view():
    df = spark.readStream.table("customers_bronze")
    df = df.withColumn("name",upper(col("name")))
    df = df.withColumn("domain",split(col("email"),"@")[1])
    df = df.withColumn("processDate",current_timestamp())
    return df

#customers Silver Table(with upsert)

dlt.create_streaming_table(
    name='customers_silver'
)

dlt.create_auto_cdc_flow(
    target='customers_silver',
    source='customers_silver_view',
    keys=['customer_id'],
    sequence_by= col('processDate'),
    stored_as_scd_type=1
)
