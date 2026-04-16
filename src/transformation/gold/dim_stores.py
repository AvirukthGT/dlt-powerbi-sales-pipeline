import dlt 
from pyspark.sql.functions import *

#Gold streaming view

@dlt.view(
    name='stores_gold_view'
)
def stores_gold_view():
    df=spark.readStream.table("stores_silver_view")
    return df

#creatiing fact table

dlt.create_streaming_table(name="dim_stores")

dlt.create_auto_cdc_flow(
    source='stores_gold_view',
    target='dim_stores',
    keys=['store_id'],
    sequence_by=col('processDate'),
    stored_as_scd_type=2
)
