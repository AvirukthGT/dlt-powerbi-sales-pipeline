import dlt 
from pyspark.sql.functions import *

#Gold streaming view

@dlt.view(
    name='products_gold_view'
)
def products_gold_view():
    df=spark.readStream.table("products_silver_view")
    return df

#creatiing fact table

dlt.create_streaming_table(name="dim_products")

dlt.create_auto_cdc_flow(
    source='products_gold_view',
    target='dim_products',
    keys=['product_id'],
    sequence_by=col('processDate'),
    stored_as_scd_type=1
)
