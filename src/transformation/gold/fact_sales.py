import dlt 
from pyspark.sql.functions import *

#Gold streaming view

@dlt.view(
    name='sales_gold_view'
)
def sales_gold_view():
    df=spark.readStream.table("sales_silver_view")
    return df

#creatiing fact table

dlt.create_streaming_table(name="fact_sales")

dlt.create_auto_cdc_flow(
    source='sales_gold_view',
    target='fact_sales',
    keys=['sales_id'],
    sequence_by=col('processDate'),
    stored_as_scd_type=1
)
