import dlt 
from pyspark.sql.functions import *

#Gold streaming view

@dlt.view(
    name='customers_gold_view'
)
def customers_gold_view():
    df=spark.readStream.table("customers_silver_view")
    return df

#creatiing fact table

dlt.create_streaming_table(name="dim_customers")

dlt.create_auto_cdc_flow(
    source='customers_gold_view',
    target='dim_customers',
    keys=['customer_id'],
    sequence_by=col('processDate'),
    stored_as_scd_type=2,
    except_column_list=['processDate']
)
