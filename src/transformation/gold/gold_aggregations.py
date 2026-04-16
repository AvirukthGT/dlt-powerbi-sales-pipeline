import dlt
from pyspark.sql.functions import *

# 1. Executive View: Sales by Region and Category
@dlt.table(
    name="gold_sales_by_region_category",
    comment="Aggregated sales performance segmented by store region and product category."
)
def gold_sales_by_region_category():
    return (
        dlt.read("obt_sales")
        .groupBy("store_region", "product_category")
        .agg(
            round(sum("total_amount"), 2).alias("total_revenue"),
            sum("quantity").alias("total_units_sold"),
            count("sales_id").alias("total_transactions")
        )
        .orderBy(desc("total_revenue"))
    )

# 2. Marketing View: Customer Spend Summary (RFM Base)
@dlt.table(
    name="gold_customer_spend_summary",
    comment="Summary of lifetime spend and transaction counts per customer."
)
def gold_customer_spend_summary():
    return (
        dlt.read("obt_sales")
        .groupBy("customer_id", "customer_name", "customer_location")
        .agg(
            round(sum("total_amount"), 2).alias("lifetime_spend"),
            count("sales_id").alias("total_purchases"),
            max("sales_date").alias("last_purchase_date")
        )
        .orderBy(desc("lifetime_spend"))
    )

# 3. Operations View: Daily Sales Trend
@dlt.table(
    name="gold_daily_sales_trend",
    comment="Daily aggregation of sales revenue and volume."
)
def gold_daily_sales_trend():
    return (
        dlt.read("obt_sales")
        .groupBy("sales_date")
        .agg(
            round(sum("total_amount"), 2).alias("daily_revenue"),
            sum("quantity").alias("daily_units_sold")
        )
        .orderBy(desc("sales_date"))
    )