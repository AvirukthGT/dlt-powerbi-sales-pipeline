import dlt
from pyspark.sql.functions import *

@dlt.table(
    name="obt_sales",
    comment="One-Big-Table denormalizing sales facts with customer, product, and store dimensions for BI."
)
def obt_sales():
    # Use dlt.read() to read the latest materialized state of the gold tables
    fact_sales = dlt.read("fact_sales")
    dim_customers = dlt.read("dim_customers")
    dim_products = dlt.read("dim_products")
    dim_stores = dlt.read("dim_stores")

    # Join the fact table with all dimensions
    obt_df = fact_sales.alias("f") \
        .join(dim_customers.alias("c"), col("f.customer_id") == col("c.customer_id"), "left") \
        .join(dim_products.alias("p"), col("f.product_id") == col("p.product_id"), "left") \
        .join(dim_stores.alias("s"), col("f.store_id") == col("s.store_id"), "left") \
        .select(
            # Sales Metrics
            col("f.sales_id"),
            col("f.date").alias("sales_date"),
            col("f.quantity"),
            col("f.total_amount"),
            col("f.discount"),
            col("f.pricePerSale"),
            
            # Customer Details
            col("c.customer_id"),
            col("c.name").alias("customer_name"),
            col("c.email"),
            col("c.domain").alias("customer_email_domain"),
            col("c.location").alias("customer_location"),
            
            # Product Details
            col("p.product_id"),
            col("p.product_name"),
            col("p.category").alias("product_category"),
            col("p.price").alias("product_base_price"),
            
            # Store Details
            col("s.store_id"),
            col("s.store_name"),
            col("s.region").alias("store_region")
        )
    
    return obt_df