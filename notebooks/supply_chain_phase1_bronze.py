# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import current_timestamp, lit, col, explode
  
print("Imports loaded.")

# COMMAND ----------

# Define schemas for raw JSON landing
orders_schema = StructType([
    StructField("order_id", StringType(), False),
    StructField("customer_id", StringType(), True),
    StructField("order_date", TimestampType(), True),
    StructField("product_id", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("total_amount", FloatType(), True)
])

inventory_schema = StructType([
    StructField("product_id", StringType(), False),
    StructField("stock_level", IntegerType(), True),
    StructField("warehouse_location", StringType(), True),
    StructField("last_updated", TimestampType(), True)
])

shipments_schema = StructType([
    StructField("shipment_id", StringType(), True),
    StructField("order_id", StringType(), True),
    StructField("carrier", StringType(), True),
    StructField("shipment_date", StringType(), True),         
    StructField("estimated_delivery_date", StringType(), True), 
    StructField("actual_delivery_date", StringType(), True)   
])

suppliers_schema = StructType([
    StructField("supplier_id", StringType(), False),
    StructField("supplier_name", StringType(), True),
    StructField("contact_email", StringType(), True),
    StructField("lead_time_days", IntegerType(), True)
])

# COMMAND ----------

# Create bronze database
spark.sql("CREATE SCHEMA IF NOT EXISTS bronze")

# COMMAND ----------

# Raw file path in Unity Catalog Volume
FILE_PATH = "/Volumes/dbw_supply_chain_analytics/default/raw_data/sample_supply_chain_data.json"

def ingest_to_bronze(domain):
    # Flatten nested JSON and add audit columns
    raw_df = spark.read.option("multiline", "true").json(FILE_PATH)
    
    df = raw_df.select(explode(col(domain)).alias("data")).select("data.*")
    df = df.withColumn("_ingest_time", current_timestamp()) \
           .withColumn("_source_origin", lit("sample_supply_chain_data.json"))
    
    # Overwrite bronze tables
    df.write.format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable(f"bronze.{domain}")
    
    print(f"Table bronze.{domain} loaded: {df.count()} rows")

# Run ingestion for all domains
for d in ["orders", "inventory", "shipments", "suppliers"]:
    ingest_to_bronze(d)

# COMMAND ----------

# Basic null check for monitoring
def check_nulls(table):
    df = spark.table(f"bronze.{table}")
    print(f"\n--- {table.upper()} NULL CHECK ---")
    for c in df.columns:
        null_count = df.filter(df[c].isNull()).count()
        if null_count > 0:
            print(f"Column {c}: {null_count} nulls found")
        else:
            print(f"Column {c}: OK")

for t in ["orders", "inventory", "shipments", "suppliers"]:
    check_nulls(t)

# COMMAND ----------

# Optimize tables to compact small files and improve read speed
for t in ["orders", "inventory", "shipments", "suppliers"]:
    spark.sql(f"OPTIMIZE bronze.{t}")

# COMMAND ----------

# Verify table creation in the bronze schema
display(spark.sql("SHOW TABLES IN bronze"))

# Check data with audit metadata
display(spark.table("bronze.orders")
        .select("order_id", "customer_id", "_ingest_time", "_source_origin")
        .limit(5))