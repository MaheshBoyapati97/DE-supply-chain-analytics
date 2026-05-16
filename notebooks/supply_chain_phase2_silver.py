# Databricks notebook source
from pyspark.sql.functions import col, desc, row_number, datediff, when, lit
from pyspark.sql.window import Window

# Setup silver layer
silver_schema = "silver"
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {silver_schema}")

# Target domain tables
tables = ["orders", "inventory", "shipments", "suppliers"]

# COMMAND ----------

# 1. Load and clean orders
df_orders = spark.table("bronze.orders")

# Fill missing status with UNKNOWN to avoid breaking downstream aggregations
df_orders_cleaned = df_orders.fillna({"status": "UNKNOWN"})

# 2. Deduplication: Keep the most recent record per Order ID
window_spec = Window.partitionBy("order_id").orderBy(desc("_ingest_time"))

df_orders_final = df_orders_cleaned.withColumn("row_num", row_number().over(window_spec)) \
    .filter(col("row_num") == 1) \
    .drop("row_num")

# 3. Persist to Silver
df_orders_final.write.format("delta").mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("silver.orders")

print(f"Silver orders table loaded: {df_orders_final.count()} rows")

# COMMAND ----------

# --- 1. Shipments Enrichment (Keep as is) ---
df_ship_raw = spark.table("bronze.shipments")
ship_win = Window.partitionBy("shipment_id").orderBy(desc("_ingest_time"))
df_ship_dedup = df_ship_raw.withColumn("rn", row_number().over(ship_win)).filter("rn = 1").drop("rn")

df_orders_ref = spark.table("bronze.orders").select("order_id", "order_date")

df_ship_final = df_ship_dedup.join(df_orders_ref, "order_id", "inner") \
    .withColumn("warehouse_lead_time", datediff(col("shipment_date"), col("order_date"))) \
    .withColumn("delivery_delay", datediff(col("actual_delivery_date"), col("estimated_delivery_date")))

df_ship_final.write.format("delta").mode("overwrite").saveAsTable("silver.shipments")

# --- 2. Inventory Synchronization (THE FIX) ---
# Instead of filtering inventory by active orders (which is causing the 0 match),
# we keep all inventory records but fill in missing default values.
df_inv_raw = spark.table("bronze.inventory")

df_inv_final = df_inv_raw.fillna({
    "warehouse_location": "Global_Distribution_Center", 
    "stock_level": 0
})

# Ensure we have one unique record per product ID to prevent join explosions
inv_win = Window.partitionBy("product_id").orderBy(desc("_ingest_time"))
df_inv_dedup = df_inv_final.withColumn("rn", row_number().over(inv_win)).filter("rn = 1").drop("rn")

df_inv_dedup.write.format("delta").mode("overwrite").saveAsTable("silver.inventory")

# --- 3. Suppliers ---
spark.table("bronze.suppliers").write.format("delta").mode("overwrite").saveAsTable("silver.suppliers")

print("Silver layer enrichment complete. Inventory preserved.")