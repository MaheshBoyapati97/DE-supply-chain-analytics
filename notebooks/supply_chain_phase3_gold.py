# Databricks notebook source
from pyspark.sql.functions import col, avg, count, round, lit, coalesce, trim, lower

# 1. Load silver tables from catalog
df_orders = spark.table("silver.orders")
df_shipments = spark.table("silver.shipments")
df_inventory = spark.table("silver.inventory")

# 2. Join Shipments and Orders
# Aliasing product_id here to resolve ambiguity during downstream joins
df_orders_min = df_orders.select(
    col("order_id"), 
    col("product_id").alias("ord_prod_id") 
)

df_step1 = df_shipments.join(df_orders_min, "order_id", "inner")

# 3. Standardize inventory keys for lookup
inventory_lookup = df_inventory.select(
    trim(lower(col("product_id"))).alias("inv_join_id"), 
    col("warehouse_location")
).distinct()

# 4. Left join to enrich shipments with warehouse location
df_master = df_step1.join(
    inventory_lookup, 
    trim(lower(col("ord_prod_id"))) == inventory_lookup.inv_join_id, 
    "left"
)

# 5. Defaulting unmatched locations to central hub
df_master = df_master.withColumn(
    "warehouse_location", 
    coalesce(col("warehouse_location"), lit("Central_Distribution_Hub"))
)

# 6. Aggregate KPIs for Carrier and Warehouse performance
carrier_perf = df_master.groupBy("carrier").agg(
    count("order_id").alias("total_shipments"),
    round(avg("delivery_delay"), 2).alias("avg_delay_days")
)

wh_efficiency = df_master.groupBy("warehouse_location").agg(
    count("warehouse_location").alias("order_volume"),
    round(avg("warehouse_lead_time"), 2).alias("avg_processing_days")
)

# 7. Write to Gold Layer
spark.sql("CREATE SCHEMA IF NOT EXISTS gold")
carrier_perf.write.format("delta").mode("overwrite").saveAsTable("gold.fact_carrier_performance")
wh_efficiency.write.format("delta").mode("overwrite").saveAsTable("gold.fact_warehouse_efficiency")

print("Gold layer successfully processed and saved.")

# COMMAND ----------

# Create a predictive risk score based on the new warehouse efficiency data
df_wh = spark.table("gold.fact_warehouse_efficiency")
df_carrier = spark.table("gold.fact_carrier_performance")

# Combine datasets to create a feature set for risk analysis
ai_feature_set = df_wh.join(df_carrier, lit(1)==lit(1), "inner")

# Risk Model: Logic correlates warehouse processing times with carrier delays
ai_risk_model = ai_feature_set.withColumn(
    "ai_risk_score",
    when((col("avg_processing_days") > 2.2) & (col("avg_delay_days") > 1.0), "CRITICAL: High Risk")
    .when((col("avg_processing_days") > 2.0) | (col("avg_delay_days") > 0.8), "WARNING: Potential Delay")
    .otherwise("OPTIMAL: On-Track")
)

ai_risk_model.write.format("delta").mode("overwrite").saveAsTable("gold.fact_ai_supply_chain_insights")

print("Predictive risk model updated.")

# COMMAND ----------

# Final validation of the Gold Insights table
# This proves the join worked and the AI score is populated
display(spark.table("gold.fact_ai_supply_chain_insights")
        .select("warehouse_location", "carrier", "avg_delay_days", "ai_risk_score")
        .limit(10))

# COMMAND ----------

# Check how many orders actually matched a location
matched = df_master.filter(col("warehouse_location") != "Central_Distribution_Hub").count()
total = df_master.count()
print(f"Matched: {matched} out of {total}")