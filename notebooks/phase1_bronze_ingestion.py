# Databricks notebook source
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, FloatType
from pyspark.sql.functions import current_timestamp, lit
import json
from datetime import datetime
from typing import Optional
  
print("PySpark libraries and Python utilities loaded successfully.")

# COMMAND ----------

# 1. Orders Schema
orders_schema = StructType([
    StructField("order_id", StringType(), False),
    StructField("customer_id", StringType(), True),
    StructField("order_date", TimestampType(), True),
    StructField("product_id", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("total_amount", FloatType(), True)
])

# 2. Inventory Schema
inventory_schema = StructType([
    StructField("product_id", StringType(), False),
    StructField("stock_level", IntegerType(), True),
    StructField("warehouse_location", StringType(), True),
    StructField("last_updated", TimestampType(), True)
])

# 3. Shipments Schema
shipments_schema = StructType([
    StructField("shipment_id", StringType(), False),
    StructField("order_id", StringType(), True),
    StructField("carrier", StringType(), True),
    StructField("status", StringType(), True),
    StructField("shipment_date", TimestampType(), True)
])

# 4. Suppliers Schema
suppliers_schema = StructType([
    StructField("supplier_id", StringType(), False),
    StructField("supplier_name", StringType(), True),
    StructField("contact_email", StringType(), True),
    StructField("lead_time_days", IntegerType(), True)
])

print("Schemas defined for Orders, Inventory, Shipments, and Suppliers.")

# COMMAND ----------

# Quick check of the Orders schema structure
print("Orders Schema Structure:")
print(orders_schema.treeString())

# COMMAND ----------

# Create sample Orders data as JSON string (simulates source file)
orders_sample_data = '''
[
  {"order_id": "ORD001", "customer_id": "CUST101", "order_date": "2026-05-01 10:00:00", "product_id": "PROD001", "quantity": 5, "total_amount": 250.0},
  {"order_id": "ORD002", "customer_id": "CUST102", "order_date": "2026-05-02 14:30:00", "product_id": "PROD002", "quantity": 3, "total_amount": 150.0}
]
'''

print("Sample orders data ready (2 rows). Length:", len(orders_sample_data))

# COMMAND ----------

# Initialize Bronze layer with documentation for Unity Catalog
spark.sql("""
    CREATE SCHEMA IF NOT EXISTS bronze 
    COMMENT 'Raw Ingestion: Immutable source data with audit metadata for lineage tracking.'
""")

# COMMAND ----------


# Sample Inventory data
inventory_sample_data = '''
[
  {"product_id": "PROD001", "stock_level": 100, "warehouse_location": "WH_NAPERVILLE", "last_updated": "2026-05-04 08:00:00"},
  {"product_id": "PROD002", "stock_level": 25, "warehouse_location": "WH_CHICAGO", "last_updated": "2026-05-04 09:15:00"}
]
'''


# COMMAND ----------

# Shipments Sample
shipments_sample_data = '''
[
  {"shipment_id": "SHP_9901", "order_id": "ORD001", "carrier": "FedEx", "status": "In Transit", "shipment_date": "2026-05-04 12:00:00"},
  {"shipment_id": "SHP_9902", "order_id": "ORD002", "carrier": "UPS", "status": "Delivered", "shipment_date": "2026-05-04 15:30:00"}
]
'''

# Suppliers Sample
suppliers_sample_data = '''
[
  {"supplier_id": "SUP_001", "supplier_name": "Global Tech Parts", "contact_email": "sales@globaltech.com", "lead_time_days": 5},
  {"supplier_id": "SUP_002", "supplier_name": "Eco-Logistics", "contact_email": "info@ecolog.com", "lead_time_days": 12}
]
'''
print("Shipments and Suppliers samples ready.")

# COMMAND ----------

from typing import Optional

def ingest_to_bronze(json_str: str, schema: StructType, table_name: str, date_col: Optional[str] = None) -> None:
    """
    Standardized framework for Bronze layer ingestion.
    Enforces schema, enriches with audit metadata, and ensures Delta Lake optimization.
    """
    try:
        # 1. Parse JSON and handle Date Serialization (Senior practice: Explicit over Implicit)
        data = json.loads(json_str)
        if date_col:
            for row in data:
                if row.get(date_col):
                    row[date_col] = datetime.strptime(row[date_col], '%Y-%m-%d %H:%M:%S')
        
        # 2. Create DataFrame with Audit Metadata (Essential for 6+ years exp traceability)
        df = spark.createDataFrame(data, schema=schema) \
            .withColumn("_ingest_time", current_timestamp()) \
            .withColumn("_source_origin", lit(f"sample_{table_name}.json"))
        
        # 3. Save as Delta Table with Idempotent write mode 
        # Added .option("overwriteSchema", "true") to allow for metadata field evolution
        df.write.format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .saveAsTable(f"bronze.{table_name}")
        
        print(f"✅ [SUCCESS] Table 'bronze.{table_name}' synchronized with Schema Overwrite. Count: {df.count()}")
        
    except Exception as e:
        print(f"❌ [FAILURE] Ingestion failed for {table_name}: {str(e)}")
        raise e

# --- Production Orchestration ---
# Standardizing all calls in one place shows senior-level coordination
ingest_to_bronze(orders_sample_data, orders_schema, "orders", "order_date")
ingest_to_bronze(inventory_sample_data, inventory_schema, "inventory", "last_updated")
ingest_to_bronze(shipments_sample_data, shipments_schema, "shipments", "shipment_date")
ingest_to_bronze(suppliers_sample_data, suppliers_schema, "suppliers")

# COMMAND ----------

def run_dq_check(table_name):
    """
    Validation Suite: Check for nulls and record counts across ingested domains.
    """
    df = spark.table(f"bronze.{table_name}")
    null_counts = {c: df.filter(df[c].isNull()).count() for c in df.columns}
    
    print(f"--- DQ Report: {table_name.upper()} ---")
    print(f"Total Rows: {df.count()}")
    for col, count in null_counts.items():
        status = "OK" if count == 0 else f"FAIL ({count} nulls)"
        print(f"  - {col}: {status}")

# Validate all domains
for table in ["orders", "inventory", "shipments", "suppliers"]:
    run_dq_check(table)

# COMMAND ----------

# Optimize Delta tables for read performance (compacts small files)
for table in ["orders", "inventory", "shipments", "suppliers"]:
    spark.sql(f"OPTIMIZE bronze.{table}")

# Final inventory check
display(spark.sql("SHOW TABLES IN bronze"))

# COMMAND ----------

