# DE-supply-chain-analytics
End-to-end Data Engineering project: Real-time supply chain intelligence using Azure Databricks, Unity Catalog, and dbt.

# Real-Time Supply Chain Intelligence Platform

## Project Overview
This project builds an end-to-end cloud data pipeline to improve supply chain visibility and operational efficiency. Using a Medallion Architecture (Bronze, Silver, Gold), the platform processes orders, inventory, shipment, and supplier data to identify delays, monitor stock levels, and evaluate vendor performance.

## Objective
The goal of this project is to simulate a modern data engineering solution for a real-time supply chain use case. The platform is designed to support raw ingestion, data transformation, governance, and AI-powered insights.

## Business Problem
Supply chain teams often lack timely visibility into delayed shipments, low inventory, and supplier performance. This project aims to consolidate source data into a governed analytics platform so these issues can be monitored and analyzed more effectively.

## Architecture Overview
The project follows a Medallion Architecture pattern:
- **Bronze**: Raw ingestion of source data with minimal transformation.
- **Silver**: Cleaning, standardization, and enrichment of source data.
- **Gold**: Aggregated business-ready tables for reporting and analytics.

---

## Phase 1: Data Sourcing & Bronze Ingestion
Before building the pipeline, we identified the key data sources required for supply chain visibility. This phase focuses on defining the origin, frequency, and purpose of the data.

### Data Domains & Strategy
| Source | Data Type | Refresh Frequency | Purpose |
| :--- | :--- | :--- | :--- |
| **Orders** | Transactional | Daily Batch | Tracks customer demand and fulfillment lifecycle. |
| **Inventory** | Snapshot | Hourly | Monitors stock levels across warehouses to help prevent stockouts. |
| **Shipments** | Event | Near Real-Time | Tracks shipment status updates for delay analysis. |
| **Suppliers** | Master/Reference | Weekly | Provides vendor details for supplier performance analysis. |

---

### Bronze Ingestion Completed

### Key Updates:
* **Dynamic Data Generation:** Developed a custom Python-based generator (`generate_data.py`) to produce **1,000+ synthetic supply chain records**, moving away from static mock data.
* **Intentional Data Quality Stress-Testing:** Engineered the source dataset with purposeful **duplicate records** and **null values** (e.g., ~27% null rate in Order Status) to rigorously validate downstream cleaning logic.
* **Scalable Ingestion Pattern:** Implemented a robust PySpark "Explode" logic that dynamically flattens nested JSON structures into governed Delta Lake tables.
* **Automated DQ Reporting:** Integrated a validation suite that provides a real-time Data Quality (DQ) report for every ingestion run, tracking row counts and schema integrity.
* **Infrastructure:** Configured a **Managed Volume** in Databricks Unity Catalog to serve as the landing zone for raw `.json` files.

### Technical Implementation Detail
- **Source Path:** `/Volumes/dbw_supply_chain_analytics/default/raw_data/sample_supply_chain_data.json`
- **Tables Created:** `bronze.orders`, `bronze.inventory`, `bronze.shipments`, `bronze.suppliers`
- **Audit Metadata:** Enriched all tables with `_ingest_time` (timestamp) and `_source_origin` (filename) for full lineage traceability.

### Implementation Proof
The following screenshots document the successful end-to-end execution of Phase 1, from initial ingestion to data governance and quality validation:

**1. Ingestion Success Log**
*Visual confirmation of 1,000 rows processed successfully into the Databricks environment.*
![Ingestion Success](docs/screenshots/phase1_bronze_ingestion_success.png)

**2. Bronze Catalog Structure**
*Proves governance and organization of the four supply chain tables within the Unity Catalog schema.*
![Unity Catalog Structure](docs/screenshots/unity_catalog_bronze_schema.png)

**3. Data Quality Report**
*Shows the pipeline detecting intentional null values in the raw dataset for downstream resolution.*
![DQ Report](docs/screenshots/phase1_dq_report_null_checks.png)

**4. Audit Metadata Verification**
*Demonstrates traceability with ingestion timestamps and source file tracking for every record.*
![Audit Metadata Proof](docs/screenshots/bronze_orders_audit_metadata.png)

### Engineering Challenges & Resolutions
- **Scalable Ingestion Strategy**: Developed a robust pipeline designed to process bulk JSON data (1,000+ records) from a Databricks Managed Volume. The system was engineered to handle complex nested structures and scale beyond simple mock data.
- **Intentional Quality Stress-Testing**: Purposefully incorporated data quality issues (nulls and duplicates) within the source dataset to rigorously validate the error-handling and deduplication logic of downstream transformations.
- **Production-Grade Auditability**: Implemented system-level metadata columns (`_ingest_time` and `_source_origin`) to ensure every record provides full lineage traceability for troubleshooting and compliance.

### Sample Bronze Queries
```sql
-- Quick check of ingestion success by source file
SELECT _source_origin, COUNT(*) 
FROM bronze.orders 
GROUP BY 1;

-- Customer spend analysis (identifying data ready for Silver transformation)
SELECT customer_id, SUM(total_amount) AS total_spent
FROM bronze.orders
GROUP BY customer_id;

-- Inventory health check across warehouses
SELECT warehouse_location, AVG(stock_level) AS avg_stock
FROM bronze.inventory
GROUP BY warehouse_location;

---

### Tools and Technologies
- **Azure Databricks**: Primary compute and notebook environment for scalable data processing.
- **Delta Lake**: Storage layer providing ACID transactions and high-performance metadata handling.
- **Unity Catalog**: Centralized governance and fine-grained access control for all data assets.
- **GitHub**: Version control, collaboration, and CI/CD integration.
- **VS Code**: Local development environment for Python scripting and data generation.
- **PySpark (Python)**: Core distributed processing engine for ingestion and complex transformations.
- **SQL**: Used for ad-hoc data analysis, validation, and warehouse-style querying.
- **dbt Core**: Planned for later phases to manage modular SQL transformations and documentation.

## Project Status
- [x] **Environment Setup**: Databricks workspace and Unity Catalog configuration.
- [x] **Project Planning**: Data domain identification and sourcing strategy.
- [x] **Phase 1: Bronze Ingestion**: Bulk data ingestion with 1,000+ records and audit metadata.
- [ ] **Phase 2: Silver Transformation**: Data deduplication, cleaning, and standardization.
- [ ] **Phase 3: Gold Aggregation**: Business-level reporting tables and KPIs.
- [ ] **Phase 4: Governance**: Implementing Row-Level Security (RLS) and Column-Level Security (CLS).
- [ ] **Phase 5: AI Integration**: Real-time insights and RAG-based analysis.

## Author
Mahesh Boyapati