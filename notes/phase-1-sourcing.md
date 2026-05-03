# Phase 1: Data Sourcing Map

This document outlines the schema of the raw JSON files generated for the Supply Chain Analytics project.

| Source File | Key Fields | Data Type | Description |
| :--- | :--- | :--- | :--- |
| **orders_raw.json** | `order_id` | Integer | Unique identifier for the order |
| | `product_id` | Integer | Links to the inventory/product |
| | `quantity` | Integer | Number of units ordered |
| | `order_date` | Timestamp | When the order was placed |
| **inventory_raw.json** | `warehouse_id` | String | Location identifier |
| | `product_id` | Integer | Links to the product |
| | `stock_level` | Integer | Current units in stock |
| **shipments_raw.json** | `shipment_id` | String | Unique tracking number |
| | `order_id` | Integer | Links back to the order |
| | `status` | String | Pending, Shipped, Delivered |
| **supplier_master_raw.json** | `supplier_id` | Integer | Unique supplier ID |
| | `supplier_name` | String | Name of the vendor |
| | `region` | String | Supplier location |