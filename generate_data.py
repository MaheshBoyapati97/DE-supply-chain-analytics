import json
import random
import os
from datetime import datetime, timedelta

def generate_bulk_data():
    # 1. Create Suppliers (50 suppliers)
    suppliers = []
    for i in range(1, 51):
        suppliers.append({
            "supplier_id": f"SUP{i:03}",
            "supplier_name": f"Supplier {chr(65 + (i % 26))}{i}",
            "location": random.choice(["Chicago", "New York", "Austin", "Seattle", "Miami"]),
            "reliability_score": round(random.uniform(0.5, 1.0), 2)
        })

    # 2. Create Orders (1,000 rows with intentional duplicates and nulls)
    orders = []
    base_date = datetime(2026, 1, 1)
    for i in range(1, 1001):
        # Purposefully create a duplicate ID every 50th record
        order_id = f"ORD{i:04}"
        if i % 50 == 0:
            order_id = f"ORD{(i-1):04}" 
            
        orders.append({
            "order_id": order_id,
            "customer_id": f"CUST{random.randint(100, 999)}",
            # FIXED: Using 3-digit padding to match Inventory (PROD001 vs PROD001)
            "product_id": f"PROD{random.randint(1, 100):03}", 
            "order_date": (base_date + timedelta(days=random.randint(0, 30))).strftime("%Y-%m-%d"),
            "total_amount": round(random.uniform(50.0, 5000.0), 2),
            "status": random.choice(["Shipped", "Processing", "Cancelled", None]) # Intentional Nulls
        })

    # 3. Create Inventory Snapshots (100 Unique Products across 10 Cities)
    inventory = []
    locations = [
        "New York", "Los Angeles", "Chicago", "Houston", "Phoenix", 
        "Philadelphia", "San Antonio", "San Diego", "Dallas", "San Jose"
    ]
    
    for i in range(1, 101):
        inventory.append({
            "product_id": f"PROD{i:03}",
            # FIXED: Cycle through 10 cities to ensure even distribution in Dashboard
            "warehouse_location": locations[i % len(locations)], 
            "stock_level": random.randint(0, 500),
            "reorder_point": 50
        })

    # 4. Create Shipments (Standardized Names)
    shipments = []
    for i in range(0, 1000):
        # Fetch the corresponding order date to ensure logical shipping dates
        ord_date_str = orders[i]["order_date"]
        ord_date = datetime.strptime(ord_date_str, "%Y-%m-%d")
        
        # Warehouse Processing: 1-3 days after order
        ship_date = ord_date + timedelta(days=random.randint(1, 3))
        
        # Carrier Transit: 3-7 days after shipping
        est_delivery = ship_date + timedelta(days=random.randint(3, 7))
        
        # Actual Delivery (80% delivered, 20% in-transit/None)
        act_delivery = None
        if random.random() > 0.2:
            # Can be 2 days early to 3 days late
            act_delivery = est_delivery + timedelta(days=random.randint(-2, 3))

        shipments.append({
            "shipment_id": f"SHP{i+1:04}",
            "order_id": orders[i]["order_id"],
            "carrier": random.choice(["FedEx", "UPS", "DHL"]),
            "shipment_date": ship_date.strftime("%Y-%m-%d"),
            "estimated_delivery_date": est_delivery.strftime("%Y-%m-%d"),
            "actual_delivery_date": act_delivery.strftime("%Y-%m-%d") if act_delivery else None
        })

    return {
        "orders": orders, 
        "suppliers": suppliers, 
        "inventory": inventory, 
        "shipments": shipments
    }

if __name__ == "__main__":
    output_dir = 'data_source'
    file_name = 'sample_supply_chain_data.json'
    
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    file_path = os.path.join(output_dir, file_name)
    
    print("Generating refined supply chain data with 10 warehouse locations...")
    data = generate_bulk_data()
    
    with open(file_path, 'w') as f:
        json.dump(data, f, indent=4)
        
    print(f"✅ Success! Data saved to: {file_path}")