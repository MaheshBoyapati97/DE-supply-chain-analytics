import json
import random
import os
from datetime import datetime, timedelta

def generate_bulk_data():
    # 1. Create Suppliers
    suppliers = []
    for i in range(1, 51):
        suppliers.append({
            "supplier_id": f"SUP{i:03}",
            "supplier_name": f"Supplier {chr(65 + (i % 26))}{i}",
            "location": random.choice(["Chicago", "New York", "Austin", "Seattle", "Miami"]),
            "reliability_score": round(random.uniform(0.5, 1.0), 2)
        })

    # 2. Create Orders (1,000 rows with some duplicates)
    orders = []
    base_date = datetime(2026, 1, 1)
    for i in range(1, 1001):
        # Purposefully create a duplicate every 50th record
        order_id = f"ORD{i:04}"
        if i % 50 == 0:
            order_id = f"ORD{(i-1):04}" 
            
        orders.append({
            "order_id": order_id,
            "customer_id": f"CUST{random.randint(100, 999)}",
            "order_date": (base_date + timedelta(days=random.randint(0, 100))).strftime("%Y-%m-%d"),
            "total_amount": round(random.uniform(50.0, 5000.0), 2),
            "status": random.choice(["Shipped", "Processing", "Cancelled", None]) # Some Nulls
        })

    # 3. Create Inventory Snapshots
    inventory = []
    for i in range(1, 501):
        inventory.append({
            "product_id": f"PROD{random.randint(1, 100)}",
            "warehouse_location": random.choice(["Chicago", "New York", "Austin", "Seattle"]),
            "stock_level": random.randint(0, 500),
            "reorder_point": 50
        })

    # 4. Create Shipments
    shipments = []
    for i in range(1, 1001):
        shipments.append({
            "shipment_id": f"SHP{i:04}",
            "order_id": f"ORD{i:04}",
            "carrier": random.choice(["FedEx", "UPS", "DHL"]),
            "estimated_delivery": (base_date + timedelta(days=random.randint(5, 110))).strftime("%Y-%m-%d"),
            "actual_delivery": (base_date + timedelta(days=random.randint(5, 115))).strftime("%Y-%m-%d") if random.random() > 0.2 else None
        })

    return {"orders": orders, "suppliers": suppliers, "inventory": inventory, "shipments": shipments}

if __name__ == "__main__":
    # 1. Define the directory and file name
    output_dir = 'data_source'
    file_name = 'sample_supply_chain_data.json'
    
    # 2. CREATE THE FOLDER IF IT IS MISSING
    # exist_ok=True prevents an error if the folder already exists
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        print(f"Created directory: {output_dir}")

    file_path = os.path.join(output_dir, file_name)
    
    # 3. Generate and save the data
    print("Generating bulk supply chain data (1,000+ rows)...")
    data = generate_bulk_data()
    
    with open(file_path, 'w') as f:
        json.dump(data, f, indent=4)
        
    print(f"✅ Success! Data saved to: {file_path}")


# Save it
data = generate_bulk_data()
with open('data_source/sample_supply_chain_data.json', 'w') as f:
    json.dump(data, f, indent=4)

print("Generated 1,000+ rows of messy supply chain data!")