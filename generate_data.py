import json
import random
from datetime import datetime
import os

# Create a 'raw_data' folder to act as our "Loading Dock"
os.makedirs('raw_data', exist_ok=True)

def generate_truck_data():
    truck_ids = [f"TRUCK_{i}" for i in range(101, 106)]
    locations = ["Chicago", "New York", "Los Angeles", "Houston", "Phoenix"]
    
    data = {
        "timestamp": datetime.now().isoformat(),
        "truck_id": random.choice(truck_ids),
        "last_location": random.choice(locations),
        "fuel_level": round(random.uniform(10.5, 99.9), 2),
        "temperature_celsius": round(random.uniform(-5.0, 25.0), 2),
        "load_weight_kg": random.randint(5000, 20000)
    }
    
    # Save as a JSON file with a unique timestamp name
    filename = f"raw_data/shipment_{datetime.now().strftime('%H%M%S')}.json"
    with open(filename, 'w') as f:
        json.dump(data, f)
    print(f"Generated: {filename}")

# Generate 5 "shipment" updates
for _ in range(5):
    generate_truck_data()