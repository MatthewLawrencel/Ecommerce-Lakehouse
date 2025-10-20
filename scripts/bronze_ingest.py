import pandas as pd
import os
from datetime import datetime

# Paths
RAW_PATH = os.path.expanduser("~/ecom-lakehouse/data/raw/orders.csv")
BRONZE_PATH = os.path.expanduser("~/ecom-lakehouse/data/bronze/orders_bronze.csv")

def ingest_bronze():
    """Simulate raw data ingestion into Bronze layer."""
    os.makedirs(os.path.dirname(BRONZE_PATH), exist_ok=True)

    # Simulated raw data
    data = {
        "order_id": [1, 2, 3],
        "customer_name": ["Alice", "Bob", "Charlie"],
        "amount": [250.75, 100.50, 300.40],
        "order_date": ["2025-10-17", "2025-10-18", "2025-10-19"]
    }

    df = pd.DataFrame(data)
    df.to_csv(BRONZE_PATH, index=False)
    print(f"[{datetime.now()}] ✅ Bronze layer data saved to: {BRONZE_PATH}")

if __name__ == "__main__":
    ingest_bronze()

