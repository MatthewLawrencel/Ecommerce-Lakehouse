import pandas as pd
from datetime import datetime
import os

# Paths
BRONZE_PATH = "/home/lawre/ecom-lakehouse/data/bronze/orders_bronze.csv"
SILVER_PATH = "/home/lawre/ecom-lakehouse/data/silver"

# Ensure silver directory exists
os.makedirs(SILVER_PATH, exist_ok=True)

# Load raw data
df = pd.read_csv(BRONZE_PATH)

print("✅ Bronze data loaded:")
print(df.head(), "\n")

# ✅ Rename columns to standard names
df = df.rename(columns={
    "customer_name": "customer",
    "amount": "total_amount"
})

# ✅ Clean data
df["order_date"] = pd.to_datetime(df["order_date"], errors="coerce")
df = df.dropna(subset=["customer", "total_amount"])

# ✅ Convert data types
df["total_amount"] = df["total_amount"].astype(float)
df["order_id"] = df["order_id"].astype(int)

# ✅ Add derived column
df["order_month"] = df["order_date"].dt.to_period("M").astype(str)

# ✅ Save as silver CSV
output_path = os.path.join(SILVER_PATH, f"orders_silver_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv")
df.to_csv(output_path, index=False)

print(f"✅ Silver data saved to: {output_path}")

