import pandas as pd
from datetime import datetime
import os

# Paths
SILVER_PATH = "/home/lawre/ecom-lakehouse/data/silver"
GOLD_PATH = "/home/lawre/ecom-lakehouse/data/gold"

# Ensure gold directory exists
os.makedirs(GOLD_PATH, exist_ok=True)

# Find the latest silver file
files = sorted(
    [f for f in os.listdir(SILVER_PATH) if f.startswith("orders_silver_")],
    reverse=True
)
if not files:
    raise FileNotFoundError("No silver files found!")
latest_silver = os.path.join(SILVER_PATH, files[0])

# Load Silver data
df = pd.read_csv(latest_silver)
print(f"✅ Loaded Silver file: {latest_silver}")

# Aggregate sales by month and customer
agg = (
    df.groupby(["order_month", "customer"])
    .agg(total_spent=("total_amount", "sum"), orders=("order_id", "count"))
    .reset_index()
    .sort_values(["order_month", "total_spent"], ascending=[True, False])
)

# Save as Gold CSV
output_path = os.path.join(GOLD_PATH, f"orders_gold_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv")
agg.to_csv(output_path, index=False)

print("✅ Gold layer created successfully!")
print(f"📂 File saved to: {output_path}")
print("\n💡 Preview:")
print(agg.head())

