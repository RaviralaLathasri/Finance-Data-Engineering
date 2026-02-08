import pandas as pd
import os

# Paths
input_csv = "../data/raw/nigerian_retail_transactions.csv"
output_dir = "../data/processed"
output_csv = os.path.join(output_dir, "transformed_transactions.csv")

# Make sure processed folder exists
os.makedirs(output_dir, exist_ok=True)

# Read extracted CSV
df = pd.read_csv(input_csv)

# Example transformation
df['amount_bucket'] = pd.cut(df['amount_ngn'],
                             bins=[0, 1000, 5000, 10000, 50000, 100000],
                             labels=['very low','low','medium','high','very high'])

# Save transformed CSV
df.to_csv(output_csv, index=False)

print(f"Transformation completed! Saved to {output_csv}")
