from datasets import load_dataset
import os

dataset = load_dataset(
    "electricsheepafrica/nigerian-banking-retail-transactions",
    split="train"
)

os.makedirs("data", exist_ok=True)

batch_size = 500_000

for i in range(0, len(dataset), batch_size):
    chunk_dataset = dataset.select(range(i, min(i + batch_size, len(dataset))))
    df = chunk_dataset.to_pandas()

    mode = "w" if i == 0 else "a"
    header = (i == 0)

    df.to_csv(
        "data/nigerian_retail_transactions.csv",
        mode=mode,
        header=header,
        index=False
    )

    print(f"Saved rows {i} to {i + len(df)}")

print("Extract completed")
