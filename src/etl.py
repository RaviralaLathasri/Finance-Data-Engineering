from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os
import pandas as pd
from datasets import load_dataset
import psycopg2

# -----------------------------
# ETL Functions
# -----------------------------
def extract(**kwargs):
    print("Starting Extract...")
    dataset = load_dataset(
        "electricsheepafrica/nigerian-banking-retail-transactions",
        split="train"
    )
    os.makedirs("/opt/airflow/data/raw", exist_ok=True)
    csv_path = "/opt/airflow/data/raw/nigerian_retail_transactions.csv"

    batch_size = 500_000
    for i in range(0, len(dataset), batch_size):
        chunk_dataset = dataset.select(range(i, min(i + batch_size, len(dataset))))
        df = chunk_dataset.to_pandas()
        mode = "w" if i == 0 else "a"
        header = (i == 0)
        df.to_csv(csv_path, mode=mode, header=header, index=False)
        print(f"Saved rows {i} to {i + len(df)}")

    print("Extract completed ✅")
    return csv_path

def transform(**kwargs):
    print("Starting Transform...")
    input_csv = "/opt/airflow/data/raw/nigerian_retail_transactions.csv"
    output_dir = "/opt/airflow/data/processed"
    os.makedirs(output_dir, exist_ok=True)
    output_csv = os.path.join(output_dir, "transformed_transactions.csv")

    df = pd.read_csv(input_csv)
    df['amount_bucket'] = pd.cut(df['amount_ngn'],
                                 bins=[0, 1000, 5000, 10000, 50000, 100000],
                                 labels=['very low','low','medium','high','very high'])
    df.to_csv(output_csv, index=False)
    print(f"Transform completed! Saved to {output_csv} ✅")
    return output_csv

def load(**kwargs):
    print("Starting Load...")
    input_csv = "/opt/airflow/data/processed/transformed_transactions.csv"
    df = pd.read_csv(input_csv)

    DB_HOST = "postgres"
    DB_PORT = "5432"
    DB_NAME = "ny_taxi"
    DB_USER = "root"
    DB_PASSWORD = "root"

    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )
    cur = conn.cursor()

    table_name = "nigerian_transactions"
    cols = df.columns
    col_types = []
    for c in df.columns:
        if pd.api.types.is_numeric_dtype(df[c]):
            col_types.append(f"{c} NUMERIC")
        else:
            col_types.append(f"{c} TEXT")

    create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join(col_types)});"
    cur.execute(create_table_query)
    conn.commit()

    for _, row in df.iterrows():
        values = tuple(row)
        placeholders = ", ".join(["%s"] * len(row))
        insert_query = f"INSERT INTO {table_name} ({', '.join(cols)}) VALUES ({placeholders})"
        cur.execute(insert_query, values)

    conn.commit()
    cur.close()
    conn.close()
    print(f"Load completed! {len(df)} rows inserted into `{table_name}` ✅")

# -----------------------------
# DAG Definition
# -----------------------------
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2026, 1, 27),
    'retries': 1,
}

with DAG(
    dag_id='nigerian_transactions_etl',
    default_args=default_args,
    schedule_interval='@daily',  # runs daily
    catchup=False,
) as dag:

    t1 = PythonOperator(
        task_id='extract',
        python_callable=extract
    )

    t2 = PythonOperator(
        task_id='transform',
        python_callable=transform
    )

    t3 = PythonOperator(
        task_id='load',
        python_callable=load
    )

    # DAG flow: Extract -> Transform -> Load
    t1 >> t2 >> t3
