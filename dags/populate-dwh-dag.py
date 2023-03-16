from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from sqlalchemy import create_engine
import os
import pandas as pd

args = {"owner": "Winny M.",
        "start_date": days_ago(1)}
dag = DAG(
    dag_id="populate_dwh",
    default_args=args,
    schedule_interval="@daily"
)


def load_tables(path):
    """
    Loading tables from a given filepath to a defined DB
    """
    engine = create_engine("postgresql+psycopg2://airflow:airflow@postgres:5432/airflow")
    if path.lower().endswith(".json"):
        with pd.read_json(path, lines = True, chunksize=10_000, nrows=50_000) as reader:
            for chunk in reader:
                if "hours" in chunk.columns:
                    chunk.drop("hours", axis=1, inplace=True)
                if "attributes" in chunk.columns:
                    chunk.drop("attributes", axis=1, inplace=True)
                chunk.to_sql(os.path.basename(path).replace(".json", ""), engine, if_exists='append', index=False)
    elif path.lower().endswith(".csv"):
        with pd.read_csv(path, chunksize=1e6) as reader:
            for chunk in reader:
                chunk.to_sql(os.path.basename(path).replace(".csv", ""), engine, if_exists='append', index=False)


def load_all_files(dl_path="/opt/datalake"):
    """
    Obtaining all files to be loaded to SQL tables
    """
    files_paths= [os.path.join(dl_path, f) for f in os.listdir(dl_path)]
    for fp in files_paths:
        load_tables(fp)

with dag:
    populate = PythonOperator(
        task_id="popul_dwh",
        python_callable=load_all_files
    )
