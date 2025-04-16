"""
Airflow DAG to orchestrate daily ingestion and anonymization of user data.

- Downloads static customer and postcode files
- Invokes main pipeline via BashOperator
- Demonstrates modular code reuse and parameterization
"""
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from src.pipeline import main  # Your pipeline’s entry function

with DAG('data_pipeline', start_date=datetime(2023, 1, 1), schedule_interval='@daily') as dag:
    run_pipeline = PythonOperator(
        task_id='run_pipeline',
        python_callable=main,
        op_args=['data/Customer_Data.json', 'data/National_Statistics_Postcode_Lookup_Latest_Centroids.csv']
    )