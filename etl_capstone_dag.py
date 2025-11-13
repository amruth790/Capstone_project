from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd

def extract_data():
    data = pd.read_csv('/opt/airflow/dags/data/sales_data.csv')
    data.to_csv('/opt/airflow/dags/temp/extracted.csv', index=False)
    print("✅ Data extracted")

def transform_data():
    df = pd.read_csv('/opt/airflow/dags/temp/extracted.csv')
    df['Total'] = df['Quantity'] * df['Price']
    df.to_csv('/opt/airflow/dags/temp/transformed.csv', index=False)
    print("✅ Data transformed")

def load_data():
    df = pd.read_csv('/opt/airflow/dags/temp/transformed.csv')
    df.to_csv('/opt/airflow/dags/output/final_sales.csv', index=False)
    print("✅ Data loaded")

default_args = {
    'owner': 'Aravind',
    'start_date': datetime(2025, 11, 10),
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

with DAG('capstone_etl_dag',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False) as dag:

    extract = PythonOperator(
        task_id='extract_data',
        python_callable=extract_data
    )

    transform = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data
    )

    load = PythonOperator(
        task_id='load_data',
        python_callable=load_data
    )

    extract >> transform >> load
