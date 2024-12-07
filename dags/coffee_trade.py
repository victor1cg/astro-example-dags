from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime, timezone

import pandas as pd
import os
from io import StringIO

default_args = {
    'owner' : 'Victor',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1
}

def download_s3_file(bucket_name: str, key: str):
    s3_hook = S3Hook(aws_conn_id='aws_default')
    file_obj = s3_hook.get_key(key=key, bucket_name=bucket_name)
    content = file_obj.get()['Body'].read().decode('utf-8')
    return content

def process_data(**kwargs):
    ti = kwargs['ti']
    csv_content = ti.xcom_pull(task_id='download_file')

    df = pd.read_csv(StringIO(csv_content))

    df['time_stamp'] = datetime.now(timezone.utc)
    # Return the modified DataFrame as a CSV string
    return df.to_csv(index=False)

# INSERTING DATA ##########
def insert_into_postgres(table: str, **kwargs):
    ti = kwargs['ti']
    processed_csv = ti.xcom_pull(task_ids='process_data')
    df = pd.read_csv(StringIO(processed_csv))

    # Establish a connection to the PostgreSQL database
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    # Insert rows into the table
    for _, row in df.iterrows():
        cursor.execute(
            f"""
            INSERT INTO {table} (transaction_id, transaction_date, transaction_time, store_id, store_location, 
            product_id, transaction_qty, unit_price, product_category, product_type, product_detail, size, to
