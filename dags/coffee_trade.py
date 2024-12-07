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

def download_s3_file(bucket_name:str, key:str):
    s3_hook = S3Hook(aws_conn_id = 'aws_default')
    file_obj = s3_hook.get_key(key=key , bucket_name = bucket_name)
    content = file_obj.get()['Body'].read().decode('utf-8')
    return content

def process_data(**kwargs):
    ti=kwargs['ti']
    csv_content = ti.xcom_pull(task_id = 'download_file')

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
            INSERT INTO {table} 
            (transaction_id, transaction_date, transaction_time, store_id, store_location, 
            product_id, transaction_qty, unit_price, product_category, product_type, product_detail, size, 
            total_bill, month_name, day_name, hour, day_of_week, month, time_stamp)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            tuple(row)  # Ensure this is properly passing a tuple of the row's values
        )

    conn.commit()
    cursor.close()


with DAG(
    'tutorial',
    default_args=default_args,
    description='Load data from S3 to Postgres with TimeStamp Column',
    schedule_interval=None,
    start_date=datetime(2024,11,1),
    catchup = False,
    tags=['example'],
) as dag:
     
    download_file = PythonOperator(
          task_id = 'download_file',
          python_callable = 'download_s3_file',
          op_kwargs = {
               'bucket_name':'airflow-files-goncavic01',
               'key':' coffee_data/'
          })

    process_data = PythonOperator(
        task_id='process_data',
        python_callable=process_data,
        )

    insert_data = PythonOperator(
        task_id='insert_data',
        python_callable=insert_into_postgres,
        op_kwargs={
            'table': 'nasa.rovers.coffee_trade'
        },
     )
    
    start = EmptyOperator(task_id = 'start') 
    
    end = EmptyOperator (task_id = 'end')

    #set task dependecies
    start >> download_file >> process_data >> insert_data >> end