from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import boto3
import psycopg2
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv('/usr/local/airflow/.env')  # Adjust path based on MWAA setup

# AWS & DB Credentials (from .env file)
S3_BUCKET = os.getenv("S3_BUCKET")

RDS_HOST = os.getenv("RDS_HOST")
RDS_DB = os.getenv("RDS_DB")
RDS_USER = os.getenv("RDS_USER")
RDS_PASS = os.getenv("RDS_PASS")

REDSHIFT_HOST = os.getenv("REDSHIFT_HOST")
REDSHIFT_DB = os.getenv("REDSHIFT_DB")
REDSHIFT_USER = os.getenv("REDSHIFT_USER")
REDSHIFT_PASS = os.getenv("REDSHIFT_PASS")
REDSHIFT_PORT = os.getenv("REDSHIFT_PORT", 5439)

# Default DAG args
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 3, 12),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define DAG
dag = DAG(
    'music_streaming_etl',
    default_args=default_args,
    description='ETL pipeline for music streaming service',
    schedule_interval='@hourly',
    catchup=False,
)

# Task 1: Extract Data from PostgreSQL (RDS)
def extract_from_rds():
    conn = psycopg2.connect(
        dbname=RDS_DB, user=RDS_USER, password=RDS_PASS,
        host=RDS_HOST, port="5432"
    )
    query = "SELECT * FROM songs"
    df = pd.read_sql(query, conn)
    df.to_csv('/tmp/songs.csv', index=False)
    conn.close()

extract_rds_task = PythonOperator(
    task_id='extract_from_rds',
    python_callable=extract_from_rds,
    dag=dag,
)

# Task 2: Upload to S3
def upload_to_s3():
    s3 = boto3.client('s3')
    s3.upload_file('/tmp/songs.csv', S3_BUCKET, 'songs.csv')

upload_s3_task = PythonOperator(
    task_id='upload_to_s3',
    python_callable=upload_to_s3,
    dag=dag,
)

# Task 3: Load Data into Redshift
load_to_redshift = S3ToRedshiftOperator(
    task_id='load_to_redshift',
    schema='public',
    table='songs',
    s3_bucket=S3_BUCKET,
    s3_key='songs.csv',
    copy_options=["CSV"],
    redshift_conn_id='redshift_default',
    dag=dag,
)

# Task 4: Compute KPIs
compute_kpis = PostgresOperator(
    task_id='compute_kpis',
    postgres_conn_id='redshift_default',
    sql='''
        INSERT INTO kpi_table (genre, listen_count, avg_duration, popularity_index)
        SELECT genre, COUNT(*), AVG(duration), SUM(play_count + likes + shares)
        FROM streaming_data
        GROUP BY genre;
    ''',
    dag=dag,
)

# Define DAG Dependencies
extract_rds_task >> upload_s3_task >> load_to_redshift >> compute_kpis
