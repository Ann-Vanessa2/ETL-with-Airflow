from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime, timedelta
import pandas as pd
from io import StringIO
import psycopg2
import os
import logging
from dotenv import load_dotenv

# # Load environment variables from .env
# load_dotenv('/opt/airflow/config/.env')

# Load environment variables from .env
env_path = '/opt/airflow/config/.env'
if os.path.exists(env_path):
    load_dotenv(env_path)
else:
    raise FileNotFoundError(f".env file not found at {env_path}")

# Ensure required environment variables are set
required_env_vars = ["S3_BUCKET", "RDS_HOST", "RDS_DB", "RDS_USER", "RDS_PASS"]
for var in required_env_vars:
    if not os.getenv(var):
        raise ValueError(f"Environment variable {var} is missing!")

# retrieve environment variables
S3_BUCKET = os.getenv("S3_BUCKET")
RDS_HOST = os.getenv("RDS_HOST")
RDS_DB = os.getenv("RDS_DB")
RDS_USER = os.getenv("RDS_USER")
RDS_PASS = os.getenv("RDS_PASS")

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# DAG default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 3, 12),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'music_streaming_etl',
    default_args=default_args,
    description='ETL pipeline for music streaming service',
    schedule_interval='@hourly',
    catchup=False,
)

# Task 1: Extract Data from RDS
def extract_from_rds():
    try:
        logging.info("Connecting to RDS database...")
        conn = psycopg2.connect(
            dbname=RDS_DB, user=RDS_USER, password=RDS_PASS,
            host=RDS_HOST, port="5432"
        )

        tracks_query = "SELECT * FROM tracks"
        users_query = "SELECT * FROM users"
        streams_query = "SELECT * FROM streams"
        df_tracks = pd.read_sql(tracks_query, conn)
        df_users = pd.read_sql(users_query, conn)
        df_streams = pd.read_sql(streams_query, conn)
        
        tracks_file_path = '/opt/airflow/dags/songs.csv'
        users_file_path = '/opt/airflow/dags/users.csv'
        streams_file_path = '/opt/airflow/dags/streams.csv'
        
        df_tracks.to_csv(tracks_file_path, index=False)
        df_users.to_csv(users_file_path, index=False)
        df_streams.to_csv(streams_file_path, index=False)
        
        logging.info(f"Data extracted and saved to {tracks_file_path} and {users_file_path}")
        conn.close()

    except Exception as e:
        logging.error(f"Error extracting data from RDS: {e}")
        raise

extract_rds_task = PythonOperator(
    task_id='extract_from_rds',
    python_callable=extract_from_rds,
    dag=dag,
)

# Task 2: Upload to S3
def upload_to_s3():
    try:
        files = {'songs.csv': 'raw-data/songs.csv', 
                 'users.csv': 'raw-data/users.csv', 
                 'streams.csv': 'raw-data/streams.csv'}
        
        s3_hook = S3Hook(aws_conn_id='aws_default')
        for local_file, s3_key in files.items():
            file_path = f'/opt/airflow/dags/{local_file}'
            logging.info(f"Uploading {file_path} to S3 bucket {S3_BUCKET} at {s3_key}")
            s3_hook.load_file(filename=file_path, key=s3_key, bucket_name=S3_BUCKET, replace=True)
            logging.info(f"{local_file} successfully uploaded to S3")
    except Exception as e:
        logging.error(f"Error uploading to S3: {e}")
        raise

upload_s3_task = PythonOperator(
    task_id='upload_to_s3',
    python_callable=upload_to_s3,
    dag=dag,
)

# Task 3: Transform Data and Compute KPIs directly from S3
def transform_data():
    try:
        # Initialize S3 Hook
        s3_hook = S3Hook(aws_conn_id='aws_default')

        # Define S3 paths
        raw_files = {
            'songs.csv': 'raw-data/songs.csv',
            'users.csv': 'raw-data/users.csv',
            'streams.csv': 'raw-data/streams.csv'
        }
        kpi_key = 'processed-data/kpis.csv'

        logging.info("Reading raw data directly from S3...")

        # Read CSVs directly from S3 into Pandas DataFrame
        df_tracks = pd.read_csv(StringIO(s3_hook.read_key(raw_files["songs.csv"], S3_BUCKET)))
        df_users = pd.read_csv(StringIO(s3_hook.read_key(raw_files["users.csv"], S3_BUCKET)))
        df_streams = pd.read_csv(StringIO(s3_hook.read_key(raw_files["streams.csv"], S3_BUCKET)))

        logging.info("Transforming data...")
        df_tracks.drop_duplicates(inplace=True)
        df_tracks.dropna(inplace=True)
        df_streams.dropna(inplace=True)

        # Convert listen_time to datetime and extract date/hour
        if 'listen_time' not in df_streams.columns:
            raise KeyError("'listen_time' column is missing!")
        
        df_streams['listen_time'] = pd.to_datetime(df_streams['listen_time'], errors='coerce')

        df_streams['date'] = df_streams['listen_time'].dt.date
        df_streams['hour'] = df_streams['listen_time'].dt.hour

        # Check for missing values in the 'hour' column
        if df_streams['hour'].isnull().sum() > 0:
            raise ValueError("❌ Some rows have missing 'hour' values. Check listen_time column!")

        # Merging the datasets
        df_merged = df_streams.merge(df_tracks, on='track_id', how='left')
        df_merged = df_merged.merge(df_users, on='user_id', how='left')

        logging.info("Computing KPIs...")
        kpis = {
            'listen_count': df_merged.groupby(['date', 'track_genre'])['track_id'].count(),
            'avg_track_duration': df_merged.groupby(['date', 'track_genre'])['duration_ms'].mean(),
            'popularity_index': df_merged.groupby(['date', 'track_genre'])['popularity'].mean(),
            'most_popular_track': df_merged.loc[df_merged.groupby(['date', 'track_genre'])['listen_time'].idxmax()][['date', 'track_genre', 'track_id']],
            'unique_listeners': df_merged.groupby(['date', 'hour'])['user_id'].nunique(),
            'top_artists_per_day': df_merged.groupby(['date', 'hour'])['artists'].apply(lambda x: x.value_counts().idxmax()),
            'track_diversity_index': df_merged.groupby(['date', 'hour']).apply(lambda x: x['track_id'].nunique() / len(x) if len(x) > 0 else 0)
        }

        kpi_df = pd.DataFrame(kpis)

        # Save KPI DataFrame to S3
        csv_buffer = StringIO()
        kpi_df.to_csv(csv_buffer, index=True)
        s3_hook.load_string(csv_buffer.getvalue(), key=kpi_key, bucket_name=S3_BUCKET, replace=True)

        logging.info(f"✅ KPI computation complete. Results saved to S3 at {kpi_key}")

    except Exception as e:
        logging.error(f"Error in data transformation: {e}")
        raise

# Airflow Task
transform_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    dag=dag,
)


extract_rds_task >> upload_s3_task >> transform_task
