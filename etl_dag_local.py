from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import pandas as pd
from io import StringIO
import psycopg2
import os
import logging
from dotenv import load_dotenv


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
        daily_kpi_key = 'processed-data/daily_kpi.csv'
        hourly_kpi_key = 'processed-data/hourly_kpi.csv'

        logging.info("Reading raw data directly from S3...")

        # Read CSVs directly from S3 into Pandas DataFrame
        try:
            df_tracks = pd.read_csv(StringIO(s3_hook.read_key(raw_files["songs.csv"], S3_BUCKET)))
            df_users = pd.read_csv(StringIO(s3_hook.read_key(raw_files["users.csv"], S3_BUCKET)))
            df_streams = pd.read_csv(StringIO(s3_hook.read_key(raw_files["streams.csv"], S3_BUCKET)))
        except Exception as e:
            logging.error(f"Error reading data from S3: {e}")
            raise

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
            raise ValueError("Some rows have missing 'hour' values. Check listen_time column!")

        # Merging datasets
        df_merged = df_streams.merge(df_tracks, on='track_id', how='left')
        df_merged = df_merged.merge(df_users, on='user_id', how='left')

        logging.info("Computing Daily KPIs...")

        # Daily KPIs
        daily_kpi_df = df_merged.groupby(['date', 'track_genre']).agg({
            'track_id': 'count',             # Total listen count
            'duration_ms': 'mean',           # Average track duration
            'popularity': 'mean'             # Popularity index
        }).reset_index().rename(columns={
            'track_id': 'listen_count',
            'duration_ms': 'avg_track_duration',
            'popularity': 'popularity_index'
        })

        # Compute the most popular track per day & genre
        most_popular_track = df_merged.groupby(['date', 'track_genre'])['track_id'].agg(lambda x: x.value_counts().idxmax()).reset_index()
        most_popular_track.rename(columns={'track_id': 'most_popular_track'}, inplace=True)

        # Merge popular track data with daily KPIs
        daily_kpi_df = daily_kpi_df.merge(most_popular_track, on=['date', 'track_genre'], how='left')

        logging.info("Computing Hourly KPIs...")

        # Hourly KPIs
        hourly_kpi_df = df_merged.groupby(['date', 'hour']).agg({
            'user_id': pd.Series.nunique,    # Unique listeners per hour
            'track_id': lambda x: x.nunique() / len(x) if len(x) > 0 else 0  # Track diversity index
        }).reset_index().rename(columns={
            'user_id': 'unique_listeners',
            'track_id': 'track_diversity_index'
        })

        # Compute the top artist per hour
        top_artists_per_hour = df_merged.groupby(['date', 'hour'])['artists'].agg(lambda x: x.value_counts().idxmax()).reset_index()
        top_artists_per_hour.rename(columns={'artists': 'top_artists_per_hour'}, inplace=True)

        # Merge with hourly KPIs
        hourly_kpi_df = hourly_kpi_df.merge(top_artists_per_hour, on=['date', 'hour'], how='left')

        # Ensure all numeric columns are float64 to avoid S3 type issues
        for col in daily_kpi_df.select_dtypes(include=['number']).columns:
            daily_kpi_df[col] = daily_kpi_df[col].astype('float64')

        for col in hourly_kpi_df.select_dtypes(include=['number']).columns:
            hourly_kpi_df[col] = hourly_kpi_df[col].astype('float64')

        logging.info("Saving KPIs to S3...")

        # Convert DataFrames to CSV and upload to S3
        daily_csv_buffer = StringIO()
        daily_kpi_df.to_csv(daily_csv_buffer, index=False)
        s3_hook.load_string(daily_csv_buffer.getvalue(), key=daily_kpi_key, bucket_name=S3_BUCKET, replace=True)

        hourly_csv_buffer = StringIO()
        hourly_kpi_df.to_csv(hourly_csv_buffer, index=False)
        s3_hook.load_string(hourly_csv_buffer.getvalue(), key=hourly_kpi_key, bucket_name=S3_BUCKET, replace=True)

        logging.info(f"KPI computation complete. Daily KPIs saved to {daily_kpi_key}, Hourly KPIs saved to {hourly_kpi_key}")

    except Exception as e:
        logging.error(f"Error in data transformation: {e}")
        raise

transform_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    dag=dag,
)

# Task 4: Load Data into Redshift
def load_to_redshift():
    try:
        redshift_hook = PostgresHook(postgres_conn_id='redshift_default')

        # S3 paths for the KPI files
        daily_kpi_s3_path = "s3://music-streaming-data-a/processed-data/daily_kpi.csv"
        hourly_kpi_s3_path = "s3://music-streaming-data-a/processed-data/hourly_kpi.csv"
        
        # IAM Role
        iam_role = "arn:aws:iam::242201306552:role/service-role/AmazonRedshift-CommandsAccessRole-20250314T101143"

        # Step 1: Load data into staging tables
        copy_daily_kpis = f"""
        COPY public.staging_daily_kpis(date, track_genre, listen_count, avg_track_duration, popularity_index, most_popular_track)
        FROM '{daily_kpi_s3_path}'
        IAM_ROLE '{iam_role}'
        DELIMITER ','
        CSV
        IGNOREHEADER 1;
        """

        copy_hourly_kpis = f"""
        COPY public.staging_hourly_kpis (date, hour, unique_listeners, track_diversity_index, top_artists_per_hour)
        FROM '{hourly_kpi_s3_path}'
        IAM_ROLE '{iam_role}'
        CSV
        IGNOREHEADER 1;
        """
        # TRUNCATECOLUMNS

        logging.info("Loading daily KPIs into Redshift staging table...")
        logging.info(f"Executing COPY command: {copy_daily_kpis}")
        redshift_hook.run(copy_daily_kpis, autocommit=True)
        logging.info("Daily KPIs successfully loaded into staging!")

        logging.info("Loading hourly KPIs into Redshift staging table...")
        logging.info(f"Executing COPY command: {copy_hourly_kpis}")
        redshift_hook.run(copy_hourly_kpis, autocommit=True)
        logging.info("Hourly KPIs successfully loaded into staging!")

        # Upsert (Merge) staging data into main tables
        merge_query = """
        BEGIN;

        -- Delete old records that exist in the new batch
        DELETE FROM daily_kpis 
        WHERE date IN (SELECT DISTINCT date FROM staging_daily_kpis);

        -- Insert the new records
        INSERT INTO daily_kpis
        SELECT * FROM staging_daily_kpis;

        -- Clean up the staging table
        TRUNCATE TABLE staging_daily_kpis;

        -- Repeat for hourly_kpis
        DELETE FROM hourly_kpis 
        WHERE date IN (SELECT DISTINCT date FROM staging_hourly_kpis);

        INSERT INTO hourly_kpis
        SELECT * FROM staging_hourly_kpis;

        TRUNCATE TABLE staging_hourly_kpis;

        COMMIT;
        """

        logging.info("Merging data into main tables...")
        redshift_hook.run(merge_query)
        logging.info("Upsert completed successfully!")

    except Exception as e:
        logging.error(f"Error loading data into Redshift: {e}")
        raise

# Airflow Task
load_data_task = PythonOperator(
    task_id="load_to_redshift",
    python_callable=load_to_redshift,
    dag=dag,
)

extract_rds_task >> upload_s3_task >> transform_task >> load_data_task
