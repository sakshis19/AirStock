from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta # Ensure timedelta is imported if used
import os
import pandas as pd
import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_values
import numpy as np
from sqlalchemy import create_engine # <--- THIS IMPORT IS CRUCIAL AND MUST BE AT THE TOP

# Database connection details (ensure these match your docker-compose.yml)
POSTGRES_HOST = "pg-db" # This is the service name in docker-compose.yml
POSTGRES_PORT = "5432"
POSTGRES_DB = "stockdb"
POSTGRES_USER = "airflow"
POSTGRES_PASSWORD = "airflow"

# --- Helper function for calculating RSI (Relative Strength Index) ---
def calculate_rsi(series, window=14):
    delta = series.diff()
    gain = delta.where(delta > 0, 0)
    loss = -delta.where(delta < 0, 0)
    avg_gain = gain.rolling(window=window, min_periods=1).mean()
    avg_loss = loss.rolling(window=window, min_periods=1).mean()
    rs = np.where(avg_loss == 0, np.inf, avg_gain / avg_loss)
    rsi = 100 - (100 / (1 + rs))
    return rsi

def ingest_data_to_postgres():
    """
    Connects to PostgreSQL and ingests data from CSV files located in /opt/airflow/data.
    Dynamically creates or replaces tables based on CSV filenames and column headers.
    This function is adapted for stock CSVs.
    """
    conn_params = {
        'host': POSTGRES_HOST, # Use defined constants
        'dbname': POSTGRES_DB,
        'user': POSTGRES_USER,
        'password': POSTGRES_PASSWORD
    }

    conn = None
    cur = None
    try:
        conn = psycopg2.connect(**conn_params)
        cur = conn.cursor()
        print("Successfully connected to PostgreSQL database from Airflow DAG for ingestion.")

        data_folder = '/opt/airflow/data'
        if not os.path.exists(data_folder):
            print(f"Error: Data folder '{data_folder}' not found inside Airflow container.")
            return # Or raise an exception if this is critical

        csv_files = [f for f in os.listdir(data_folder) if f.endswith('_ns_enriched.csv')]
        if not csv_files:
            print(f"No '_ns_enriched.csv' files found in '{data_folder}'. Please ensure your stock CSVs are in the './stock_data' folder on your host.")
            return # Or raise an exception

        # --- Create SQLAlchemy Engine for Pandas in ingest_data_to_postgres ---
        # It's good practice to use SQLAlchemy engine for pandas.read_sql/to_sql
        # even in the ingest function if you're using pandas for read/write.
        db_connection_str = f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
        engine = create_engine(db_connection_str)
        print("SQLAlchemy engine created for Pandas in ingestion.")

        for filename in csv_files:
            filepath = os.path.join(data_folder, filename)
            print(f"Processing file for ingestion: {filepath}")
            
            # Use SQLAlchemy engine for pd.read_csv if it's reading from DB, but here it's CSV
            df = pd.read_csv(filepath) 
            table_name = os.path.basename(filename).replace('.csv', '').lower()
            df.columns = [col.strip().replace(' ', '_').replace('.', '').lower() for col in df.columns]
            df = df.where(pd.notna(df), None)

            if 'date' in df.columns:
                df['date'] = pd.to_datetime(df['date'], errors='coerce')
                df = df.dropna(subset=['date'])

            # Use psycopg2 for DDL (DROP TABLE)
            cur.execute(sql.SQL("DROP TABLE IF EXISTS {} CASCADE").format(sql.Identifier(table_name)))
            conn.commit()
            print(f"Dropped existing table '{table_name}' (if any).")

            # Use SQLAlchemy engine for df.to_sql
            df.to_sql(table_name, engine, if_exists='replace', index=False)
            print(f"Successfully inserted {len(df)} rows into table '{table_name}'.")

    except psycopg2.Error as e:
        print(f"Database error during ingestion: {e}")
        if conn:
            conn.rollback()
        raise
    except Exception as e:
        print(f"An unexpected error occurred during ingestion: {e}")
        raise
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()
            print("Ingestion database connection closed.")


def transform_stock_data_and_store_features():
    """
    Fetches raw stock data from PostgreSQL, performs feature engineering,
    and stores the transformed data into new feature tables.
    """
    conn = None # Initialize conn to None
    try:
        conn = psycopg2.connect(
            host=POSTGRES_HOST,
            port=POSTGRES_PORT,
            database=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD
        )
        conn.autocommit = True
        cursor = conn.cursor()
        print(f"Successfully connected to PostgreSQL database from Airflow DAG for transformation.")

        db_connection_str = f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
        engine = create_engine(db_connection_str)
        print("SQLAlchemy engine created for Pandas.")

        cursor.execute(sql.SQL(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_name LIKE '%_ns_enriched';"
        ))
        raw_table_names = [row[0] for row in cursor.fetchall()]

        if not raw_table_names:
            print("No raw stock tables found (e.g., no tables ending with '_ns_enriched'). Skipping transformation.")
            return

        for raw_table_name in raw_table_names:
            print(f"Transforming data for table: {raw_table_name}")
            
            # --- CRITICAL FIX HERE: Use sql.SQL and sql.Identifier to properly quote the table name ---
            select_query = sql.SQL("SELECT date, close, volume FROM {} ORDER BY date ASC").format(
                sql.Identifier(raw_table_name)
            ).as_string(conn) # .as_string(conn) converts the SQL object to a plain string for pd.read_sql

            df_raw = pd.read_sql(select_query, engine, parse_dates=['date'])
            
            if df_raw.empty:
                print(f"No data in {raw_table_name}. Skipping transformation for this table.")
                continue

            # --- Feature Engineering using Pandas ---
            # Make sure 'close' and 'volume' are numeric
            df_raw['close'] = pd.to_numeric(df_raw['close'], errors='coerce')
            df_raw['volume'] = pd.to_numeric(df_raw['volume'], errors='coerce')
            df_raw.dropna(subset=['close', 'volume'], inplace=True) # Drop rows where close/volume couldn't be converted

            if df_raw.empty:
                print(f"No valid numeric data in {raw_table_name} after cleaning. Skipping transformation.")
                continue

            # Set 'date' as index for time-series operations
            df_raw.set_index('date', inplace=True)

            # Simple Moving Averages (using 'close')
            df_raw['SMA_10'] = df_raw['close'].rolling(window=10).mean()
            df_raw['SMA_50'] = df_raw['close'].rolling(window=50).mean()

            # Daily Returns (using 'close')
            df_raw['Daily_Return'] = df_raw['close'].pct_change()

            # Relative Strength Index (RSI) (using 'close')
            df_raw['RSI_14'] = calculate_rsi(df_raw['close'], window=14)

            # Volatility (Standard Deviation of Daily Returns) (using 'Daily_Return')
            df_raw['Volatility_30'] = df_raw['Daily_Return'].rolling(window=30).std()

            # Add more features as per your ML model's requirements

            # Reset index to make 'date' a column again before saving to SQL
            df_transformed = df_raw.reset_index()

            # Drop rows with NaN values that result from rolling window calculations
            df_transformed.dropna(inplace=True)

            if df_transformed.empty:
                print(f"No transformed data for {raw_table_name} after dropping NaNs. Skipping save.")
                continue

            # Define the new table name for transformed features
            features_table_name = raw_table_name.replace('_ns_enriched', '_ns_features')
            
            # Drop existing features table if it exists (using psycopg2 connection for DDL)
            cursor.execute(sql.SQL("DROP TABLE IF EXISTS {} CASCADE").format(sql.Identifier(features_table_name)))
            conn.commit()
            print(f"Dropped existing features table '{features_table_name}' (if any).")

            # Store transformed data into the new table
            df_transformed.to_sql(features_table_name, engine, if_exists='replace', index=False)
            conn.commit()
            print(f"Successfully transformed data and loaded into '{features_table_name}'. Rows: {len(df_transformed)}")

    except psycopg2.Error as e:
        print(f"Database error during transformation: {e}")
        if conn:
            conn.rollback()
        raise
    except Exception as e:
        print(f"An unexpected error occurred during transformation: {e}")
        raise
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
            print("Transformation database connection closed.")


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'email_on_failure': False,
    'email_on_retry': False,
}

with DAG(
    'stock_etl_dag',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    tags=['stock_pipeline', 'etl', 'features'],
) as dag:
    ingest_task = PythonOperator(
        task_id='ingest_stock_data_to_postgres',
        python_callable=ingest_data_to_postgres,
    )

    transform_task = PythonOperator(
        task_id='transform_stock_features',
        python_callable=transform_stock_data_and_store_features,
    )

    # Define the task dependency: Ingestion must complete before Transformation
    ingest_task >> transform_task
