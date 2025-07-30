import pandas as pd
from sqlalchemy import create_engine, text
import os
import time # Import time module for delays

# --- Database Configuration ---
# DB_HOST is '127.0.0.1' for explicit IPv4 connection from host to Docker-mapped port.
DB_HOST = "127.0.0.1" # <--- CHANGE THIS FROM "localhost" TO "127.0.0.1"
DB_PORT = "5432"
DB_NAME = "stockdb"    # Matches POSTGRES_DB in your docker-compose.yml
DB_USER = "airflow"    # Matches POSTGRES_USER in your docker-compose.yml
DB_PASSWORD = "airflow" # Matches POSTGRES_PASSWORD in your docker-compose.yml

# Path to your stock data CSVs
DATA_DIR = "stock_data"

def load_csv_to_postgres(file_path, engine):
    """Loads a single CSV file into a PostgreSQL table."""
    try:
        df = pd.read_csv(file_path)

        table_name = os.path.basename(file_path).replace('.csv', '').lower()
        
        df.columns = [col.strip().replace(' ', '_').replace('.', '').lower() for col in df.columns]

        if 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date'], errors='coerce')
            df.dropna(subset=['date'], inplace=True)

        df.to_sql(table_name, engine, if_exists='replace', index=False)
        print(f"Successfully loaded {file_path} into table '{table_name}'")
    except Exception as e:
        print(f"Error loading {file_path}: {e}")

def main():
    """Iterates through CSV files in DATA_DIR and loads them to PostgreSQL."""
    DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

    max_retries = 10
    retry_delay_seconds = 5
    engine = None

    for i in range(max_retries):
        try:
            engine = create_engine(DATABASE_URL)
            with engine.connect() as connection:
                connection.execute(text("SELECT 1"))
            print("Successfully connected to PostgreSQL database!")
            break
        except Exception as e:
            print(f"Attempt {i+1}/{max_retries}: Failed to connect to PostgreSQL database: {e}")
            if i < max_retries - 1:
                print(f"Retrying in {retry_delay_seconds} seconds...")
                time.sleep(retry_delay_seconds)
            else:
                print("Max retries reached. Exiting.")
                return

    if engine is None:
        return

    if not os.path.exists(DATA_DIR):
        print(f"Error: Data directory '{DATA_DIR}' not found. Please create it and place your CSVs inside.")
        return

    csv_files = [f for f in os.listdir(DATA_DIR) if f.endswith('.csv')]
    if not csv_files:
        print(f"No CSV files found in '{DATA_DIR}'. Please place your stock CSVs in the '{DATA_DIR}' folder.")
        return

    print(f"Found {len(csv_files)} CSV files in '{DATA_DIR}'. Starting data loading...")
    for csv_file in csv_files:
        file_path = os.path.join(DATA_DIR, csv_file)
        load_csv_to_postgres(file_path, engine)
    print("Data loading complete.")

if __name__ == "__main__":
    main()
