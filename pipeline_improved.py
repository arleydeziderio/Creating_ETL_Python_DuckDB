import os
import gdown
import duckdb
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
from datetime import datetime

# Load Environment Variables
load_dotenv()

# Function to connect to the database
def connect_db():
    """Connect to DuckDB database - creates the db if it doesn't exist"""
    return duckdb.connect(database='duckdb.db', read_only=False)

# Function to check and create table if it doesn't exist
def start_table(con):
    """Create the table if it doesn't exist"""
    con.execute("""
        CREATE TABLE IF NOT EXISTS historico_arquivos (
            nome_arquivo VARCHAR,
            horario_processamento TIMESTAMP
        )
    """)

# Function to log a new file into the database with the current date/time
def log_file(con, file_name):
    """Logs a new file into the database with the current date/time."""
    con.execute("""
        INSERT INTO historico_arquivos (nome_arquivo, horario_processamento)
        VALUES (?, ?)
    """, (file_name, datetime.now()))
    
# Function to return a set with each processed file name
def processed_files(con):
    """Returns a set with each processed files name"""
    return set(row[0] for row in con.execute("SELECT nome_arquivo FROM historico_arquivos").fetchall())

# Function to download the files from given link
def download_google_drive_files(folder_url, local_directory):
    os.makedirs(local_directory, exist_ok=True)
    gdown.download_folder(folder_url, output=local_directory, quiet=False, use_cookies=False)

# Function to list files and types
def list_files_types(directory):
    """Lists files and identify if they're CSV, JSON or Parquet."""
    files_and_types = []
    for file in os.listdir(directory):
        if file.endswith(".csv") or file.endswith(".json") or file.endswith(".parquet"):
            complete_path = os.path.join(directory, file)
            type = file.split(".")[-1]
            files_and_types.append((complete_path, type))
    return files_and_types

# Function to read files according to its type and returns a DataFrame
def read_files(file_path, type):
    """Reads the file according to its type and returns a DataFrame"""
    if type == 'csv':
        return duckdb.read_csv(file_path)
    elif type == 'json':
        return pd.read_json(file_path)
    elif type == 'parquet':
        return pd.read_parquet(file_path)
    else:
        raise ValueError(f"Unsupported file type: {type}")

# Function to add a "Total Sales" column
def transform(df):
    # Run the SQL responsibile for adding the new column, over virtual table
    transformed_df = duckdb.sql("SELECT *, quantidade * valor AS total_vendas FROM df").df()
    # Remove row from virtual table to clean up
    print(transformed_df)
    return transformed_df

# Function to convert DuckDB into Pandas and save the DataFrame into PostgreSQL
def save_into_postgres(df_duckdb, table):
    DATABASE_URL = os.getenv("DATABASE_URL") # Ex: 'postgresql://user:password@localhost:5432/database_name'
    engine = create_engine(DATABASE_URL)
    # Save the DataFrame into PostgreSQL
    df_duckdb.to_sql(table, con=engine, if_exists='append', index=False)

# Function to prepare the calling of all other functions in order to import it to the app.py script
def pipeline():
    folder_url = 'https://drive.google.com/drive/folders/1koA2HmWYH7ugErMfE4-0c5Dhis11pZnj?usp=sharing'
    local_directory = './gdown_folder'

    download_google_drive_files(folder_url, local_directory)
    con = connect_db()
    start_table(con)
    processed = processed_files(con)
    files_and_types = list_files_types(local_directory)

    logs = []
    for file_path, type in files_and_types:
        file_name = os.path.basename(file_path)
        if file_name not in processed:
            df = read_files(file_path, type)
            transformed_df = transform(df)
            save_into_postgres(transformed_df, "vendas_calculado")
            log_file(con, file_name)
            print(f"File {file_name} processed and saved")
            logs.append(f"File {file_name} processed and saved")
        else:
            print(f"File {file_name} have already been processed before")
            logs.append(f"File {file_name} have already been processed before")
    return logs

# Calling above functions
if __name__ == "__main__":
    pipeline()