import os
import gdown
import duckdb
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
from duckdb import DuckDBPyRelation
from pandas import DataFrame

# Load Environment Variables
load_dotenv()

# Function to download the files from given link
def download_google_drive_files(folder_url, local_directory):
    os.makedirs(local_directory, exist_ok=True)
    gdown.download_folder(folder_url, output=local_directory, quiet=False, use_cookies=False)

# Function to list CSV files from a given directory
def listing_csv_files(directory):
    csv_files = []
    all_files = os.listdir(directory)
    for file in all_files:
        if file.endswith(".csv"):
            complete_path = os.path.join(directory, file)
            csv_files.append(complete_path)
    #print(csv_files)
    return csv_files

# Function to read CSV Files and return a DuckDB DataFram
def read_csv(file_path):
    duckdb_dataframe = duckdb.read_csv(file_path)
    return duckdb_dataframe

# Function to add a "Total Sales" column
def transform(df: DuckDBPyRelation) -> DataFrame:
    # Run the SQL responsibile for adding the new column, over virtual table
    transformed_df = duckdb.sql("SELECT *, quantidade * valor AS total_vendas FROM df").df()
    # Remove row from virtual table to clean up
    return transformed_df

# Function to convert DuckDB into Pandas and save the DataFrame into PostgreSQL
def save_into_postgres(df_duckdb, table):
    DATABASE_URL = os.getenv("DATABASE_URL") # Ex: 'postgresql://user:password@localhost:5432/database_name'
    engine = create_engine(DATABASE_URL)
    # Save the DataFrame into PostgreSQL
    df_duckdb.to_sql(table, con=engine, if_exists='append', index=False)

# Calling above functions
if __name__ == "__main__":
    folder_url = 'https://drive.google.com/drive/folders/1koA2HmWYH7ugErMfE4-0c5Dhis11pZnj?usp=sharing'
    local_directory = './gdown_folder'
    download_google_drive_files(folder_url, local_directory)
    file_list = listing_csv_files(local_directory)
    for file_path in file_list:
        duck_db_df = read_csv(file_path)
        transformed_pandas_df = transform(duck_db_df)
        save_into_postgres(transformed_pandas_df, "vendas_calculado")