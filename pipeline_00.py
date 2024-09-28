import os
import gdown
import duckdb
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv

def download_google_drive_files(folder_url, local_directory):
    os.makedirs(local_directory, exist_ok=True)
    gdown.download_folder(folder_url, output=local_directory, quiet=False, use_cookies=False)

if __name__ == "__main__":
    folder_url = 'https://drive.google.com/drive/folders/1maqV7E3NRlHp12CsI4dvrCFYwYi7BAAf'
    local_directory = './gdown_folder'
    download_google_drive_files(folder_url, local_directory)