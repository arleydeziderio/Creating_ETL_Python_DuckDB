<span id="top"></span>
<h1 align="center">Creating an ETL using Python and DuckDB </h1>

<div align="center">
	
  Project to process files in different extensions and export the processed data into a Postgres DB <br/>

</div>

 Libs used in this project

 * OS
 * gdown
 * DuckDB
 * Pandas
 * SQLAlchemy
 * dotenv
 * datetime
 * streamlit
 * psycopg2
 * psycopg2-binary
<br/>

Containerization:
* Docker

 ## To Use

To clone and run this repository you'll need [Git](https://git-scm.com), [Python](https://www.python.org/downloads/) and the above libs installed on your computer (install the libs using pip install <lib_name>).

From your command line:

```bash
# Clone this repository
git clone https://github.com/arleydeziderio/Creating_ETL_Python_DuckDB.git

# Go into the repository
cd Creating_ETL_Python_DuckDB

# Install dependencies
poetry install

# Run the app locally
python app.py
-- or --
# Run the app via web browser
Access the following URL: https://deploying-app-docker.onrender.com
```
<br/>

## Description

I've developed four different ETL scripts using Python, each one of them with a new, or better code and porpose.

* pipeline_for_csv_only.py:<br/>
    Only processes files which the extension is .csv and was conceived to do the following:
    <br/>
    <br/>
      * Extract: 
        * Download files from a Google Drive using gdown lib
        * List the files which the extension is .csv
        * read the csv files and return a DuckDB DataFrame

      * Transform:
        * Run a SQL command to add a new column in a virtual table using DuckDB

      * Load:
        * Convert the DuckDB into Pandas
        * Save the Pandas DataFrame into a PostgresDB (DB which is in a cloud environment - Render)