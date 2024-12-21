import logging
from sqlalchemy import create_engine
from sqlalchemy.engine import URL
import pandas as pd
import os

# Setup logging
logging.basicConfig(
    filename="etl_process.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

def get_credentials():
    try:
        pwd = os.environ['PGPASS']
        uid = os.environ['PGUID']
        return uid, pwd
    except KeyError as e:
        logging.error(f"Missing environment variable: {e}")
        raise

# Database details
def get_sql_server_connection():
    driver = "{SQL Server Native Client 11.0}"
    server = "localhost"
    database = "AdventureWorksDW2019"
    
    try:
        uid, pwd = get_credentials()
        connection_string = f"DRIVER={driver};SERVER={server};DATABASE={database};UID={uid};PWD={pwd}"
        connection_url = URL.create("mssql+pyodbc", query={"odbc_connect": connection_string})
        return create_engine(connection_url)
    except Exception as e:
        logging.error(f"Error creating SQL Server connection: {e}")
        raise

def get_postgres_connection():
    try:
        uid, pwd = get_credentials()
        server = "localhost"  # Update as needed
        engine = create_engine(f"postgresql://{uid}:{pwd}@{server}:5432/adventureworks")
        return engine
    except Exception as e:
        logging.error(f"Error creating PostgreSQL connection: {e}")
        raise

def extract():
    try:
        logging.info("Starting data extraction")
        src_engine = get_sql_server_connection()
        query = """
            SELECT t.name AS table_name
            FROM sys.tables t
            WHERE t.name IN ('DimProduct', 'DimProductSubcategory', 'DimProductCategory', 'DimSalesTerritory', 'FactInternetSales')
        """
        with src_engine.connect() as conn:
            src_tables = pd.read_sql_query(query, conn).to_dict()["table_name"]

        for id in src_tables:
            table_name = src_tables[id]
            logging.info(f"Extracting data from table: {table_name}")
            df = pd.read_sql_query(f"SELECT * FROM {table_name}", conn)
            load(df, table_name)

        logging.info("Data extraction completed successfully")

    except Exception as e:
        logging.error(f"Data extraction error: {e}")
        raise

def load(df, tbl):
    try:
        logging.info(f"Starting data load for table: {tbl}")
        tgt_engine = get_postgres_connection()
        with tgt_engine.begin() as conn:
            df.to_sql(f"stg_{tbl}", conn, if_exists="replace", index=False, chunksize=100000)
        logging.info(f"Data successfully loaded into table: stg_{tbl}")
    except Exception as e:
        logging.error(f"Data load error for table {tbl}: {e}")
        raise

if __name__ == "__main__":
    try:
        extract()
    except Exception as e:
        logging.critical(f"ETL process failed: {e}")
