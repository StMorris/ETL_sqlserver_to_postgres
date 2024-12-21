# ETL_sqlserver_to_postgres

### The ETL file extracts data from a SQL Server database to a postgres database. The staging table where the data is loaded, acts as a holding area where raw or intermediate data is loaded before further processing or transformation.

## A step-by-step explanation of what the ETL script does:

---

### **1. Setup Logging**
- **Purpose**: Logs are written to `etl_process.log` to track the ETL process and provide detailed information on successes, failures, and errors.
- **Details**: Each log entry includes the timestamp, log level (INFO, ERROR, CRITICAL), and a descriptive message.

---

### **2. Secure Credential Handling**
- Retrieves the PostgreSQL credentials (`PGUID` and `PGPASS`) from environment variables via the `get_credentials()` function.
- **Error Handling**: Logs an error and raises an exception if the credentials are missing.

---

### **3. Establish SQL Server Connection**
- **Purpose**: Establishes a connection to the SQL Server database using the `get_sql_server_connection()` function.
- Constructs the connection string dynamically with credentials and database details.
- **Error Handling**: Logs any errors encountered during connection setup.

---

### **4. Establish PostgreSQL Connection**
- **Purpose**: Establishes a connection to the PostgreSQL database using the `get_postgres_connection()` function.
- Constructs the PostgreSQL connection string with credentials and database details.
- **Error Handling**: Logs any errors during connection creation.

---

### **5. Extract Data (`extract()` Function)**
- **Query for Table Names**: Extracts a list of specific table names (`DimProduct`, `DimProductSubcategory`, etc.) from the SQL Server database.
- **Iterate Through Tables**: 
  - For each table, queries all rows using Pandas’ `read_sql_query` and loads the data into a DataFrame.
  - Passes the DataFrame and table name to the `load()` function.
- **Logging**: Logs progress and errors during the extraction process.

---

### **6. Load Data (`load()` Function)**
- **Purpose**: Loads the extracted data into a staging table in the PostgreSQL database (`stg_<table_name>`).
- **Chunked Loading**: Uses a `chunksize` of 100,000 rows for efficient loading.
- **Table Management**: Replaces the table in PostgreSQL if it already exists (`if_exists='replace'`).
- **Logging**: Logs progress and errors during the loading process.

---

### **7. Main Execution (`__main__`)**
- Calls the `extract()` function within a try-except block.
- Logs a critical error if the entire ETL process fails.

