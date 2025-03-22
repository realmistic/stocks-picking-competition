"""
Database connection and operations for the stocks picking competition.
Uses SQLite Cloud for remote database storage.
"""
import os
import pandas as pd
import numpy as np
import sqlitecloud
from dotenv import load_dotenv
from tqdm import tqdm

# Load environment variables from .env file if present
load_dotenv()

# Get connection URL from environment variable
SQLITECLOUD_URL = os.environ.get("SQLITECLOUD_URL")
if not SQLITECLOUD_URL:
    raise ValueError("SQLITECLOUD_URL environment variable is required")

def get_db_connection():
    """
    Get a connection to the SQLite Cloud database.
    
    Returns:
        SQLite Cloud connection object
    """
    try:
        # Connect to SQLite Cloud
        conn = sqlitecloud.connect(SQLITECLOUD_URL)
        return conn
    except Exception as e:
        print(f"Error connecting to SQLite Cloud: {str(e)}")
        raise

def get_db():
    """
    Get a database connection to SQLite Cloud.
    
    Returns:
        SQLite Cloud connection
    """
    return get_db_connection()

def init_db():
    """
    Initialize the database by creating all tables.
    
    Note: For SQLite Cloud, tables should be created using SQL statements.
    """
    print("Note: For SQLite Cloud, tables should be created using SQL statements")
    print("Run the create_tables() function to create tables in the remote database")

def execute_query(query, params=None):
    """
    Execute a SQL query on the SQLite Cloud database.
    
    Args:
        query: SQL query to execute
        params: Parameters for the query (optional)
        
    Returns:
        Query results as a pandas DataFrame
    """
    conn = get_db()
    try:
        cursor = conn.cursor()
        if params:
            cursor.execute(query, params)
        else:
            cursor.execute(query)
        
        # Get column names from cursor description
        if cursor.description:
            columns = [desc[0] for desc in cursor.description]
            data = cursor.fetchall()
            
            # Convert to pandas DataFrame
            df = pd.DataFrame(data, columns=columns)
            return df
        else:
            # Query didn't return any data (e.g., INSERT, UPDATE, DELETE)
            return None
    finally:
        conn.close()

def read_data(query):
    """
    Read data from the database using a SQL query.
    
    Args:
        query: SQL query to execute
        
    Returns:
        pandas DataFrame with the query results
    """
    return execute_query(query)

def write_data(df, table_name, if_exists='replace', batch_size=5000):
    """
    Write a pandas DataFrame to the database.
    
    Args:
        df: pandas DataFrame to write
        table_name: Name of the table to write to
        if_exists: What to do if the table exists ('replace', 'append', 'fail')
        batch_size: Number of rows to insert in each batch (default: 5000)
    """
    if df.empty:
        print("DataFrame is empty, nothing to write")
        return
    
    # Create a copy of the DataFrame to avoid modifying the original
    df_copy = df.copy()
    
    # Convert pandas Timestamp objects to ISO-formatted strings
    for col in df_copy.columns:
        if pd.api.types.is_datetime64_any_dtype(df_copy[col]):
            df_copy[col] = df_copy[col].astype(str)
        
        # Convert numpy int64/float64 to Python int/float
        elif pd.api.types.is_integer_dtype(df_copy[col]):
            df_copy[col] = df_copy[col].astype(int)
        elif pd.api.types.is_float_dtype(df_copy[col]):
            df_copy[col] = df_copy[col].astype(float)
    
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        
        # Check if table exists
        cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}'")
        table_exists = cursor.fetchone() is not None
        
        if table_exists and if_exists == 'replace':
            cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
            table_exists = False
        elif table_exists and if_exists == 'fail':
            raise ValueError(f"Table '{table_name}' already exists")
        
        # Create table if it doesn't exist
        if not table_exists:
            # Generate column definitions based on DataFrame dtypes
            columns = []
            for col in df_copy.columns:
                dtype = df_copy[col].dtype
                if pd.api.types.is_numeric_dtype(dtype):
                    if pd.api.types.is_integer_dtype(dtype):
                        sql_type = "INTEGER"
                    else:
                        sql_type = "REAL"
                else:
                    sql_type = "TEXT"
                columns.append(f"{col} {sql_type}")
            
            create_table_sql = f"CREATE TABLE {table_name} ({', '.join(columns)})"
            cursor.execute(create_table_sql)
        
        # Insert data
        placeholders = ", ".join(["?"] * len(df_copy.columns))
        insert_sql = f"INSERT INTO {table_name} VALUES ({placeholders})"
        
        # Convert DataFrame to list of tuples for insertion
        data = [tuple(row) for row in df_copy.values]
        
        # Insert in batches with progress bar
        total_batches = (len(data) + batch_size - 1) // batch_size  # Ceiling division
        print(f"Writing {len(df_copy)} rows to table {table_name} in {total_batches} batches...")
        
        with tqdm(total=len(data), desc=f"Writing to {table_name}", unit="rows") as pbar:
            for i in range(0, len(data), batch_size):
                batch = data[i:i+batch_size]
                cursor.executemany(insert_sql, batch)
                pbar.update(len(batch))
        
        conn.commit()
        print(f"Successfully wrote {len(df_copy)} rows to table {table_name}")
    finally:
        conn.close()

def create_tables():
    """
    Create tables in the SQLite Cloud database based on the models.
    """
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        
        # Create positions table
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS positions (
            id INTEGER PRIMARY KEY,
            name TEXT,
            ticker TEXT,
            exchange TEXT,
            weight REAL,
            formatted_ticker TEXT,
            shares REAL,
            price_at_start REAL,
            allocation_usd REAL,
            UNIQUE(name, ticker)
        )
        """)
        
        # Create daily_prices table
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS daily_prices (
            id INTEGER PRIMARY KEY,
            date TEXT,
            ticker TEXT,
            price REAL,
            UNIQUE(date, ticker)
        )
        """)
        
        # Create exchange_rates table
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS exchange_rates (
            id INTEGER PRIMARY KEY,
            date TEXT,
            from_currency TEXT,
            to_currency TEXT,
            rate REAL,
            UNIQUE(date, from_currency, to_currency)
        )
        """)
        
        # Create portfolio_values table
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS portfolio_values (
            id INTEGER PRIMARY KEY,
            date TEXT,
            name TEXT,
            value REAL,
            pct_change REAL,
            UNIQUE(date, name)
        )
        """)
        
        # Create performance_metrics table
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS performance_metrics (
            id INTEGER PRIMARY KEY,
            name TEXT UNIQUE,
            initial_value REAL,
            final_value REAL,
            total_return_pct REAL,
            annualized_return_pct REAL,
            last_updated TEXT
        )
        """)
        
        conn.commit()
        print("Tables created successfully")
    finally:
        conn.close()
