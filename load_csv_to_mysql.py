import pandas as pd
import mysql.connector
from mysql.connector import Error
import sys
from typing import Optional
import configparser

def read_db_config(config_file='config.ini', section='mysql'):
    """Read database configuration from config.ini file."""
    try:
        config = configparser.ConfigParser()
        config.read(config_file)
        
        if section not in config:
            raise Exception(f"Section {section} not found in {config_file}")
            
        return {
            'host': config[section]['host'],
            'user': config[section]['user'],
            'password': config[section]['password'],
            'database': config[section]['database'],
            'port': config[section].getint('port', 3306)  # Default port if not specified
        }
    except Exception as e:
        print(f"Error reading config file: {e}")
        sys.exit(1)

def create_connection(
    host: str,
    user: str,
    password: str,
    database: str,
    port: int
) -> Optional[mysql.connector.MySQLConnection]:
    """Create a connection to MySQL database."""
    try:
        connection = mysql.connector.connect(
            host=host,
            user=user,
            password=password,
            database=database,
            port=port
        )
        print("Successfully connected to MySQL database")
        return connection
    except Error as e:
        print(f"Error connecting to MySQL: {e}")
        return None

def load_csv_to_mysql(
    connection: mysql.connector.MySQLConnection,
    csv_path: str,
    table_name: str
) -> bool:
    """Load CSV file into MySQL table."""
    try:
        # Read CSV file
        df = pd.read_csv(csv_path)
        cursor = connection.cursor()

        # Create table if it doesn't exist
        columns = ", ".join([f"`{col}` VARCHAR(255)" for col in df.columns])
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            id INT AUTO_INCREMENT PRIMARY KEY,
            {columns}
        )
        """
        cursor.execute(create_table_query)
        
        # Prepare insert query
        placeholders = ", ".join(["%s"] * len(df.columns))
        columns = ", ".join([f"`{col}`" for col in df.columns])
        insert_query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
        
        # Convert DataFrame to list of tuples for insertion
        values = [tuple(row) for row in df.values]
        
        # Insert data in batches
        batch_size = 1000
        for i in range(0, len(values), batch_size):
            batch = values[i:i + batch_size]
            cursor.executemany(insert_query, batch)
            connection.commit()
            print(f"Inserted rows {i} to {min(i + batch_size, len(values))}")
        
        print(f"Successfully loaded {len(df)} rows into {table_name}")
        return True

    except Error as e:
        print(f"Error: {e}")
        return False
    finally:
        if cursor:
            cursor.close()

def main():
    # Read database configuration from config.ini
    db_config = read_db_config()
    
    # CSV file path and table name
    CSV_PATH = r'C:\Users\siva\OneDrive\Documents\Databricks\customers_data.csv'  # Change this to your CSV file path
    TABLE_NAME = 'customers'  # Change this to your desired table name
    
    # Create database connection
    connection = create_connection(**db_config)
    
    if not connection:
        print("Failed to connect to database. Exiting...")
        sys.exit(1)
    
    try:
        # Load CSV data to MySQL
        success = load_csv_to_mysql(connection, CSV_PATH, TABLE_NAME)
        if success:
            print("Data loading completed successfully")
        else:
            print("Failed to load data")
    
    finally:
        if connection and connection.is_connected():
            connection.close()
            print("Database connection closed")

if __name__ == "__main__":
    main()