import pandas as pd
import mysql.connector
import configparser

# Load configuration from config.ini
config = configparser.ConfigParser()
config.read('config.ini')

# Get MySQL connection details from the config file
mysql_config = {
    'host': config['mysql']['host'],
    'port': config['mysql']['port'],
    'user': config['mysql']['user'],
    'password': config['mysql']['password'],
    'database': config['mysql']['database'],
    'raise_on_warnings': True
}

# CSV file path
csv_file = 'C:\Users\siva\OneDrive\Documents\Databricks\customers.csv'

# Read the CSV file into a pandas DataFrame
df = pd.read_csv(csv_file)

# Connect to the MySQL database
try:
    conn = mysql.connector.connect(**mysql_config)
    cursor = conn.cursor()

    # Create a table (if it doesn't exist)
    create_table_query = """
    CREATE TABLE IF NOT EXISTS customers (
        CustomerID INT PRIMARY KEY,
        Name VARCHAR(255),
        Email VARCHAR(255),
        Age INT,
        City VARCHAR(255)
    );
    """
    cursor.execute(create_table_query)
    print("Table created or already exists.")

    # Insert data into the MySQL table
    for _, row in df.iterrows():
        insert_query = """
        INSERT INTO customers (CustomerID, Name, Email, Age, City)
        VALUES (%s, %s, %s, %s, %s);
        """
        cursor.execute(insert_query, tuple(row))

    # Commit the transaction
    conn.commit()
    print("Data inserted successfully.")

except mysql.connector.Error as err:
    print(f"Error: {err}")

finally:
    # Close the database connection
    if conn.is_connected():
        cursor.close()
        conn.close()
        print("MySQL connection is closed.")