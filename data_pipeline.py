import pandas as pd
import mysql.connector
import configparser

# Load configuration from config.ini
config = configparser.ConfigParser()
config.read('config.ini')

# Get MySQL connection details from the config file
mysql_config = {
    'host': config['mysql']['host'],
    'port': int(config['mysql']['port']),
    'user': config['mysql']['user'],
    'password': config['mysql']['password'],
    'database': config['mysql']['database'],
    'raise_on_warnings': True
}

# File path for CSV data
csv_file = 'data/order_data.csv'

# Initialize connection as None
conn = None

# Load order data from CSV
order_df = pd.read_csv(csv_file)

# Connect to the MySQL database
try:
    # Establish MySQL connection
    conn = mysql.connector.connect(**mysql_config)
    cursor = conn.cursor()

    # Fetch customer data from MySQL
    query = "SELECT CustomerID, Name, Email, Age, City FROM customers;"
    customer_df = pd.read_sql(query, conn)

    # Perform an inner join between customer data and order data on 'CustomerID'
    merged_df = pd.merge(customer_df, order_df, on='CustomerID', how='inner')

    # Print out the merged data (optional for debugging)
    print("Merged DataFrame:")
    print(merged_df)

    # Create a new table to store the merged data
    create_table_query = """
    CREATE TABLE IF NOT EXISTS customer_orders (
        CustomerID INT,
        Name VARCHAR(255),
        Email VARCHAR(255),
        Age INT,
        City VARCHAR(255),
        OrderID INT PRIMARY KEY,
        Product VARCHAR(255),
        Quantity INT,
        OrderDate DATE
    );
    """
    cursor.execute(create_table_query)
    print("Table 'customer_orders' created or already exists.")

    # Insert the merged data into the new MySQL table
    insert_query = """
    INSERT INTO customer_orders (CustomerID, Name, Email, Age, City, OrderID, Product, Quantity, OrderDate)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
    """

    # Convert the merged dataframe to a list of tuples for insertion
    data_to_insert = [tuple(row) for row in merged_df.to_records(index=False)]

    # Insert the data using executemany for batch processing
    cursor.executemany(insert_query, data_to_insert)

    # Commit the transaction
    conn.commit()
    print(f"Data inserted successfully. {len(data_to_insert)} rows inserted.")

except mysql.connector.Error as err:
    print(f"Error: {err}")

finally:
    # Close the database connection
    if conn and conn.is_connected():
        cursor.close()
        conn.close()
        print("MySQL connection is closed.")
