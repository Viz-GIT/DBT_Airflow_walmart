import psycopg2
import os
import csv

# Database connection parameters
db_host = "your_host"
db_port = "ypur_port"
db_name = "walmart_db"
db_user = "your_user"
db_password = "your_password"

# Function to create the connection string
def create_conn_string(host, port, dbname, user, password):
    return psycopg2.connect(
        host=host,
        port=port,
        dbname=dbname,
        user=user,
        password=password
    )

# CSV files mapping to tables
csv_files = {
    "customers.csv": "raw.customers",
    "stores.csv": "raw.stores",
    "products.csv": "raw.products",
    "employees.csv": "raw.employees",
    "orders.csv": "raw.orders",
    "order_items.csv": "raw.order_items",
}

data_dir = os.path.join(os.path.dirname(__file__), "data")

conn = None
try:
    # Connect to the database
    conn = create_conn_string(db_host, db_port, db_name, db_user, db_password)
    cursor = conn.cursor()
    
    # Load each CSV file into its corresponding table
    for csv_file, table_name in csv_files.items():
        csv_path = os.path.join(data_dir, csv_file)
        
        if os.path.exists(csv_path):
            print(f"Loading {csv_file} into {table_name}...")
            
            with open(csv_path, 'r') as f:
                cursor.copy_expert(f"COPY {table_name} FROM STDIN WITH (FORMAT CSV, HEADER TRUE)", f)
            
            conn.commit()
            print(f"✓ Successfully loaded {csv_file}")
        else:
            print(f"✗ File not found: {csv_path}")
    
    print("\n✓ All data loaded successfully!")
    
except Exception as e:
    print(f"Error: {e}")
    if conn is not None:
        conn.rollback()

finally:
    if conn is not None:
        conn.close()