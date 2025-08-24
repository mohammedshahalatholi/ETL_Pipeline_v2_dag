import psycopg2
import logging
import os
import datetime


conn = {
    "dbname": "practice",
    "user": "postgres",
    "password": "admin",
    "host": "localhost",
    "port": "5432"
}

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',filename='etl_v2.log')

#get data from data base
connection = psycopg2.connect(**conn)
logging.info("Database connection established")
cursor = connection.cursor()
logging.info("Cursor created")
def extract_data():
    try:
        cursor.execute("SELECT list_item, SUM(sales) AS total_sales, SUM(orders) AS total_orders FROM test_table GROUP BY list_item ORDER BY total_sales DESC;")
        data = cursor.fetchall()
        print (data)
        logging.info(f"Data extracted successfully: {data}")
        return data
    except Exception as e:
        logging.error(f"Error extracting data: {e}")
        return []
def transform_data(data):
    transformed = []
    for row in data:
        transformed_row = {idx: value for idx, value in zip(['list_item', 'total_sales', 'total_orders'], row)}
        transformed.append(transformed_row)
        print(transformed_row)
    logging.info(f"Data transformed successfully: {transformed_row}")
    return transformed
def load(data):
    try:
        for row in data:
            
            cursor.execute("INSERT INTO reporting_table (list_item, total_sales, total_orders) VALUES (%s, %s, %s)", (row['list_item'], row['total_sales'], row['total_orders']))
        connection.commit()
        logging.info("Data loaded successfully")
    except Exception as e:
        logging.error(f"Error loading data: {e}")

if __name__ == "__main__":
    data = extract_data()
    transformed = transform_data(data)
    load_data = load(transformed)
