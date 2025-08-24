from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import psycopg2
import logging

# Database connection
conn_params = {
    "dbname": "practice",
    "user": "postgres",
    "password": "admin",
    "host": "localhost",
    "port": "5432"
}

# Logging setup
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- ETL Functions ---
def extract(**kwargs):
    try:
        connection = psycopg2.connect(**conn_params)
        cursor = connection.cursor()
        query = """
            SELECT list_item, CURRENT_DATE AS report_date, SUM(sales) AS total_sales, SUM(orders) AS total_orders
            FROM test_table
            GROUP BY list_item, CURRENT_DATE
            ORDER BY total_sales DESC;
        """
        cursor.execute(query)
        data = cursor.fetchall()
        logging.info(f"Extracted {len(data)} rows")
        cursor.close()
        connection.close()
        return data
    except Exception as e:
        logging.error(f"Error in extract: {e}")
        return []

def transform(**kwargs):
    ti = kwargs['ti']
    data = ti.xcom_pull(task_ids='extract_task')
    transformed = []
    for row in data:
        transformed.append({
            'list_item': row[0],
            'report_date': row[1],
            'total_sales': row[2],
            'total_orders': row[3]
        })
    logging.info(f"Transformed {len(transformed)} rows")
    return transformed

def load(**kwargs):
    ti = kwargs['ti']
    transformed = ti.xcom_pull(task_ids='transform_task')
    try:
        connection = psycopg2.connect(**conn_params)
        cursor = connection.cursor()
        for row in transformed:
            cursor.execute("""
                INSERT INTO reporting_table (list_item, report_date, total_sales, total_orders)
                VALUES (%s, %s, %s, %s)
            """, (row['list_item'], row['report_date'], row['total_sales'], row['total_orders']))
        connection.commit()
        cursor.close()
        connection.close()
        logging.info("Load completed successfully")
    except Exception as e:
        logging.error(f"Error in load: {e}")

# --- DAG Setup ---
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

with DAG(
    'etl_reporting_dag',
    default_args=default_args,
    description='ETL pipeline for reporting_table every 5 minutes',
    schedule_interval='*/5 * * * *',  # every 5 minutes
    start_date=datetime(2025, 8, 24),
    catchup=False
) as dag:

    extract_task = PythonOperator(
        task_id='extract_task',
        python_callable=extract,
        provide_context=True
    )

    transform_task = PythonOperator(
        task_id='transform_task',
        python_callable=transform,
        provide_context=True
    )

    load_task = PythonOperator(
        task_id='load_task',
        python_callable=load,
        provide_context=True
    )

    extract_task >> transform_task >> load_task
