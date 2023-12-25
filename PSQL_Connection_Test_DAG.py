# This dag is to test psql conenction and list few lines of table
# Ran this dag from airflow docker and connecting to Postgresql database
# Reference to create a docker: https://sleek-data.blogspot.com/2023/09/how-to-install-airflow-on-windows.html

from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import psycopg2

def test_postgresql_connection():
    # PostgreSQL connection details
    db_name = "database_name"
    db_user = "database_user"
    db_password = "database_password"
    # db_host = "localhost"
    db_host = "host.docker.internal"
    db_port = "5432"
    table_name = "table_name"

    # Constructing the connection string
    conn_string = f"dbname={db_name} user={db_user} password={db_password} host={db_host} port={db_port}"

    # Initialize the connection variable outside the try block
    conn = None

    try:
        # Establishing a connection
        conn = psycopg2.connect(conn_string)
        print("Connected to the PostgreSQL database!")

        # Performing a simple query
        cursor = conn.cursor()
        cursor.execute(f"SELECT * FROM {table_name} LIMIT 5;")
        rows = cursor.fetchall()

        # Displaying the results
        print(f"First 5 rows from table '{table_name}':")
        for row in rows:
            print(row)

    except Exception as e:
        print(f"Error: {e}")

    finally:
        # Closing the cursor
        if cursor:
            cursor.close()

        # Closing the connection
        if conn:
            conn.close()
            print("Connection closed.")

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

# Creating the DAG
dag = DAG(
    'postgresql_connection_dag',
    default_args=default_args,
    description='A simple DAG to test PostgreSQL connection',
    schedule_interval=None,  # Set the desired schedule interval
)

# Creating a PythonOperator task to run the PostgreSQL connection test
run_postgresql_connection_task = PythonOperator(
    task_id='run_postgresql_connection',
    python_callable=test_postgresql_connection,
    dag=dag,
)

# This block is used to run the DAG directly when the script is executed
if __name__ == "__main__":
    dag.cli()
