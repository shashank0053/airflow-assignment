from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

# Define the function to print the message
def print_hello():
    print("Hello Airflow")

# Default arguments for the DAG
default_args = {
    'start_date': datetime(2024, 12, 5),
    'catchup': False,  # Ensures the DAG doesn't backfill
}

# Create the DAG
with DAG(
    dag_id='hello_airflow_demo',
    default_args=default_args,
    schedule_interval=None,  # No automatic scheduling
    tags=['demo'],  # Tag the DAG
) as dag:

    # Define the task
    hello_task = PythonOperator(
        task_id='print_hello_task',
        python_callable=print_hello,  # Call the print_hello function
        retries=0  # Ensures no retries, so it always appears successful
    )

