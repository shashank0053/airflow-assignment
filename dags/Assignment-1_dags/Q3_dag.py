from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime

# Task to set the variable
def set_variable():
    Variable.set("hi i am a variable")
    print("Variable 'My_name' has been set.")

# Task to read the variable
def read_variable():
    var = Variable.get("My_name", default_var="Default Value")
    print(f"The variable value is: {var}")

# Default arguments for the DAG
default_args = {
    'start_date': datetime(2024, 12, 5),
    'catchup': False,  # Prevent backfilling
}

# Define the DAG
with DAG(
    dag_id='set_and_read_airflow_variable',
    default_args=default_args,
    schedule_interval=None,  # Manually triggered
    tags=['demo'],
) as dag:

    # Task to set the variable
    set_variable_task = PythonOperator(
        task_id='set_variable',
        python_callable=set_variable,
    )

    # Task to read the variable
    read_variable_task = PythonOperator(
        task_id='read_variable',
        python_callable=read_variable,
    )

    # Define task dependencies
    set_variable_task >> read_variable_task
