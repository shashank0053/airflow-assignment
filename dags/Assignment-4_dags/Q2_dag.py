from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.utils.email import send_email


dag = DAG(
    'email_notification_example',
    default_args={
        'owner': 'airflow',
        'retries': 1,
        'email_on_failure': True,
        'email_on_retry': True,
        'email': ['k.venkatashashank@gmail.com'],
    },
    description='DAG with email notifications on failure or retries',
    schedule_interval='@daily',
    start_date=days_ago(1),
)


def fail_task():
    raise Exception('This is a failed task')


empty_task = EmptyOperator(
    task_id='empty_task',
    dag=dag,
)


fail_task_operator = PythonOperator(
    task_id='fail_task_operator',
    python_callable=fail_task,
    dag=dag,
)

def failure_callback(context):
    try:
        send_email(
            to='deadpool.arjit1209@gmail.com',  # Corrected email format
            subject=f"Airflow Task Failed: {context['task_instance'].task_id}",
            html_content=f"Task {context['task_instance'].task_id} failed with error: {context['exception']}"
        )
    except Exception as e:
        print(f"Error while sending email: {e}")

fail_task_operator.on_failure_callback = failure_callback


empty_task >> fail_task_operator