import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime

def send_alert():
    Variable.set("Alert_message", "Hello, this is a message from Airflow!")
    var = Variable.get("Alert_message", default_var="Default Alert")
    webhook_url = "https://chat.googleapis.com/v1/spaces/AAAA9RDskoI/messages?key=AIzaSyDdI0hCZtE6vySjMm-WEfRq3CPzqKqqsHI&token=gvP2DoV6A7DQ6I1XPmSynrXd7D9-N6GE0dInwFw7Sno"
    message = {"text": var}
    response = requests.post(webhook_url, json=message)
    if response.status_code == 200:
        print("Message sent successfully!")
    else:
        print(f"Failed to send message: {response.status_code}")

default_args = {
    'start_date': datetime(2024, 12, 5),
    'catchup': False,
}

with DAG(
    dag_id='send_alert_google_chat',
    default_args=default_args,
    schedule_interval=None,
    tags=['demo']
) as dag:
    task = PythonOperator(
        task_id='send_alert',
        python_callable=send_alert
    )
