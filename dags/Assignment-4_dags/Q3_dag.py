from airflow import DAG
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from airflow.utils.dates import days_ago
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator



SLACK_WEBHOOK_URL = "https://[placeholder]-hq.slack.com/archives/[placeholder]"

SLACK_CHANNEL = "#all-[placeholder]"


dag = DAG(
    'slack_notification_example',
    default_args={
        'owner': 'airflow',
    },
    description='DAG with Slack notifications',
    schedule_interval='@daily',
    start_date=days_ago(1),
)


def send_slack_message(context, task_instance, status):

    task_name = task_instance.task_id
    message = f"Task `{task_name}` {status.upper()} in DAG `{dag.dag_id}`"


    if status == "success":
        message = f":white_check_mark: {message} :white_check_mark: \nEverything completed successfully!"
    else:
        message = f":x: {message} :x: \nSomething went wrong!"


    slack_message = SlackWebhookOperator(
        task_id=f"slack_notify_{task_name}_{status}",
        webhook_token=SLACK_WEBHOOK_URL,
        message=message,
        channel=SLACK_CHANNEL,
        dag=dag
    )
    slack_message.execute(context=context)


start_task = EmptyOperator(
    task_id='start',
    dag=dag,
)


def sample_task():

    print("sample task")

task_to_monitor = PythonOperator(
    task_id='task_to_monitor',
    python_callable=sample_task,
    on_success_callback=lambda context, task_instance: send_slack_message(context, task_instance, "success"),
    on_failure_callback=lambda context, task_instance: send_slack_message(context, task_instance, "failure"),
    dag=dag,
)


start_task >> task_to_monitor

