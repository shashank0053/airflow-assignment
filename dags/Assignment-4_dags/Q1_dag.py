from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.utils.dates import days_ago
from airflow.providers.mysql.hooks.mysql import MySqlHook


dag = DAG(
    'ques4',
    default_args={
        'owner': 'airflow',
        'retries': 1,
    },
    description='A simple DAG to interact with MySQL',
    schedule_interval='@once',
    start_date=days_ago(1),
)


create_table = MySqlOperator(
    task_id='create_table',
    mysql_conn_id='mysql',
    sql="""
    CREATE TABLE IF NOT EXISTS users (
        id INT PRIMARY KEY AUTO_INCREMENT,
        name VARCHAR(50),
        age INT
    );
    """,
    dag=dag,
)


insert_values = MySqlOperator(
    task_id='insert_values',
    mysql_conn_id='mysql',
    sql="""
    INSERT INTO users (name, age)
    VALUES ('azure', 3), 
           ('gcp', 5),
           ('aws', 8);
    """,
    dag=dag,
)


def select_and_log():

    hook = MySqlHook(mysql_conn_id='mysql')
    sql = "SELECT * FROM users;"
    results = hook.get_records(sql)
    for row in results:
        print(f"User: {row[1]}, Age: {row[2]}")

select_values = PythonOperator(
    task_id='select_values',
    python_callable=select_and_log,
    dag=dag,
)


create_table >> insert_values >> select_values