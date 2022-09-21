from datetime import timedelta

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.mysql_operator import MySqlOperator
from airflow.utils.dates import datetime
from airflow.utils.dates import timedelta
from airflow.utils.dates import days_ago
from airflow.operators.email_operator import EmailOperator

default_args = {
    "owner": "Beka",
    #'start_date': airflow.utils.dates.days_ago(2),
    # 'end_date': datetime(),
    # 'depends_on_past': False,
    "email": ["bekiman21@gmail.com"],
    "email_on_failaure": False,
    #'email_on_retry': False,
    # If a task fails, retry it once after waiting
    # at least 5 minutes
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag_exec_scripts = DAG(
    dag_id="create_table_and_load_data",
    default_args=default_args,
    # schedule_interval='0 0 * * *',
    schedule_interval="@once",
    start_date=days_ago(1),
    dagrun_timeout=timedelta(minutes=60),
    description="executing dag scripts",
)

create_db = MySqlOperator(
    sql="sql/create_db.sql",
    task_id="create_db_task",
    mysql_conn_id="mysql_dwh",
    dag=dag_exec_scripts,
)

create_schema = MySqlOperator(
    sql="sql/create_schema.sql",
    task_id="create_schema_task",
    mysql_conn_id="mysql_dwh",
    dag=dag_exec_scripts,
)

create_table = MySqlOperator(
    sql="sql/create_table.sql",
    task_id="createtable_task",
    mysql_conn_id="mysql_dwh",
    dag=dag_exec_scripts,
)

load_data = MySqlOperator(
    sql="sql/load_data.sql",
    task_id="load_data_task",
    mysql_conn_id="mysql_dwh",
    dag=dag_exec_scripts,
)

email = EmailOperator(
    to="yidisam18@gmail.com",
    subject="Data migrated successfully",
    html_content="<h3>Data migrated successfully</h3>",
)


create_db >> create_schema >> create_table >> load_data >> email
