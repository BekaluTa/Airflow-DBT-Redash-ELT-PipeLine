from datetime import timedelta

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import datetime
from airflow.utils.dates import timedelta
from airflow.utils.dates import days_ago

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
    dag_id="dbt_exec_scripts",
    default_args=default_args,
    # schedule_interval='0 0 * * *',
    schedule_interval="@daily",
    start_date=days_ago(1),
    dagrun_timeout=timedelta(minutes=60),
    description="executing dag scripts",
)


dbt_run = BashOperator(task_id="dbt_run", bash_command="dbt run", dag=dag_exec_scripts)

dbt_test = BashOperator(
    task_id="dbt_test", bash_command="dbt test", dag=dag_exec_scripts
)

dbt_run >> dbt_test
