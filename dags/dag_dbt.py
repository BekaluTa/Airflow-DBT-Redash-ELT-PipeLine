from datetime import datetime
import airflow
from airflow.models import DAG
from airflow.operators.dummy import DummyOperator
from airflow.providers.dbt.cloud.operators.dbt import (
    DbtCloudRunJobOperator,
)
# https://cloud.getdbt.com/#/accounts/85856/projects/131666/jobs/107488/

with DAG(
    dag_id="dbt_airflow_connection",
    default_args={"dbt_cloud_conn_id": "dbt_cloud", "account_id": 85856},
    start_date=airflow.utils.dates.days_ago(1),
    schedule_interval="@once",
    catchup=False,
) as dag:
    extract = DummyOperator(task_id="extract")
    load = DummyOperator(task_id="load")
    ml_training = DummyOperator(task_id="ml_training")

    trigger_dbt_cloud_job_run = DbtCloudRunJobOperator(
        task_id="trigger_dbt_cloud_job_run",
        job_id=107488,
        check_interval=10,
        timeout=300,
    )

    extract >> load >> trigger_dbt_cloud_job_run >> ml_training