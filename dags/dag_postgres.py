from sqlalchemy import create_engine
import pandas as pd
import os
from datetime import timedelta,datetime
import airflow
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator

default_args={
    'owner':'martinluther',
    'retries':5,
    'retry_delay':timedelta(minutes=2)
}


with DAG(
    dag_id="dag_postgres",
    start_date=airflow.utils.dates.days_ago(1),
    schedule_interval="@once",
    catchup=False,
) as dag:
    
    # [END postgres_operator_howto_guide_create_pet_table]
    # [START postgres_operator_howto_guide_populate_pet_table]
    # [END postgres_operator_howto_guide_populate_pet_table]
    # [START postgres_operator_howto_guide_get_all_pets]
    get_all_traffic= PostgresOperator(
        task_id="get_all_traffic", 
        postgres_conn_id="postgres_traffic",
        sql="SELECT * FROM endpoints_trafficinfo;"
    )
    # [END postgres_operator_howto_guide_get_all_pets]
    get_all_traffic 
    # [END postgres_operator_howto_guide]

    