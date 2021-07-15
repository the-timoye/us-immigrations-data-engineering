from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import task
from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator
import configparser
from textwrap import dedent
from plugins import model_data

config = configparser.ConfigParser();
config.read('config.cfg')

start_date = datetime(2021, 6, 30, 0, 0, 0)
default_args = {
    'owner': 'Timi',
    'depends_on_past': False,
    'email_on_failure': False
}


with DAG (
    'immi',
    default_args = default_args,
    description = """
        Includes tasks that migrate staging immigration & us-cities data from S3 to Redshift,
        and creates Dimentions and facts tables for easy analysis.
        This DAG retries failed tasks once after 15 minutes, and emails the admin on failure.
    """,
    schedule_interval='@daily',
    start_date=start_date,
) as dag:
    begin_execution = DummyOperator(
        task_id = "Begin_execution", dag = dag
    )
    build_model = PythonOperator(
        task_id="model_data",
        python_callable=model_data,
    )
    end_execution = DummyOperator (
        task_id = "End_Execution"
    )

begin_execution >> build_model
# create tables on redshift
# move staging data from  s3 to redshift
# insert data into other tables on redshift
# check the number of rows on the staging tables
# run some sql queries
# end execution
build_model >> end_execution