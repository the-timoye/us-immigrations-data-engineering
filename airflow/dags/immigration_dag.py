from datetime import datetime
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
import configparser
from operators.StagTablesOperator import StageTablesOperator
from plugins import model_data
from sql.create_tables import CreateTables
from sql.insert_data import InsertQueries
from airflow.utils.task_group import TaskGroup
from helpers.data_quality import (data_count_check, null_value_check, column_type_check)

config = configparser.ConfigParser();
config.read('config.cfg')

start_date = datetime(2021, 7, 14, 0,0,0)
default_args = {
    'owner': 'Timi',
    'depends_on_past': False,
    'email_on_failure': False
}

with DAG (
    'immigration_dag',
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

    with TaskGroup ("stage_immigration_table") as stage_immigration_table:
        create_immigration_tables = PostgresOperator(
            task_id = f"create_staging_immigrations_table",
            dag=dag,
            postgres_conn_id="redshift_conn_id",
            sql=CreateTables.staging_immigrations,
        )
        copy_immigration_data_to_redshift_table = StageTablesOperator(
            task_id = f"stage_staging_immigrations_table",
            table = "staging_immigrations",
            dag=dag,
            s3_bucket = config['S3']['BUCKET'],
            s3_key= config['S3']['IMMIGRATION_KEY'],
            ignore_headers = 1,
            data_format='PARQUET'
        )
    with TaskGroup ("stage_cities_table") as stage_cities_table:
        create_immigration_tables = PostgresOperator(
            task_id = f"create_staging_cities_table",
            dag=dag,
            postgres_conn_id="redshift_conn_id",
            sql=CreateTables.staging_cities,
        )
        copy_cities_data_to_redshift_table = StageTablesOperator(
            task_id = f"stage_staging_cities_table",
            table = "staging_cities",
            dag=dag,
            s3_bucket = config['S3']['BUCKET'],
            s3_key= config['S3']['CITIES_KEY'],
            ignore_headers = 1,
            data_format='PARQUET'
        )
    with TaskGroup ("load_cities_facts") as load_cities_facts:
        create_tables = PostgresOperator(
            task_id = f"create_us_geography_table",
            dag=dag,
            default_args = default_args,
            postgres_conn_id="redshift_conn_id",
            sql=CreateTables.us_geography
        )

        insert_into_tables = PostgresOperator(
            task_id = f"insert_into_us_geography_table",
            dag=dag,
            default_args = default_args,
            postgres_conn_id="redshift_conn_id",
            sql=f"""
                INSERT INTO us_geography (
                    {InsertQueries.us_geography}
                );
            """
        )
    with TaskGroup ("load_immigration_facts") as load_immigration_facts:
        create_tables = PostgresOperator(
            task_id = f"create_immigrants_facts_table",
            dag=dag,
            default_args = default_args,
            postgres_conn_id="redshift_conn_id",
            sql=CreateTables.immigrants_facts
        )

        insert_into_tables = PostgresOperator(
            task_id = f"insert_into_immigrants_facts_table",
            dag=dag,
            default_args = default_args,
            postgres_conn_id="redshift_conn_id",
            sql=f"""
                INSERT INTO immigrants_facts (
                    {InsertQueries.immigrations_facts}
                );
            """
        )

    tasks_break = DummyOperator (
        task_id = "Break_Session"
    )

    with TaskGroup("create_dimentions_table") as create_dimentions_table:
        create_immigrants = PostgresOperator(
            task_id = f"create_immigrants",
            dag=dag,
            default_args = default_args,
            postgres_conn_id="redshift_conn_id",
            sql=CreateTables.immigrants
        )
        create_us_cities = PostgresOperator(
            task_id = f"create_us_cities",
            dag=dag,
            default_args = default_args,
            postgres_conn_id="redshift_conn_id",
            sql=CreateTables.us_cities
        )
        create_us_states = PostgresOperator(
            task_id = f"create_us_states",
            dag=dag,
            default_args = default_args,
            postgres_conn_id="redshift_conn_id",
            sql=CreateTables.us_states
        )
        create_visa_types = PostgresOperator(
            task_id = f"create_visa_types",
            dag=dag,
            default_args = default_args,
            postgres_conn_id="redshift_conn_id",
            sql=CreateTables.visa_types
        )
        create_transport_modes = PostgresOperator(
            task_id = f"create_transport_modes",
            dag=dag,
            default_args = default_args,
            postgres_conn_id="redshift_conn_id",
            sql=CreateTables.transport_modes
        )
        create_travel_info = PostgresOperator(
            task_id = f"create_travel_info",
            dag=dag,
            default_args = default_args,
            postgres_conn_id="redshift_conn_id",
            sql=CreateTables.travel_info
        )
    with TaskGroup("insert_into_dimensions_tables") as insert_into_dimensions_tables:
        insert_into_us_cities_table = PostgresOperator(
            task_id = f"insert_into_us_cities_table",
            dag=dag,
            default_args = default_args,
            postgres_conn_id="redshift_conn_id",
            sql=f"""
                INSERT INTO us_cities (
                    {InsertQueries.us_cities}
                );
            """
        )
        insert_into_us_states_table = PostgresOperator(
            task_id = f"insert_into_us_states_table",
            dag=dag,
            default_args = default_args,
            postgres_conn_id="redshift_conn_id",
            sql=f"""
                INSERT INTO us_states (
                    {InsertQueries.us_states}
                );
            """
        )
        insert_into_visa_types_table = PostgresOperator(
            task_id = f"insert_into_visa_types_table",
            dag=dag,
            default_args = default_args,
            postgres_conn_id="redshift_conn_id",
            sql=f"""
                INSERT INTO visa_types (
                    {InsertQueries.visa_types}
                );
            """
        )
        insert_into_transport_modes_table = PostgresOperator(
            task_id = f"insert_into_transport_modes_table",
            dag=dag,
            default_args = default_args,
            postgres_conn_id="redshift_conn_id",
            sql=f"""
                INSERT INTO transport_modes (
                    {InsertQueries.transport_modes}
                );
            """
        )
        insert_into_travel_info_table = PostgresOperator(
            task_id = f"insert_into_travel_info_table",
            dag=dag,
            default_args = default_args,
            postgres_conn_id="redshift_conn_id",
            sql=f"""
                INSERT INTO travel_info (
                    {InsertQueries.travels_info}
                );
            """
        )
        insert_into_immigrants_table = PostgresOperator(
            task_id = f"insert_into_immigrants_table",
            dag=dag,
            default_args = default_args,
            postgres_conn_id="redshift_conn_id",
            sql=f"""
                INSERT INTO immigrants (
                    {InsertQueries.immigrants}
                );
            """
        )

    with TaskGroup("run_quality_checks") as run_quality_checks:
        staging_immigrations_data_count_check = PythonOperator(
            task_id = f"staging_immigrations_data_count_check",
            dag=dag,
            provide_context=True,
            python_callable=data_count_check,
            params = {
                "table": "staging_immigrations",
                "expected_row_count": 1410013
            }
        )
        staging_cities_data_count_check = PythonOperator(
            task_id = f"staging_cities_data_count_check",
            dag=dag,
            provide_context=True,
            python_callable=data_count_check,
            params = {
                "table": "staging_cities",
                "expected_row_count": 2891
            }
        )
        staging_cities_null_values_check = PythonOperator(
            task_id = f"staging_cities_null_values_check",
            dag=dag,
            provide_context=True,
            python_callable=null_value_check,
            params = {
                "table": "staging_cities",
                "column_name": "city"
            }
        )
        staging_immigrations_null_values_check = PythonOperator(
            task_id = f"staging_immigrations_null_values_check",
            dag=dag,
            provide_context=True,
            python_callable=null_value_check,
            params = {
                "table": "staging_immigrations",
                "column_name": "birth_year"
            }
        )
        staging_immigrations_column_type_check = PythonOperator(
            task_id = f"staging_immigrations_column_type_check",
            dag=dag,
            provide_context=True,
            python_callable=column_type_check,
            params = {
                "table": "staging_immigrations",
                "column_name": "arrival_date",
                "data_type": "date"
            }
        )
        staging_cities_column_type_check = PythonOperator(
            task_id = f"staging_cities_column_type_check",
            dag=dag,
            provide_context=True,
            python_callable=column_type_check,
            params = {
                "table": "staging_cities",
                "column_name": "total_population",
                "data_type": "integer"
            }
        )

    end_execution = DummyOperator (
        task_id = "End_Execution"
    )

begin_execution >> build_model
build_model >> stage_immigration_table
build_model >> stage_cities_table
stage_immigration_table >> load_immigration_facts
stage_cities_table >> load_cities_facts
[load_immigration_facts, load_cities_facts] >> tasks_break
create_dimentions_table << tasks_break
create_dimentions_table >> insert_into_dimensions_tables
insert_into_dimensions_tables >> run_quality_checks
run_quality_checks >> end_execution
