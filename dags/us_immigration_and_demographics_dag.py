from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
import sys
sys.path.insert(0, '/Users/samar/airflow')
from plugins.custom_operators.data_quality import DataQualityOperator
from plugins.custom_operators.load_table import LoadTableOperator
from plugins.custom_operators.create_table import CreateTableOperator
from  plugins.helpers import SqlQueries, SQLCreateTables


default_args = {
    'owner': 'Samar Javed',
    'start_date': datetime(2022, 9, 21),
    'catchup': False,
    'depends_on_past': False,
    'retries': 1
}

dag = DAG('us_immigration_and_demographics_dag',
          default_args=default_args,
          description='Load and transform data in Postgres with Airflow',
          catchup=False,
          start_date=datetime(2022, 9, 21),
          schedule_interval='0 * * * *'
        )

start_operator = DummyOperator(task_id='Begin_execution', dag=dag)

load_staging_tables = BashOperator(
    task_id='Load_staging_tables',
    bash_command="python /Users/samar/airflow/us_immigration_and_demographics_staging.py",
    dag=dag
)

create_final_tables = CreateTableOperator(
    task_id="Create_final_tables",
    sql=SQLCreateTables.sql_create,
    postgres_conn_id="postgres",
    dag=dag
)

load_airport_codes_table = LoadTableOperator(
    task_id="Load_airport_codes_table",
    sql=SqlQueries.airport_codes_insert,
    postgres_conn_id="postgres",
    dag=dag,
    table="airport_codes"
)

load_demographics_table = LoadTableOperator(
    task_id='Load_demographics_table',
    dag=dag,
    table="demographics",
    postgres_conn_id="postgres",
    sql = SqlQueries.demographics_insert
)

load_global_temperature_table = LoadTableOperator(
    task_id='Load_global_temperature_table',
    dag=dag,
    table="global_temperature",
    postgres_conn_id="postgres",
    sql = SqlQueries.global_temperature_insert
)

load_i94address_table = LoadTableOperator(
    task_id='Load_i94address_table',
    dag=dag,
    table="i94address",
    postgres_conn_id="postgres",
    sql = SqlQueries.i94address_insert
)

load_i94country_table = LoadTableOperator(
    task_id='Load_i94country_table',
    dag=dag,
    table="i94country",
    postgres_conn_id="postgres",
    sql = SqlQueries.i94country_insert
)

load_i94mode_table = LoadTableOperator(
    task_id='Load_i94mode_table',
    dag=dag,
    table="i94mode",
    postgres_conn_id="postgres",
    sql = SqlQueries.i94mode_insert
)

load_i94port_table = LoadTableOperator(
    task_id='Load_i94port_table',
    dag=dag,
    table="i94port",
    postgres_conn_id="postgres",
    sql = SqlQueries.i94port_insert
)

load_i94visa_table = LoadTableOperator(
    task_id='Load_i94visa_table',
    dag=dag,
    table="i94visa",
    postgres_conn_id="postgres",
    sql = SqlQueries.i94visa_insert
)

load_immigration_table = LoadTableOperator(
    task_id='Load_immigration_table',
    dag=dag,
    table="immigration",
    postgres_conn_id="postgres",
    sql = SqlQueries.immigration_insert
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    postgres_conn_id="postgres",
    dq_checks=[
        {"sql_check": "SELECT COUNT(*) FROM immigration WHERE cicid is null", "expected_result": 0},
        {"sql_check": "SELECT COUNT(*) FROM demographics WHERE race is null", "expected_result": 0},
        {'sql_check': "SELECT COUNT(*) FROM i94address WHERE code is null", 'expected_result': 0},
        {'sql_check': "SELECT COUNT(*) FROM i94country WHERE code is null", 'expected_result': 0},
        {'sql_check': "SELECT COUNT(*) FROM i94port WHERE code is null", 'expected_result': 0},
        {'sql_check': "SELECT COUNT(*) FROM i94mode WHERE code is null", 'expected_result': 0},
        {'sql_check': "SELECT COUNT(*) FROM i94visa WHERE code is null", 'expected_result': 0}
    ]
)


end_operator = DummyOperator(task_id='Stop_execution', dag=dag)

load_operator = DummyOperator(task_id='Start_loading', dag=dag)

# Task Ordering:

start_operator >> load_staging_tables >> load_operator
start_operator >> create_final_tables >> load_operator
load_operator >> load_global_temperature_table
load_global_temperature_table >> load_airport_codes_table >> load_immigration_table
load_global_temperature_table >> load_demographics_table >> load_immigration_table
load_global_temperature_table >> load_i94address_table >> load_immigration_table
load_global_temperature_table >> load_i94country_table >> load_immigration_table
load_global_temperature_table >> load_i94mode_table >> load_immigration_table
load_global_temperature_table >> load_i94port_table >> load_immigration_table
load_global_temperature_table >> load_i94visa_table >> load_immigration_table
load_immigration_table >> run_quality_checks >> end_operator


