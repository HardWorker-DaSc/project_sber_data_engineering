import os
import sys

from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup

from datetime import datetime


path_general = os.path.expanduser('~/project_sbr')
os.environ['PROJECT_SBER_PATH'] = path_general
sys.path.insert(0, path_general)
path = os.environ.get('PROJECT_SBER_PATH')


from modules.load_column_table import load_column_table
from modules.load_new_table import load_new_table
from modules.load_pkl_to_db_new_table import load_pkl_to_db_new_table

from modules.processing_ga_hits_pkl import processing_ga_hits_pkl
from modules.processing_ga_sessions_pkl import processing_ga_sessions_pkl
from modules.processing_new_hits import processing_new_hits
from modules.processing_new_sessions import processing_new_sessions

from modules.sql_join_new_processing_df_in_db_table import sql_join_new_processing_df_in_db_table


keys_check_sessions = [
    'session_id',
    'client_id',
    'visit_date',
    'visit_time',
    'visit_number',
    'utm_source',
    'utm_medium',
    'utm_campaign',
    'utm_adcontent',
    'utm_keyword',
    'device_category',
    'device_os',
    'device_brand',
    'device_model',
    'device_screen_resolution',
    'device_browser',
    'geo_country',
    'geo_city',
]
keys_check_hits = [
    'session_id',
    'hit_date',
    'hit_time',
    'hit_number',
    'hit_type',
    'hit_referer',
    'hit_page_path',
    'event_category',
    'event_action',
    'event_label',
    'event_value'
]

sql_query_sessions = """
           SELECT session_id, organic_traff
           FROM table_processing_sessions
           """
sql_query_hits = """
           SELECT session_id, target
           FROM table_processing_hits
           """

args = {
    'owner': 'practice_user',
    'start_date': datetime(2025, 4, 10),
    # 'retries': 1,
}

with DAG(
    dag_id='dag_create_table',
    default_args=args,
    schedule_interval='@once',
    catchup=False,
) as dag:
    start_tasks = BashOperator(
        task_id='start_tasks',
        bash_command='echo "Start tasks!"',
    )
    with TaskGroup(
            'load_raw_and_processing_group_and_load'
    ) as raw_proc_and_load:

        load_raw_table = PythonOperator(
            task_id='load_raw_table',
            python_callable=load_pkl_to_db_new_table,
            op_kwargs={'directory_file': f'{path}/data/data_old'}
        )
        processing_hits = PythonOperator(
            task_id='processing_hits',
            python_callable=processing_ga_hits_pkl,
            op_kwargs={
                'path_to_file': f'{path}/data/data_old/ga_hits.pkl',
                'output_path_file': f'{path}/data/data_processing/data_old/processing_hits.pkl',
            }
        )
        processing_sessions = PythonOperator(
            task_id='processing_sessions',
            python_callable=processing_ga_sessions_pkl,
            op_kwargs={
                'path_to_file': f'{path}/data/data_old/ga_sessions.pkl',
                'output_path_file': f'{path}/data/data_processing/data_old/processing_sessions.pkl',
            }
        )
        load_processing_table = PythonOperator(
            task_id='load_processing_table',
            python_callable=load_pkl_to_db_new_table,
            op_kwargs={'directory_file': f'{path}/data/data_processing/data_old'}
        )
        load_raw_table >> processing_hits >> processing_sessions >> load_processing_table

    with TaskGroup(
            'load_db_tables_columns'
    ) as download_db_columns:

        download_column_sessions = SQLExecuteQueryOperator(
            task_id='download_column_sessions',
            conn_id='airflow_docker_pr-database-1',
            sql=sql_query_sessions,
            do_xcom_push=True,
        )
        save_column_sessions = PythonOperator(
            task_id='save_column_sessions',
            python_callable=load_column_table,
            op_kwargs={
                'task_name': 'download_column_sessions',
                'columns': ['session_id', 'organic_traff'],
                'path_output': f'{path}/data/data_columns/download_column_ga_sessions.pkl'
            },
            provide_context=True,
        )
        download_column_hits = SQLExecuteQueryOperator(
            task_id='download_column_hits',
            conn_id='airflow_docker_pr-database-1',
            sql=sql_query_hits,
            do_xcom_push=True,
        )
        save_column_hits = PythonOperator(
            task_id='save_column_hits',
            python_callable=load_column_table,
            op_kwargs={
                'task_name': 'download_column_hits',
                'columns': ['session_id', 'target'],
                'path_output': f'{path}/data/data_columns/download_column_ga_hits.pkl'
            },
            provide_context = True,
        )
        download_column_sessions >> save_column_sessions >> download_column_hits >> save_column_hits

    with TaskGroup(
            'join_new_raw_df_in_db_table'
    ) as join_new_data:

        load_join_raw_table_h = PythonOperator(
            task_id='load_join_raw_table_hits',
            python_callable=load_new_table,
            op_kwargs={
                'pattern': 'ga_sessions_new*.json',
                'name': 'ga_sessions',
                'keys_check': keys_check_sessions
            }
        )
        load_join_raw_table_s = PythonOperator(
            task_id='load_join_raw_table_sessions',
            python_callable=load_new_table,
            op_kwargs={
                'pattern': 'ga_hits_new*.json',
                'name': 'ga_hits',
                'keys_check': keys_check_hits
            }
        )
        load_join_raw_table_h >> load_join_raw_table_s

    with TaskGroup(
        'processing_new_data'
    ) as processing_new_tables:

        processing_new_session = PythonOperator(
            task_id='processing_new_sessions',
            python_callable=processing_new_sessions
        )
        processing_new_hit = PythonOperator(
            task_id='processing_new_hits',
            python_callable=processing_new_hits
        )
        processing_new_session >> processing_new_hit

    with TaskGroup(
            'sql_join_table'
    ) as sql_join_tables:

        sql_join_table_sessions = PythonOperator(
            task_id='sql_join_table_sessions',
            python_callable=sql_join_new_processing_df_in_db_table,
            op_kwargs={
                'path_file': f'{path}/data/data_upload_table/sessions_new.pkl'
            }
        )
        sql_join_table_hits = PythonOperator(
            task_id='sql_join_table_hits',
            python_callable=sql_join_new_processing_df_in_db_table,
            op_kwargs={
                'path_file': f'{path}/data/data_upload_table/hits_new.pkl'
            }
        )
        sql_join_table_sessions >> sql_join_table_hits

    ending_task = BashOperator(
        task_id='tasks_finished',
        bash_command='echo "Task finished and dag switch off!"'
    )
    start_tasks >> raw_proc_and_load >> download_db_columns >> join_new_data >> processing_new_tables >> sql_join_tables >> ending_task
