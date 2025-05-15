import os
import pandas as pd

from airflow.hooks.base import BaseHook
from sqlalchemy_utils.types.pg_composite import psycopg2


path = os.environ.get('PROJECT_SBER_PATH', '..')

def check_for_compatibility(path_file) -> None:
    general_path = os.path.dirname(path_file)

    old_hit_path =  f'{path}/data/data_columns/download_column_ga_hits.pkl'
    new_hit_path = f'{general_path}/hits_new.pkl'

    df_hits1 = pd.read_pickle(old_hit_path)
    df_hits2 = pd.read_pickle(new_hit_path)
    df_hits_all = pd.concat(
        [df_hits1['session_id'], df_hits2['session_id']],
        ignore_index=True
    )
    df_sessions = pd.read_pickle(path_file)

    df_sessions_compatibility = (
        df_sessions[df_sessions['session_id'].isin(df_hits_all)]
    )
    df_sessions_compatibility.to_pickle(path_file)

def sql_join_new_processing_df_in_db_table(path_file):

    if 'sessions' in path_file:
        check_for_compatibility(path_file)


    data = pd.read_pickle(path_file)
    conn_id = 'airflow_docker_pr-database-1'
    connect = BaseHook.get_connection(conn_id)

    keys_processing_sessions = [
        'session_id',
        'client_id',
        'visit_date',
        'visit_time',
        'visit_number',
        'utm_source',
        'utm_medium',
        'utm_campaign',
        'utm_adcontent',
        'device_category',
        'device_brand',
        'device_screen_resolution',
        'device_browser',
        'geo_country',
        'geo_city',
        'device_os_y',
        'organic_traff'
    ]
    keys_processing_hits = [
        'session_id',
        'hit_date',
        'hit_time',
        'hit_number',
        'hit_page_path',
        'event_category',
        'event_action',
        'target'
    ]

    if 'sessions' in path_file:
        keys = keys_processing_sessions
        table = 'table_processing_sessions'
        condition = 'organic_traff'
    else:
        keys = keys_processing_hits
        table = 'table_processing_hits'
        condition = 'target'


    key = ', '.join(keys)
    values = ', '.join('%s' for _ in keys)
    sets = ', '.join([f'{col} = EXCLUDED.{col}' for col in keys[1:]])

    update_table_sessions = f"""
    INSERT INTO {table} ({key})
    VALUES ({values})
    ON CONFLICT (session_id) DO UPDATE
    SET {sets}
    WHERE {table}.{condition} <> EXCLUDED.{condition}
    AND EXCLUDED.{condition} = 1;
    """

    with psycopg2.connect(
        dbname = connect.schema,
        user = connect.login,
        password = connect.password,
        host = connect.host,
        port= connect.port,
    ) as connect:
        with connect.cursor() as cursor:
            for index, row in data.iterrows():
                values_temp = tuple(row[col] for col in keys)
                cursor.execute(update_table_sessions, values_temp)

            connect.commit()