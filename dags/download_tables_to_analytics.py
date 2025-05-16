import os

from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import datetime


###
##
# Т.ж. эти запросы можно выполнить вне тасок через pgAdmin4
# Может понадобиться доп. разрешение на работу в директории root
##
###


sql_table_hits = f"""
COPY (SELECT * FROM table_processing_hits) TO
'/root/project_sbr/data/data_analytics/processing_hits.csv' WITH CSV HEADER;
"""

sql_table_sessions = f"""
COPY (SELECT * FROM table_processing_sessions) TO
'/root/project_sbr/data/data_analytics/processing_sessions.csv' WITH CSV HEADER;
"""

args = {
    'owner': 'practice_user',
    'start_date': datetime(2025, 5, 15)
}
with DAG(
    dag_id='download_tables_to_analytics',
    default_args=args,
    schedule_interval='@once',
) as dag:
    export_table_hits = SQLExecuteQueryOperator(
        task_id='export_table_hits',
        conn_id='airflow_docker_pr-database-1',
        sql=sql_table_hits,
    )
    export_table_sessions = SQLExecuteQueryOperator(
        task_id='export_table_sessions',
        conn_id='airflow_docker_pr-database-1',
        sql=sql_table_sessions,
    )

    export_table_hits >> export_table_sessions