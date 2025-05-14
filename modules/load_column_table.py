import os
import pandas as pd


path = os.environ.get('PROJECT_SBER_PATH', '..')


def load_column_table(task_name, columns, path_output, **kwargs):
    ti = kwargs['ti']
    result = ti.xcom_pull(task_ids=f'load_db_tables_columns.{task_name}')

    df = pd.DataFrame(result, columns=columns)

    df.to_pickle(path_output)

if __name__ == '__main__':
    load_column_table()