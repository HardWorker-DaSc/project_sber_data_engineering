import fnmatch as fm
import json
import os
import pandas as pd

from airflow.hooks.base import BaseHook
from sqlalchemy import create_engine


path = os.environ.get('PROJECT_SBER_PATH', '..')


class DictionaryChecker:
    def __init__(self, data):
        """initialisation class checker dict"""
        if not isinstance(data, dict):
            raise ValueError('its not dict')
        self.data = data

    def has_keys(self, keys):
        """check all keys"""
        return all(key in self.data for key in keys)


def create_url_pgsql(db_connect_id):
    con = BaseHook.get_connection(db_connect_id)

    user = con.login
    password = con.password
    host = con.host
    port = con.port
    db = con.schema

    url = f'postgresql://{user}:{password}@{host}:{port}/{db}'

    return url


def load_new_table(pattern, name, keys_check):
    df = pd.DataFrame()

    path_file_new = f'{path}/data/data_new'
    db_connect_id = 'airflow_docker_pr-database-1'

    engine = create_engine(create_url_pgsql(db_connect_id))

    matching_files = [
        f
        for f in os.listdir(path_file_new)
        if fm.fnmatch(f, pattern)
    ]

    for file in matching_files:
        record = []
        path_file = os.path.join(path_file_new, file)

        with open(path_file, 'r') as files:
            data = json.load(files)

        for date, sessions in data.items():
            if len(sessions) > 0:

                for session in sessions:
                    checker = DictionaryChecker(session)

                    if checker.has_keys(keys_check):
                        record.append(session)
                    else:
                        continue
            else:
                break

        df_record = pd.DataFrame(record)
        df = pd.concat([df, df_record], ignore_index=True)

    df.to_sql(f'table_{name}', engine, if_exists='append', index=False)
    df.to_pickle(f'{path}/data/data_new/united/new_{name}.pkl')

if __name__ == '__main__':
    load_new_table()