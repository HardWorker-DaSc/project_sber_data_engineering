import pandas as pd
import os

from click import DateTime
from sqlalchemy import create_engine, Column, Integer, String, Date, Float
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker


path = os.environ.get('PROJECT_SBER_PATH')


def load_pkl_to_db_no_primary_key(path_file, table_name, db_config):
    df = pd.read_pickle(path_file)

    connection = (f"postgresql://{db_config['username']}:"
                  f"{db_config['password']}@"
                  f"{db_config['host']}:"
                  f"{db_config['port']}/"
                  f"{db_config['database']}")

    engine = create_engine(connection)

    df.to_sql(table_name, engine, if_exists='replace', index=False)


def load_pkl_to_db_primary_key(path_file, table_name, db_config):
    df = pd.read_pickle(path_file)

    connection = (f"postgresql://{db_config['username']}:"
                  f"{db_config['password']}@"
                  f"{db_config['host']}:"
                  f"{db_config['port']}/"
                  f"{db_config['database']}")

    engine = create_engine(connection)
    Base = declarative_base()

    def create_table_class(table_name, df):
        primary_column_key = 'session_id'
        class DynamicTable(Base):
            __tablename__ = table_name

            for column_name, column_type in zip(df.columns, df.dtypes):
                if column_name == primary_column_key:
                    vars()[column_name] = Column(String, primary_key=True)
                elif column_type == 'object':
                    vars()[column_name] = Column(String)
                elif column_type == 'int64':
                    vars()[column_name] = Column(Integer)
                elif column_type.name == 'category':
                    vars()[column_name] = Column(String)
                elif column_type.name == 'float64':
                    vars()[column_name] = Column(Float)
                elif column_type.name == 'datetime64[ns]':
                    vars()[column_name] = Column(Date)

        return DynamicTable

    DynamicTable = create_table_class(table_name, df)

    Base.metadata.create_all(engine)

    df.to_sql(table_name, engine, if_exists='append', index=False)


def load_pkl_to_db_new_table(directory_file):

    db_config = {
        'username': 'user_practice',
        'password': 'user_practice',
        'host': 'airflow_docker_pr-database-1',
        'port': '5432',
        'database': 'user_practice'
    }

    for filename in os.listdir(directory_file):

        table_name = f'table_{filename.split('.')[0]}'
        path_file = os.path.join(directory_file, filename)

        if os.path.isfile(path_file):
            if 'processing' in table_name:
                load_pkl_to_db_primary_key(path_file, table_name, db_config)
            else:
                load_pkl_to_db_no_primary_key(path_file, table_name, db_config)

if __name__ == '__main__':
    load_pkl_to_db_new_table()