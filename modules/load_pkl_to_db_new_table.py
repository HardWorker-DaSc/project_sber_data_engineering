import pandas as pd
import os

from sqlalchemy import create_engine, Column, Integer, String, Date, Float, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship

path = os.environ.get('PROJECT_SBER_PATH')


def check_for_compatibility(directory_file, name_files) -> None:
    path_file_parent = os.path.join(directory_file, name_files[0])
    path_file_subsidiary = os.path.join(directory_file, name_files[1])

    file_parent = pd.read_pickle(path_file_parent)
    file_parent = file_parent['session_id']

    file_subsidiary = pd.read_pickle(path_file_subsidiary)

    del_conflict_subsidiary = file_subsidiary[file_subsidiary['session_id'].isin(file_parent)]
    del_conflict_subsidiary.to_pickle(path_file_subsidiary)


def load_pkl_to_db_no_primary_key(path_file, table_name, db_config):
    df = pd.read_pickle(path_file)

    connection = (f"postgresql://{db_config['username']}:"
                  f"{db_config['password']}@"
                  f"{db_config['host']}:"
                  f"{db_config['port']}/"
                  f"{db_config['database']}")

    engine = create_engine(connection)

    df.to_sql(table_name, engine, if_exists='replace', index=False)


def load_pkl_to_db_primary_key(path_files, tables_name, db_config):
    df_parent = pd.read_pickle(path_files[0])
    df_subsidiary = pd.read_pickle(path_files[1])

    name_parent = tables_name[0]
    name_subsidiary = tables_name[1]

    connection = (f"postgresql://{db_config['username']}:"
                  f"{db_config['password']}@"
                  f"{db_config['host']}:"
                  f"{db_config['port']}/"
                  f"{db_config['database']}")

    engine = create_engine(connection)
    Base = declarative_base()

    def create_table_class(table_name, df, is_subsidiary=False):
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
                elif column_type == 'category':
                    vars()[column_name] = Column(String)
                elif column_type == 'float64':
                    vars()[column_name] = Column(Float)
                elif column_type == 'datetime64[ns]':
                    vars()[column_name] = Column(Date)

            if is_subsidiary:
                vars()['parent_id'] = Column(
                    String,
                    ForeignKey('table_processing_hits.session_id')
                )
                vars()['parent'] = relationship(
                    'DynamicTable',
                    backref='subsidiaries',
                )

        return DynamicTable


    ParentTable = create_table_class(name_parent, df_parent)
    Base.metadata.create_all(engine)

    df_parent.to_sql(name_parent, engine, if_exists='replace', index=False)

    SubsidiaryTable = create_table_class(name_subsidiary, df_subsidiary, is_subsidiary=True)
    Base.metadata.create_all(engine)

    df_subsidiary.to_sql(name_subsidiary, engine, if_exists='replace', index=False)


def load_pkl_to_db_new_table(directory_file):
    db_config = {
        'username': 'user_practice',
        'password': 'user_practice',
        'host': 'airflow_docker_pr-database-1',
        'port': '5432',
        'database': 'user_practice'
    }
    priority = 'hits'
    processed = 'processing'
    name_files = sorted(os.listdir(directory_file), key=lambda x: priority not in x)

    if processed in name_files[0]:

        check_for_compatibility(directory_file, name_files)

        path_files = []
        tables_name = []

        for filename in name_files:
            tables_name.append(f'table_{filename.split(".")[0]}')
            path_files.append(os.path.join(directory_file, filename))

        load_pkl_to_db_primary_key(path_files, tables_name, db_config)
    else:
        for filename in name_files:
            table_name = f'table_{filename.split(".")[0]}'
            path_file = os.path.join(directory_file, filename)

            if os.path.isfile(path_file):
                load_pkl_to_db_no_primary_key(path_file, table_name, db_config)



if __name__ == '__main__':
    load_pkl_to_db_new_table()