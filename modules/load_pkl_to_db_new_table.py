import pandas as pd
import os

from sqlalchemy import create_engine, Column, Integer, String, Date, Float, ForeignKey, Time
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, sessionmaker


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


    def read_csv(path_file):
        for chunk in pd.read_csv(path_file, chunksize=10000)
            yield chunk

    
    connection = (f"postgresql://{db_config['username']}:"
                  f"{db_config['password']}@"
                  f"{db_config['host']}:"
                  f"{db_config['port']}/"
                  f"{db_config['database']}")

    engine = create_engine(connection)

    first_cycle = True
    
    for df in read_csv(path_file):

        if first_cycle:
            df.to_sql(table_name, engine, if_exists='replace', index=False)
            first_cycle = False
        else:
            df.to_sql(table_name, engine, if_exists='append', index=False)


def load_pkl_to_db_primary_key(path_files, tables_name, db_config):
    df_parent = pd.read_pickle(path_files[0])
    df_subsidiary = pd.read_pickle(path_files[1])

    connection = (f"postgresql://{db_config['username']}:"
                  f"{db_config['password']}@"
                  f"{db_config['host']}:"
                  f"{db_config['port']}/"
                  f"{db_config['database']}")

    Base = declarative_base()

    class HitsTable(Base):
        __tablename__= tables_name[0]
        session_id = Column(String, primary_key=True)
        hit_date = Column(Date)
        hit_time = Column(Float)
        hit_number = Column(Integer)
        hit_page_path = Column(String)
        event_category = Column(String)
        event_action = Column(String)
        target = Column(Integer)

    class SessionsTable(Base):
        __tablename__= tables_name[1]
        session_id = Column(
            String,
            ForeignKey(f'{tables_name[0]}.session_id'),
            primary_key=True
        )
        client_id = Column(String)
        visit_date = Column(Date)
        visit_time = Column(Time)
        visit_number = Column(Integer)
        utm_source = Column(String)
        utm_medium = Column(String)
        utm_campaign = Column(String)
        utm_adcontent = Column(String)
        device_category = Column(String)
        device_brand = Column(String)
        device_screen_resolution = Column(String)
        device_browser = Column(String)
        geo_country = Column(String)
        geo_city = Column(String)
        device_os_y = Column(String)
        organic_traff = Column(Integer)

    engine = create_engine(connection)

    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    session = Session()

    for index, row in df_parent.iterrows():
        table_hits = HitsTable(**row.to_dict())
        session.add(table_hits)

    for index, row in df_subsidiary.iterrows():
        table_sessions = SessionsTable(**row.to_dict())
        session.add(table_sessions)
    session.commit()


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
