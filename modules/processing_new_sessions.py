import os
import pandas as pd


path = os.environ.get('PROJECT_SBER_PATH', '..')


from modules.processing_ga_sessions_pkl import processing_ga_sessions_pkl


def processing_new_sessions():

    path_to_file = f'{path}/data/data_new/united/new_ga_sessions.pkl'
    output_path_file = f'{path}/data/data_processing/data_new/processing_new_session.pkl'

    processing_ga_sessions_pkl(path_to_file, output_path_file)

    old_path_file = f'{path}/data/data_columns/download_column_ga_sessions.pkl'


    df_new = pd.read_pickle(output_path_file)
    df_old = pd.read_pickle(old_path_file)

    df_new_short = df_new[[
        'session_id',
        'organic_traff',
    ]]

    merged_df = (
        pd.concat([df_old, df_new_short])
        .sort_values(by='organic_traff', key=lambda x: x != 1)
        .drop_duplicates(subset='session_id', keep='first')
    )

    merged_df_finish = (
        df_new
        .merge(merged_df, on='session_id', suffixes=('', '_merge'), how='left')
    )

    merged_df_finish['organic_traff'] = (
        merged_df_finish
        .apply(
            lambda row: 1 if row['organic_traff_merge'] == 1 else row['organic_traff'],
            axis=1,
        )
    )
    merged_df_finish = (
        merged_df_finish
        .drop(columns=['organic_traff_merge'])
    )

    merged_df_finish.to_pickle(f'{path}/data/data_upload_table/sessions_new.pkl')

if __name__ == '__main__':
    processing_new_sessions()

