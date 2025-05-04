import os
import pandas as pd


path = os.environ.get('PROJECT_SBER_PATH', '..')


from modules.processing_ga_hits_pkl import processing_ga_hits_pkl

def processing_new_hits():

    path_to_file = f'{path}/data/data_new/united/new_ga_hits.pkl'
    output_path_file = f'{path}/data/data_processing/data_new/processing_new_hits.pkl'

    processing_ga_hits_pkl(path_to_file, output_path_file)

    old_path_file = f'{path}/data/data_columns/download_column_ga_hits.pkl'


    df_new = pd.read_pickle(output_path_file)
    df_old = pd.read_pickle(old_path_file)

    df_new_short = df_new[[
        'session_id',
        'target',
    ]]

    merged_df = (
        pd.concat([df_old, df_new_short])
        .sort_values(by='target', key=lambda x: x != 1)
        .drop_duplicates(subset='session_id', keep='first')
    )

    merged_df_finish = (
        df_new
        .merge(merged_df, on='session_id', suffixes=('', '_merge'), how='left')
    )

    merged_df_finish['target'] = (
        merged_df_finish
        .apply(
            lambda row: 1 if row['target_merge'] == 1 else row['target'],
            axis=1,
        )
    )
    merged_df_finish = (
        merged_df_finish
        .drop(columns=['target_merge'])
    )

    merged_df_finish.to_pickle(f'{path}/data/data_upload_table/hits_new.pkl')

if __name__ == '__main__':
    processing_new_hits()