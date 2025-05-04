import pandas as pd
import os
import dill
import numpy as np

from sklearn.pipeline import Pipeline
from sklearn.preprocessing import FunctionTransformer


path = os.environ.get('PROJECT_SBER_PATH', '..')



def device_os_pre(df: pd.DataFrame) -> pd.DataFrame:
    shame_df = df.shape[0]
    column_uniq = []

    for column in df.columns:
        if len(df[column].unique()) == shame_df:
            column_uniq.append(column)
            break

    df_combinations = df[
        ['device_category',
         'device_brand',
         'device_os']
    ].drop_duplicates().dropna()

    df_combinations.drop(
        df_combinations
        .loc[df_combinations['device_os'] == '(not set)']
        .index,
        inplace=True
    )

    df = df.merge(df_combinations,
                  on=['device_category', 'device_brand'],
                  how='left',
                  suffixes=('', '_y')
                  )

    df_clear = df.drop('device_os', axis=1).dropna()

    df_finish = df_clear[~df_clear[column_uniq].duplicated()]

    return df_finish


def organic_1_and_pay_0_traffic(df: pd.DataFrame) -> pd.DataFrame:
    organic_traffic = [
        'organic',
        'referral',
        '(none)'
    ]

    df['organic_traff'] = np.where(df['utm_medium'].isin(organic_traffic), 1, 0)

    return df


def processing_ga_sessions_pkl(path_to_file, output_path_file):
    data_sessions_pkl = pd.read_pickle(path_to_file)

    data_sessions_pkl['visit_date'] = pd.to_datetime(data_sessions_pkl['visit_date'])
    data_sessions_pkl[
        [
            'device_category',
            'device_os',
            'utm_medium',
            'device_brand',
            'geo_country'
        ]
    ] = data_sessions_pkl[
        [
            'device_category',
            'device_os',
            'utm_medium',
            'device_brand',
            'geo_country'
        ]
    ].astype('category')

    drop_columns_sessions = [
        'device_model',
        'utm_keyword'
    ]

    df_sessions = data_sessions_pkl.drop(drop_columns_sessions, axis=1)

    column_transformer = Pipeline(steps=[
        ('device', FunctionTransformer(device_os_pre)),
        ('traffic', FunctionTransformer(organic_1_and_pay_0_traffic))
    ])

    finish = column_transformer.fit_transform(df_sessions)

    with open(output_path_file, 'wb') as file:
        dill.dump(finish, file)

if __name__ == '__main__':
    processing_ga_sessions_pkl()