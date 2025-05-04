import pandas as pd
import os
import dill

from sklearn.pipeline import Pipeline
from sklearn.preprocessing import FunctionTransformer


path = os.environ.get('PROJECT_SBER_PATH', '..')


def hit_time_median_category_event(df: pd.DataFrame) -> pd.DataFrame:
    df_finish = pd.DataFrame()
    median_time = 0
    # category_median = {}

    df_temp = df[['hit_time', 'event_category']].copy()
    categories = df_temp['event_category'].value_counts().index.tolist()

    for category in categories:
        median_time = df_temp[df_temp['event_category'] == category].hit_time.astype(float).quantile(0.50)
        # category_median[category] = median_time

        df_temp1 = df_temp[((df_temp.hit_time.isna() == True)
                            & (df_temp['event_category'] == category))].hit_time.fillna(median_time)
        df_finish = pd.concat([df_finish, df_temp1])

    df_finish = df_finish.rename(columns={0: 'hit_time'})
    df.loc[df_finish.index, 'hit_time'] = df_finish['hit_time']

    return df  # , category_median


def no_duplicated_target_and_null(df: pd.DataFrame) -> pd.DataFrame:
    target_action = [
        'sub_car_claim_click',
        'sub_car_claim_submit_click',
        'sub_open_dialog_click',
        'sub_custom_question_submit_click',
        'sub_call_number_click',
        'sub_callback_submit_click',
        'sub_submit_success',
        'sub_car_request_submit_click'
    ]

    df_temp = df.copy()
    pattern_word = '|'.join(target_action)

    df_temp['target'] = (df_temp['event_action']
                           .str
                           .contains(pattern_word)
                           .astype(int)
                           )

    df_sort = df_temp.sort_values(by=['target'], ascending=False)

    df_finish = df_sort.drop_duplicates(subset='session_id', keep='first')

    return df_finish


drop_columns_hit = [
    'event_label',
    'event_value',
    'hit_referer',
    'hit_type'
]

def dropna_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.dropna()
    return df

def processing_ga_hits_pkl(path_to_file, output_path_file) -> None:
    data_hits_pkl = pd.read_pickle(path_to_file)

    data_hits_pkl['hit_date'] = pd.to_datetime(data_hits_pkl['hit_date'])
    data_hits_pkl['event_category'] = data_hits_pkl['event_category'].astype('category')

    data_hits_pkl = data_hits_pkl.drop(drop_columns_hit, axis=1)

    column_transformer = Pipeline(steps=[
        ('median_time', FunctionTransformer(hit_time_median_category_event)),
        ('drop_nan', FunctionTransformer(dropna_columns)),
        ('drop_duplicated', (FunctionTransformer(no_duplicated_target_and_null))),
    ])

    finish = column_transformer.fit_transform(data_hits_pkl)

    with open(output_path_file, 'wb') as file:
        dill.dump(finish, file)

if __name__ == '__main__':
    processing_ga_hits_pkl()