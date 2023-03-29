import pandas as pd
from pandas.testing import assert_frame_equal,assert_index_equal
from src.transform_lambda.transform import transform_currency


def test_transform_currency_returns_correct_data_frame_structure():
    expected_df_shape = (3, 2)
    expected_df_cols = {'currency_code','currency_id'}

    currency_df = pd.read_csv('test/transforming_lambda/data/currency.csv', encoding='utf-8')

    res_df = transform_currency(currency_df)

    assert res_df.shape == expected_df_shape
    #assert_index_equal(pd.Index(expected_df_cols), res_df.columns)
    assert set(res_df.columns) == expected_df_cols
    #print(list(res_df.columns))
