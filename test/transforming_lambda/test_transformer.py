from moto import mock_s3
import boto3
import os
from datetime import datetime
import json
import pandas as pd
import pytest
from pandas.testing import assert_frame_equal, assert_index_equal
from src.transform_lambda.transform import transform_currency, read_csv


bucket_name = f'test-extraction-bucket-{int(datetime.now().timestamp())}'


@pytest.fixture(scope="module")
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "us-east-1"
    os.environ['OI_STORER_SECRET_STRING'] = json.dumps(
        {'s3_bucket_name': bucket_name})


@pytest.fixture(scope="module")
def s3(aws_credentials):
    with mock_s3():
        file_list = ['address', 'design', 'counterparty',
                     'purchase_order', 'staff', 'sales_order',
                     'payment', 'transaction', 'payment_type',
                     'currency', 'department']
        s3_client = boto3.client("s3")
        s3_client.create_bucket(Bucket=bucket_name)
        for file_name in file_list:
            with open(f'test/transforming_lambda/data/{file_name}.csv', 'rb') as f:
                s3_client.put_object(Bucket=bucket_name,
                                     Key=f'{file_name}', Body=f)
        yield s3_client

@pytest.fixture(scope="module",params=[('currency',(3, 2))])
def s3_file(request):
    key, shape = request.param
    s3_file_df = read_csv(key)[key]
    return s3_file_df, shape


def test_transform_currency_returns_correct_data_frame_structure():
    expected_df_shape = (3, 2)
    expected_df_cols = {'currency_code', 'currency_id'}

    currency_df = pd.read_csv(
        'test/transforming_lambda/data/currency.csv', encoding='utf-8')

    res_df = transform_currency(currency_df)

    assert res_df.shape == expected_df_shape

    assert set(res_df.columns) == expected_df_cols


def test_transform_currency_returns_correct_data_frame_from_s3(s3,s3_file):
    s3_file_df, expected_df_shape = s3_file
    res_df = transform_currency(s3_file_df)
    assert res_df.shape == expected_df_shape
