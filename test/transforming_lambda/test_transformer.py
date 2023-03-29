from moto import mock_s3
import boto3
import os
from datetime import datetime
import json
import pandas as pd
import pytest
from pandas.testing import assert_frame_equal, assert_index_equal
from src.transforming_lambda.transform import (transform_currency,
                                               transform_design, read_csv, transform_address)
import src.transforming_lambda.transform as tf

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


@pytest.fixture(scope="module", params=[('currency', (3, 2)), ('design', (106, 4)), ('address', (30, 8))])
def s3_file(request):
    key, shape = request.param
    s3_file_df = read_csv(key)
    return s3_file_df, shape, key


def test_transform_currency_returns_correct_data_frame_from_s3(s3, s3_file):
    s3_file_df, expected_df_shape, table = s3_file

    transform_fn = getattr(tf, f'transform_{table}')
    res_df = transform_fn(s3_file_df)

    assert res_df.shape == expected_df_shape


def test_transform_currency_returns_correct_data_frame_structure():
    expected_df_shape = (3, 2)
    expected_df_cols = {'currency_code', 'currency_id'}

    currency_df = pd.read_csv(
        'test/transforming_lambda/data/currency.csv', encoding='utf-8')

    res_df = transform_currency(currency_df)

    assert res_df.shape == expected_df_shape

    assert set(res_df.columns) == expected_df_cols


def test_transform_design_returns_correct_data_frame_structure():
    expected_df_shape = (106, 4)
    expected_df_cols = {'design_id', 'design_name',
                        'file_location', 'file_name'}

    design_df = pd.read_csv(
        'test/transforming_lambda/data/design.csv', encoding='utf-8')

    res_df = transform_design(design_df)

    assert res_df.shape == expected_df_shape

    assert set(res_df.columns) == expected_df_cols


def test_transform_address_returns_correct_data_frame_structure():
    expected_df_shape = (30, 8)
    expected_df_cols = {'location_id', 'address_line_1', 'address_line_2',
                        'district', 'city', 'postal_code', 'country', 'phone'}

    address_df = pd.read_csv(
        'test/transforming_lambda/data/address.csv', encoding='utf-8')

    res_df = transform_address(address_df)

    assert res_df.shape == expected_df_shape

    assert set(res_df.columns) == expected_df_cols
