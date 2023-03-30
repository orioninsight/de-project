from moto import mock_s3
import boto3
import os
from datetime import datetime
import json
import pandas as pd
import pytest
from pandas.testing import assert_frame_equal, assert_series_equal
import tempfile
import fastparquet as fp
from src.transforming_lambda.transform import Transformer


bucket_name = f'test-extraction-bucket-{int(datetime.now().timestamp())}'
processed_bucket_name =\
    f'test-processed-bucket-{int(datetime.now().timestamp())}'
# TEST_DATA_PATH = 'test/transforming_lambda/data'
# get dynamic data path
script_path = os.path.abspath(__file__)
script_dir = os.path.dirname(script_path)
TEST_DATA_PATH = f'{script_dir}/data'


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
        s3_client.create_bucket(Bucket=processed_bucket_name)
        for file_name in file_list:
            with open(f'{TEST_DATA_PATH}/{file_name}.csv', 'rb') as f:
                s3_client.put_object(Bucket=bucket_name,
                                     Key=f'{file_name}', Body=f)
        yield s3_client


@pytest.fixture(scope='module')
def transformer(s3):
    return Transformer(bucket_name, processed_bucket_name)


@pytest.fixture(scope="module", params=[
    ('currency', (3, 2)),
    ('design', (10, 4)),
    ('address', (10, 8))
])
def s3_file(request, transformer):
    key, shape = request.param
    s3_file_df = transformer.read_csv(key)
    return s3_file_df, shape, key


@pytest.fixture(scope='function')
def s3_deleter(s3):
    file_name = Transformer.FILE_LIST[0]
    s3.delete_object(Bucket=bucket_name, Key=file_name)
    yield file_name
    s3.put_object(Bucket=bucket_name,
                  Key=f'{file_name}',
                  Body=open(f'{TEST_DATA_PATH}/{file_name}.csv', 'rb'))


def test_list_csv_files_returns_list_of_csv_files(s3,
                                                  transformer):
    for file in transformer.list_csv_files():
        assert file in Transformer.FILE_LIST


def test_list_csv_files_raises_exception_missing_files(s3,
                                                       transformer,
                                                       s3_deleter):
    with pytest.raises(Exception, match='Files are not complete'):
        transformer.list_csv_files()


def test_read_csv_returns_data_frame(s3, transformer):
    csv_data = '''a,b,c,d\n
                  1,2,3,4
                    '''
    for file_name in Transformer.FILE_LIST:
        s3.put_object(Bucket=bucket_name,
                      Key=f'{file_name}_test',
                      Body=csv_data.encode('utf-8'))
    result = transformer.read_csv('department_test')

    # Define the expected dataframe
    expected_df = pd.DataFrame(data={'a': [1], 'b': [2], 'c': [3], 'd': [4]})
    # Test that the dataframe is equal to the expected dataframe
    assert_frame_equal(result, expected_df)


def test_store_as_parquet_object_is_stored_bucket(s3, transformer):
    df = pd.DataFrame(data={'a': [1], 'b': [2], 'c': [3], 'd': [4]})

    transformer.store_as_parquet('mock_address', df)
    retrieved_parquet_metadata = transformer.s3_client.head_object(
        Bucket=processed_bucket_name, Key='mock_address')

    assert retrieved_parquet_metadata is not None


def test_store_as_parquet_check_integrity_of_object(s3, transformer):
    df = pd.DataFrame(data={'a': [1], 'b': [2], 'c': [3], 'd': [4]})

    transformer.store_as_parquet('mock_address', df)

    # Download the parquet file from S3 to a temporary local file
    with tempfile.NamedTemporaryFile() as temp_file:
        transformer.s3_client.download_file(
            transformer.s3_processed_bucket_name, 'mock_address',
            temp_file.name)

        # Read the parquet file using fastparquet
        pf = fp.ParquetFile(temp_file.name)
        retrieved_df = pf.to_pandas()

        assert_frame_equal(df, retrieved_df)


def test_store_as_parquet_incorrect_object_passed_as_df(s3, transformer):
    df = 'not a dataframe'

    with pytest.raises(ValueError, match='ERROR: object not a dataframe'):
        transformer.store_as_parquet('mock_address', df)


def test_store_as_parquet_error_when_file_name_not_string(s3, transformer):

    df = pd.DataFrame(data={'a': [1], 'b': [2], 'c': [3], 'd': [4]})

    with pytest.raises(TypeError, match='ERROR: file_name expects a string'):
        transformer.store_as_parquet(True, df)


def test_transform_currency_returns_correct_data_frame_from_s3(s3, s3_file,
                                                               transformer):
    s3_file_df, expected_df_shape, table = s3_file

    transform_fn = getattr(transformer, f'transform_{table}')
    res_df = transform_fn(s3_file_df)
    assert res_df.shape == expected_df_shape


def test_transform_currency_returns_correct_data_frame_structure(transformer):
    expected_df_shape = (3, 2)
    expected_df_cols = {'currency_code', 'currency_id'}

    currency_df = pd.read_csv(
        f'{TEST_DATA_PATH}/currency.csv', encoding='utf-8')

    res_df = transformer.transform_currency(currency_df)
    assert res_df.shape == expected_df_shape
    assert set(res_df.columns) == expected_df_cols


def test_transform_design_returns_correct_data_frame_structure(transformer):
    expected_df_shape = (10, 4)
    expected_df_cols = {'design_id', 'design_name',
                        'file_location', 'file_name'}

    design_df = pd.read_csv(
        f'{TEST_DATA_PATH}/design.csv', encoding='utf-8')

    res_df = transformer.transform_design(design_df)
    assert res_df.shape == expected_df_shape
    assert set(res_df.columns) == expected_df_cols


def test_transform_address_returns_correct_data_frame_structure(transformer):
    expected_df_shape = (10, 8)
    expected_df_cols = {'location_id', 'address_line_1', 'address_line_2',
                        'district', 'city', 'postal_code', 'country', 'phone'}

    address_df = pd.read_csv(
        f'{TEST_DATA_PATH}/address.csv', encoding='utf-8')

    res_df = transformer.transform_address(address_df)
    assert res_df.shape == expected_df_shape
    assert set(res_df.columns) == expected_df_cols


def test_create_dim_date_creates_data_frame_structure(transformer):
    expected_dim_date_shape = (180, 8)
    expected_first_row = pd.Series(
        name=0,  # Equals row num here
        data=[20221103, 2022, 11, 3, 3, 'Thursday', 'November', 4],
        index=['date_id', 'year', 'month', 'day', 'day_of_week',
               'day_name', 'month_name', 'quarter'])
    res_df = transformer.create_dim_date()
    assert expected_dim_date_shape == res_df.shape
    assert_series_equal(res_df.iloc[0, :], expected_first_row)
