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
from src.transform_lambda.transform import Transformer


BUCKET_NAME = f'test-extraction-bucket-{int(datetime.now().timestamp())}'
PROCESS_BUCKET_NAME =\
    f'test-processed-bucket-{int(datetime.now().timestamp())}'
script_path = os.path.abspath(__file__)
script_dir = os.path.dirname(script_path)
TEST_DATA_PATH = f'{script_dir}/data'
TEST_DATA_PATH = 'test/transform_lambda/data'


@pytest.fixture(scope="module")
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "us-east-1"
    os.environ['OI_STORER_INFO'] = json.dumps(
        {'s3_BUCKET_NAME': BUCKET_NAME})


@pytest.fixture(scope="module")
def s3(aws_credentials):
    with mock_s3():
        file_list = ['address', 'design', 'counterparty',
                     'purchase_order', 'staff', 'sales_order',
                     'payment', 'transaction', 'payment_type',
                     'currency', 'department']
        s3_client = boto3.client("s3")
        s3_client.create_bucket(Bucket=BUCKET_NAME)
        s3_client.create_bucket(Bucket=PROCESS_BUCKET_NAME)
        for file_name in file_list:
            with open(f'{TEST_DATA_PATH}/{file_name}.csv', 'rb') as f:
                s3_client.put_object(Bucket=BUCKET_NAME,
                                     Key=f'{file_name}', Body=f)
        yield s3_client


@pytest.fixture(scope='module')
def transformer(s3):
    return Transformer(BUCKET_NAME, PROCESS_BUCKET_NAME)


@pytest.fixture(scope="module", params=[
    ('currency', (3, 3)),
    ('design', (107, 4)),
    ('address', (30, 8))
])
def s3_file(request, transformer):
    key, shape = request.param
    s3_file_df = transformer.read_csv(key)
    return s3_file_df, shape, key


@pytest.fixture(scope='function')
def s3_deleter(s3):
    file_name = Transformer.FILE_LIST[0]
    s3.delete_object(Bucket=BUCKET_NAME, Key=file_name)
    yield file_name
    s3.put_object(Bucket=BUCKET_NAME,
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
        s3.put_object(Bucket=BUCKET_NAME,
                      Key=f'{file_name}_test',
                      Body=csv_data.encode('utf-8'))
    result = transformer.read_csv('department_test')

    # Define the expected dataframe
    expected_df = pd.DataFrame(data={'a': [1], 'b': [2], 'c': [3], 'd': [4]})
    # Test that the dataframe is equal to the expected dataframe
    assert_frame_equal(result, expected_df)


def test_read_csv_raises_error(s3, transformer):
    with pytest.raises(Exception):
        transformer.read_csv('NO_SUCH_CSV_FILE')


def test_store_as_parquet_object_is_stored_bucket(s3, transformer):
    df = pd.DataFrame(data={'a': [1], 'b': [2], 'c': [3], 'd': [4]})

    transformer.store_as_parquet('mock_address', df)
    retrieved_parquet_metadata = transformer.s3_client.head_object(
        Bucket=PROCESS_BUCKET_NAME, Key='mock_address')

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
    expected_df_shape = (3, 3)
    expected_df_cols = {'currency_code', 'currency_id', 'currency_name'}

    currency_df = pd.read_csv(
        f'{TEST_DATA_PATH}/currency.csv', encoding='utf-8')

    res_df = transformer.transform_currency(currency_df)
    assert res_df.shape == expected_df_shape
    assert set(res_df.columns) == expected_df_cols


def test_transform_currency_raises_error_given_no_csv_file(transformer):
    with pytest.raises(Exception):
        pd.read_csv('NO_SUCH_FILE.csv', encoding='utf-8')


def test_transform_design_returns_correct_data_frame_structure(transformer):
    expected_df_shape = (107, 4)
    expected_df_cols = {'design_id', 'design_name',
                        'file_location', 'file_name'}

    design_df = pd.read_csv(
        f'{TEST_DATA_PATH}/design.csv', encoding='utf-8')

    res_df = transformer.transform_design(design_df)
    assert res_df.shape == expected_df_shape
    assert set(res_df.columns) == expected_df_cols


def test_transform_address_returns_correct_data_frame_structure(transformer):
    expected_df_shape = (30, 8)
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


def test_transform_sales_order_returns_correct_data_frame(transformer):
    expected_df_shape = (1544, 15)
    expected_df_cols = {'sales_record_id', 'created_date',
                        'created_time', 'last_updated_date',
                        'last_updated_time',
                        'sales_order_id', 'sales_staff_id',
                        'counterparty_id', 'units_sold', 'unit_price',
                        'currency_id', 'design_id', 'agreed_payment_date',
                        'agreed_delivery_date',
                        'agreed_delivery_location_id'}
    sales_order_df = pd.read_csv(
        f'{TEST_DATA_PATH}/sales_order.csv', encoding='utf-8')
    res_df = transformer.transform_sales_order(sales_order_df)
    assert res_df.shape == expected_df_shape
    assert set(res_df.columns) == expected_df_cols


def test_transform_sales_order_returns_correct_data(transformer):
    expected_fact_sales = pd.DataFrame(
        data={'sales_record_id': [1], 'sales_order_id': [1],
              'created_date': ['2022-11-03'],
              'created_time': ['14:20:52.186000'],
              'last_updated_date': ['2022-11-03'],
              'last_updated_time': ['14:20:52.186000'],
              'sales_staff_id': [16], 'counterparty_id': [18],
              'units_sold': [84754],
              'unit_price': [2.43], 'currency_id': [3], 'design_id': [9],
              'agreed_payment_date': ['2022-11-03'],
              'agreed_delivery_date': ['2022-11-10'],
              'agreed_delivery_location_id': 4})

    expected_fact_sales['created_date'] = pd.to_datetime(
        expected_fact_sales['created_date']).dt.date
    expected_fact_sales['created_time'] = pd.to_datetime(
        expected_fact_sales['created_time']).dt.time
    expected_fact_sales['last_updated_date'] = pd.to_datetime(
        expected_fact_sales['last_updated_date']).dt.date
    expected_fact_sales['last_updated_time'] = pd.to_datetime(
        expected_fact_sales['last_updated_time']).dt.time

    sales_order_df = pd.read_csv(
        f'{TEST_DATA_PATH}/sales_order.csv', encoding='utf-8')
    res_df = transformer.transform_sales_order(sales_order_df)
    assert_frame_equal(res_df.iloc[:1], expected_fact_sales)


def test_transform_staff_dept_table_returns_correct_df_structure(transformer):
    expected_dim_staff_shape = (20, 6)
    expected_df_cols = {'staff_id', 'first_name', 'last_name',
                        'department_name', 'location', 'email_address'}

    staff_df = pd.read_csv(
        f'{TEST_DATA_PATH}/staff.csv', encoding='utf-8')
    department_df = pd.read_csv(
        f'{TEST_DATA_PATH}/department.csv', encoding='utf-8')

    res_df = transformer.transform_staff(staff_df, department_df)
    assert res_df.shape == expected_dim_staff_shape
    assert set(res_df.columns) == expected_df_cols


def test_transform_counterparty_returns_correct_df_structure(transformer):
    expected_dim_counterparty_shape = (20, 9)
    expected_df_cols = {'counterparty_id',
                        'counterparty_legal_name',
                        'counterparty_legal_address_line_1',
                        'counterparty_legal_address_line_2',
                        'counterparty_legal_district',
                        'counterparty_legal_city',
                        'counterparty_legal_postal_code',
                        'counterparty_legal_country',
                        'counterparty_legal_phone_number'}

    counterparty_df = pd.read_csv(
        f'{TEST_DATA_PATH}/counterparty.csv', encoding='utf-8')
    address_df = pd.read_csv(
        f'{TEST_DATA_PATH}/address.csv', encoding='utf-8')

    res_df = transformer.transform_counterparty(counterparty_df, address_df)
    assert res_df.shape == expected_dim_counterparty_shape
    assert set(res_df.columns) == expected_df_cols
