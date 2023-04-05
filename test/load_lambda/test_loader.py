from pathlib import Path
from moto import mock_s3
import boto3
from datetime import datetime
import os
import json
import pandas as pd
import pytest
from src.load_lambda.load import Loader, loader_handler
from pandas.testing import assert_frame_equal
from sqlalchemy import create_engine, text


PROCESSED_BUCKET_NAME =\
    f'test-processed-bucket-{int(datetime.now().timestamp())}'
script_path = os.path.abspath(__file__)
script_dir = os.path.dirname(script_path)
TEST_DATA_PATH = f'{script_dir}/data'

TEST_DB = 'test.db'


@pytest.fixture(scope="module")
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "us-east-1"
    os.environ['OI_PROCESSED_INFO'] = json.dumps(
        {'s3_bucket_name': PROCESSED_BUCKET_NAME})


@pytest.fixture(scope="function")
def s3(aws_credentials):
    with mock_s3():
        s3_client = boto3.client("s3")
        s3_client.create_bucket(Bucket=PROCESSED_BUCKET_NAME)
        for file_name in Loader.FILE_LIST.keys():
            with open(f'{TEST_DATA_PATH}/{file_name}.parq', 'rb') as f:
                s3_client.put_object(Bucket=PROCESSED_BUCKET_NAME,
                                     Key=f'{file_name}', Body=f)
        yield s3_client


@pytest.fixture(scope="module")
def s3_mod(aws_credentials):
    with mock_s3():
        s3_client = boto3.client("s3")
        s3_client.create_bucket(Bucket=PROCESSED_BUCKET_NAME)
        for file_name in Loader.FILE_LIST.keys():
            with open(f'{TEST_DATA_PATH}/{file_name}.parq', 'rb') as f:
                s3_client.put_object(Bucket=PROCESSED_BUCKET_NAME,
                                     Key=f'{file_name}', Body=f)
        yield s3_client


@pytest.fixture(scope='function')
def loader(s3):
    loader = Loader(PROCESSED_BUCKET_NAME)
    loader.conn = create_engine('sqlite:///test.db').connect()
    yield loader
    loader.close()
    db_file = Path('test.db')
    if db_file.is_file:
        db_file.unlink()


@pytest.fixture(scope='function')
def address_loader(s3):
    loader = Loader(PROCESSED_BUCKET_NAME)
    loader.connect_db(**json.loads(os.environ.get('OI_TOTESYS_DW_INFO')))
    for table in list(Loader.FILE_LIST.values())[::-1]:
        loader.delete_table(table)
    yield loader
    with loader.engine.begin() as conn:
        conn.execute(text('DELETE FROM dim_location'))
    loader.close()


@pytest.fixture(scope='module')
def all_loader(s3_mod):
    loader = Loader(PROCESSED_BUCKET_NAME)
    loader.connect_db(**json.loads(os.environ.get('OI_TOTESYS_DW_INFO')))
    for table in list(Loader.FILE_LIST.values())[::-1]:
        loader.delete_table(table)
    loader_handler({}, None)
    yield loader
    for table in list(Loader.FILE_LIST.values())[::-1]:
        loader.delete_table(table)
    loader.close()


# In same table order as Loader.FILE_LIST.values
EXPECTED_ROW_COUNTS = [30, 110, 20, 20, 3, 180, 4, 2437, 835, 2437, 1602]


@pytest.fixture(scope='module',
                params=zip(Loader.FILE_LIST.values(), EXPECTED_ROW_COUNTS),
                ids=lambda x: x[0])
def table_info(request):
    yield request.param


def test_read_s3_parquet_returns_data_frame(loader):
    for file in loader.FILE_LIST.keys():
        res_df = loader.read_s3_parquet(file)
        assert isinstance(res_df, pd.DataFrame)


def test_read_s3_parquet_returns_correct_data(loader):
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
    res_df = loader.read_s3_parquet('sales_order')
    assert_frame_equal(res_df.iloc[:1], expected_fact_sales)


def test_load_address(address_loader):
    address_loader.load_table('address', 'dim_location')
    with address_loader.conn as conn:
        res = conn.execute(text('SELECT * FROM dim_location'))
        rows = res.fetchall()
    assert len(rows) == 30


def test_delete_address(address_loader):
    address_loader.load_table('address', 'dim_location')
    address_loader.delete_table('dim_location')
    with address_loader.conn as conn:
        res = conn.execute(text('SELECT * FROM dim_location'))
        rows = res.fetchall()
    assert len(rows) == 0


def test_loader_handler(all_loader, table_info):
    table_name, expected_row_count = table_info
    with all_loader.engine.begin() as conn:
        res = conn.execute(text(f'SELECT * FROM {table_name}'))
        rows = res.fetchall()
    assert len(rows) == expected_row_count
