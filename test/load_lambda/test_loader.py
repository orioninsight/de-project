from moto import mock_s3, mock_rds
import boto3
from datetime import datetime
import os
import json
import pandas as pd
import pytest
from src.load_lambda.load import Loader
from pandas.testing import assert_frame_equal
import sqlite3
from unittest.mock import patch
from sqlalchemy import create_engine,text


processed_bucket_name =\
    f'test-processed-bucket-{int(datetime.now().timestamp())}'
script_path = os.path.abspath(__file__)
script_dir = os.path.dirname(script_path)
TEST_DATA_PATH = f'{script_dir}/data'

TEST_DB = 'test.db'

@pytest.fixture(scope="function")
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "us-east-1"
    os.environ['OI_PROCESSED_INFO'] = json.dumps(
        {'s3_bucket_name': processed_bucket_name})


@pytest.fixture(scope="function")
def s3(aws_credentials):
    with mock_s3():
        file_list = ['address', 'design', 'counterparty',
                     'staff', 'sales_order',
                     'currency', 'date']

        s3_client = boto3.client("s3")
        s3_client.create_bucket(Bucket=processed_bucket_name)
        for file_name in file_list:
            with open(f'{TEST_DATA_PATH}/{file_name}.parq', 'rb') as f:
                s3_client.put_object(Bucket=processed_bucket_name,
                                     Key=f'{file_name}', Body=f)
        yield s3_client


@pytest.fixture(scope='function')
def loader(s3):
    loader = Loader()
    loader.conn = create_engine('sqlite:///test.db').connect()
    yield loader
    loader.close()

@pytest.fixture(scope='function')
def live_loader(s3):
    loader = Loader()
    loader.connect_db(**json.loads(os.environ.get('OI_TOTESYS_DW_INFO')))
    yield loader
    with loader.engine.begin() as conn:
       res = conn.execute(text('DELETE FROM dim_location'))
    loader.close()


def test_read_s3_parquet_returns_data_frame(loader):
    for file in loader.FILE_LIST:
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
    

def test_load_address(live_loader):
    live_loader.load_table('address','dim_location')
    with live_loader.conn as conn:
       res = conn.execute(text('SELECT * FROM dim_location'))
       rows = res.fetchall()
    assert len(rows) == 30
    
def test_delete_address(live_loader):
    live_loader.load_table('address','dim_location')
    live_loader.delete_table('dim_location')
    with live_loader.conn as conn:
       res = conn.execute(text('SELECT * FROM dim_location'))
       rows = res.fetchall()
    assert len(rows) == 0
