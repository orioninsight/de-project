from pathlib import Path
from unittest.mock import patch
from extract_db import (extract_db_handler, VALID_TABLES)
from extraction.extractor import Extractor
import pytest
import boto3
import os
from datetime import datetime

S3_TEST_BUCKET_NAME = f'''test-extraction-bucket-{
    int(datetime.now().timestamp())}'''


@pytest.fixture(scope='function')
def storer_info():
    os.environ['OI_STORER_SECRET_STRING'] = f'''{{"s3_bucket_name":
                                        "{S3_TEST_BUCKET_NAME}"}}'''


@pytest.fixture(scope="function")
def s3():
    s3_client = boto3.client("s3")
    s3_client.create_bucket(Bucket=S3_TEST_BUCKET_NAME)
    yield s3_client
    objs = s3_client.list_objects_v2(Bucket=S3_TEST_BUCKET_NAME)['Contents']
    for obj in objs:
        s3_client.delete_object(Bucket=S3_TEST_BUCKET_NAME, Key=obj['Key'])
    s3_client.delete_bucket(Bucket=S3_TEST_BUCKET_NAME)


@pytest.fixture(scope='function')
def extractor():
    return Extractor()


@pytest.fixture(scope='function', params=['address', 'design', 'counterparty',
                                          'purchase_order', 'staff',
                                          'sales_order', 'payment',
                                          'transaction', 'payment_type',
                                          'currency', 'department'])
def downloaded_file(request):
    file_name = f"/tmp/{request.param}.csv"
    yield request.param, file_name
    file = Path(file_name)
    if file.is_file():
        file.unlink()


def test_raises_unsupported_table_exception(storer_info):
    with pytest.raises(Exception, match='Unsupported table'):
        extract_db_handler({'extract_table': ['UNSUPPORTED_TABLE']}, None)


def test_raises_exception_given_lambda_payload():
    with pytest.raises(Exception, match='payload requires list in'):
        extract_db_handler({'extract_table': 123}, None)


def test_extracts_db_table_and_stores_file_in_s3(s3, storer_info,
                                                 downloaded_file):
    table_name, file_name = downloaded_file
    extract_db_handler({'extract_table': [table_name]}, None)
    s3.download_file(S3_TEST_BUCKET_NAME,
                     table_name, file_name)
    with open(file_name, 'r', encoding='utf-8') as f:
        assert f'{table_name}_id' in f.readline().split(',')
        assert len(f.readlines()) > 0


@patch('extract_db.extract_db_helper')
def test_extracts_all_db_tables_given_no_payload(mock_db_helper):
    extract_db_handler({}, None)
    mock_db_helper.assert_called_once_with(VALID_TABLES)
