from pathlib import Path
from unittest.mock import patch
from src.extraction.extract_db import (extract_db_handler)
from src.extraction.extractor import Extractor
from src.extraction.saver import Saver
from src.extraction.storer import Storer
import pytest
from moto import mock_s3
import boto3
import os
from datetime import datetime

S3_TEST_BUCKET_NAME = f'test-extraction-bucket-{int(datetime.now().timestamp())}'


@pytest.fixture(scope="function")
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "us-east-1"


@pytest.fixture(scope='function')
def storer_info():
    os.environ['OI_STORER_SECRET_STRING'] = f'{{"s3_bucket_name":"{S3_TEST_BUCKET_NAME}"}}'


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


@pytest.fixture(scope='function', params=['address', 'design'])
def downloaded_file(request):
    file_name = f"_test_{request.param}.csv"
    yield request.param, file_name
    file = Path(file_name)
    if file.is_file():
        file.unlink()


@patch('src.extraction.extract_db.logger.error')
@patch('src.extraction.extract_db.retrieve_entry', side_effect=Exception('ERROR!'))
def test_raises_runtime_exception_on_error(mock_retrieve_entry, mock_logger_error):
    with pytest.raises(Exception) as err_info:
        extract_db_handler({'extract_table': ['UNSUPPORTED_TABLE']}, None)
    mock_logger_error.assert_called_once_with(
        'An error occurred extracting the data: ERROR!')


def test_raises_unsupported_table_exception(storer_info):
    with pytest.raises(Exception, match='Unsupported table') as err_info:
        extract_db_handler({'extract_table': ['UNSUPPORTED_TABLE']}, None)


def test_raises_exception_given_empty_or_invalid_lambda_payload():
    with pytest.raises(Exception, match='payload requires list in') as err_info:
        extract_db_handler(None, None)
    with pytest.raises(Exception, match='payload requires list in') as err_info:
        extract_db_handler({'INVALID': 'PAYLOAD'}, None)


def test_extracts_from_db_then_saves_file_and_stores_file_in_s3(s3, storer_info, downloaded_file):
    table_name, file_name = downloaded_file
    extract_db_handler({'extract_table': [table_name]}, None)
    s3.download_file(S3_TEST_BUCKET_NAME,
                     table_name, file_name)
    with open(file_name, 'r', encoding='utf-8') as f:
        assert f'{table_name}_id' in f.readline().split(',')
        assert len(f.readlines()) > 0
