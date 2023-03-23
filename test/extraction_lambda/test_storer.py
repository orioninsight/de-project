from pathlib import Path
import pytest
from moto import mock_s3
import os
import boto3
from src.extraction_lambda.extraction.extractor import Extractor
from src.extraction_lambda.extraction.saver import Saver
from src.extraction_lambda.extraction.storer import Storer

S3_TEST_BUCKET_NAME = "test-bucket"


@pytest.fixture(scope="function")
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "us-east-1"


@pytest.fixture(scope="function")
def s3(aws_credentials):
    with mock_s3():
        s3_client = boto3.client("s3")
        s3_client.create_bucket(Bucket=S3_TEST_BUCKET_NAME)
        yield s3_client


@pytest.fixture(scope='function')
def extractor():
    return Extractor()


@pytest.fixture(scope='function')
def saver():
    return Saver()


@pytest.fixture(scope='function')
def test_file():
    file_name = '_test_file.csv'
    file = Path(file_name)
    with open(file, 'w', encoding='utf-8') as f:
        f.write('THIS,IS,A,TEST,FILE,FOR,S3')
    yield file_name
    file.unlink()


@pytest.fixture(scope='function')
def storer(s3):
    return Storer(S3_TEST_BUCKET_NAME)


@pytest.fixture(scope='function')
def downloaded_file():
    file_name = "_test_download.csv"
    yield file_name
    file = Path(file_name)
    if file.is_file():
        file.unlink()


def test_stores_data_into_s3_bucket(s3, storer, test_file, downloaded_file):
    obj_key = 'table.csv'
    res = storer.store_file(test_file, obj_key)
    assert res
    s3.download_file(S3_TEST_BUCKET_NAME,
                     obj_key, downloaded_file)
    with open(downloaded_file, 'r', encoding='utf-8') as f:
        assert f.readline().split(',') == ['THIS',
                                           'IS', 'A', 'TEST', 'FILE', 'FOR', 'S3']


def test_returns_false_on_bucket_name_error(storer, test_file):
    storer.s3_bucket_name = 'NO_SUCH_BUCKET'
    res = storer.store_file(test_file, 'DIFFERENT_KEY')
    assert not res


def test_returns_false_given_invalid_arguments(storer):
    assert not storer.store_file(123, 'KEY')
    assert not storer.store_file(None, 'KEY')
    assert not storer.store_file('', 'KEY')
    assert not storer.store_file('file.txt', '')
    assert not storer.store_file('file.txt', None)
