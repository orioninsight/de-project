import pytest
from moto import mock_s3
import os
import boto3
from datetime import datetime
import json

from src.transform_lambda.transform import list_csv_files

bucket_name = f'''test-extraction-bucket-{
    int(datetime.now().timestamp())}'''


@pytest.fixture(scope="function")
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "us-east-1"
    os.environ['OI_STORER_SECRET_STRING'] = json.dumps(
        {'s3_bucket_name': bucket_name})


@mock_s3
def test_list_csv_files_returns_list_of_csv_files(aws_credentials):
    # Set up the mock S3 bucket
    s3 = boto3.resource('s3', region_name='us-east-1')
    s3.create_bucket(Bucket=bucket_name)

    # Upload all the mock CSV files to the bucket
    file_list = ['address', 'design', 'counterparty',
                 'purchase_order', 'staff', 'sales_order',
                 'payment', 'transaction', 'payment_type',
                 'currency', 'department']

    for file_name in file_list:
        s3.Object(bucket_name, f'{file_name}').put(Body=f'{file_name}')

    for file in list_csv_files():
        assert file in file_list


@mock_s3
def test_list_csv_files_raises_exception_missing_files(aws_credentials):
    # Set up the mock S3 bucket
    s3 = boto3.resource('s3', region_name='us-east-1')
    s3.create_bucket(Bucket=bucket_name)

    # Upload all the mock CSV files to the bucket
    file_list = ['address', 'design', 'counterparty',
                 'purchase_order', 'staff', 'sales_order',
                 'payment', 'transaction', 'payment_type',
                 'currency']

    for file_name in file_list:
        s3.Object(bucket_name, f'{file_name}').put(Body=f'{file_name}')

    with pytest.raises(Exception, match='files are not complete'):
        list_csv_files()
