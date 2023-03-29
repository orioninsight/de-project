import pytest
from moto import mock_s3
import os
import boto3
from datetime import datetime
import json
import pandas as pd
from pandas.testing import assert_frame_equal

from src.transform_lambda.transform import read_csv

bucket_name = f'test-extraction-bucket-{int(datetime.now().timestamp())}'


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
def test_read_csv_returns_data_frames(aws_credentials):

    s3_client = boto3.client('s3')
    s3_client.create_bucket(Bucket=bucket_name)

    # Upload all the mock CSV files to the bucket
    file_list = ['address', 'design', 'counterparty',
                 'purchase_order', 'staff', 'sales_order',
                 'payment', 'transaction', 'payment_type',
                 'currency', 'department']
    csv_data = '''a,b,c,d\n
                  1,2,3,4
                    '''

    # for file_name in file_list:
    #     s3.Object(bucket_name, f'{file_name}').put(Body=f'{csv_data}')

    for file_name in file_list:
        s3_client.put_object(Bucket=bucket_name,
                             Key=f'{file_name}', Body=csv_data.encode('utf-8'))
    s3_client.put_object(Bucket=bucket_name,
                         Key='db_state', Body='extra file'.encode('utf-8'))
    result = read_csv('department')['department']

    # Define the expected dataframe

    expected_df = pd.DataFrame(data={'a': [1], 'b': [2], 'c': [3], 'd': [4]})
    # Test that the dataframe is equal to the expected dataframe
    assert_frame_equal(result, expected_df)
