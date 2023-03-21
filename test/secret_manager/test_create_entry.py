import boto3
import botocore
import pytest
from moto import mock_secretsmanager
import json
import os
from src.secret_manager.create_entry import create_entry


@pytest.fixture(scope="function")
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "us-east-1"


@mock_secretsmanager
# @pytest.fixture(scope="function")
def test_create_entry_returns_success_message():
    secret_id = "testSId"
    host = 'testurl'
    port = '5432'
    user = "Joe"
    password = "123"
    database = 'testdb'
    assert create_entry(secret_id, host, port, user,
                        password, database) == "Secret saved."


@mock_secretsmanager
# @pytest.fixture(scope="function")
def test_create_entry_creates_new_secret():
    # arrange
    secret_id = "testSId"
    host = 'testurl'
    port = '5432'
    user = "Joe"
    password = "123"
    database = 'testdb'

    client = boto3.client('secretsmanager')

    # act
    create_entry(secret_id, host, port, user, password, database)
    response = ""
    try:
        response = client.get_secret_value(SecretId=secret_id)

    except Exception as e:
        assert e == ""

    secret = json.loads(response['SecretString'])
    # assert
    assert secret_id == response['Name']
    assert user == secret['user']
    assert password == secret['password']
    assert database == secret['database']


@mock_secretsmanager
def test_create_entry_raises_exception():
    secret_id = ""
    host = 'testurl'
    port = '5432'
    user = "Joe"
    password = "123"
    database = 'testdb'
    client = boto3.client('secretsmanager')

    # act
    with pytest.raises(botocore.exceptions.ParamValidationError):
        create_entry(secret_id, host, port, user, password, database)
