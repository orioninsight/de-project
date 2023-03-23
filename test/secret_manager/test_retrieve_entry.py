from src.extraction_lambda.secret_manager.retrieve_entry import retrieve_entry
import boto3
import ast
from moto import mock_secretsmanager
import pytest
import os
import json
import botocore


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
def test_retrieve_secret(aws_credentials):
    test_client = boto3.client('secretsmanager')
    test_client.create_secret(
        Name="test1",
        SecretString=f'{{"user": "user1", "password": "password1"}}'
    )
    retrieve_entry("test1")
    expected = {"user": "user1", "password": "password1"}
    # f = open("secrets.txt", "r")
    # result = f.read()
    read = json.loads(retrieve_entry("test1"))
    assert read["user"] == expected["user"]
    assert read["password"] == expected["password"]
    assert expected["user"] == read["user"]


@mock_secretsmanager
def test_retrieve_entry_raises_exception():
    secret_id = ""

    # act
    with pytest.raises(botocore.exceptions.ParamValidationError):
        retrieve_entry(secret_id)
