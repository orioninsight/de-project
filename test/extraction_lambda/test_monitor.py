from extraction.monitor import Monitor
import pytest
import os
import json

# monitor returns boolean

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

@pytest.fixture(scope="function")
def monitor():
    yield Monitor(S3_TEST_BUCKET_NAME)


def test_monitor_has_changed_returns_true(monitor):
    assert monitor.has_changed()
    
def test_monitor_compare_state_returns_false_given_unchanged_state(monitor):
    new_state ={"tup_inserted":0,"tup_updated":0, "tup_deleted":0}
    assert not monitor.has_state_changed(new_state)
    
def test_monitor_compare_state_returns_true_given_changed_state(monitor):
    new_state ={"tup_inserted":2,"tup_updated":0, "tup_deleted":2}
    assert monitor.has_state_changed(new_state)
    
def test_monitor_get_db_stats(monitor):
    assert set(monitor.get_db_state[0].keys()) == {
        "tup_inserted","tup_updated","tup_deleted"}
