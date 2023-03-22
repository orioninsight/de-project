from unittest.mock import patch
from src.extraction.extract_db import (extract_db_handler)
from src.extraction.extractor import Extractor
import pytest
import os


@pytest.fixture(scope="function")
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "us-east-1"


@pytest.fixture(scope='function')
def extractor():
    return Extractor()


@patch('src.extraction.extract_db.logger.error')
@patch('src.extraction.extract_db.retrieve_entry', side_effect=Exception('ERROR!'))
def test_raises_runtime_exception_on_error(mock_retrieve_entry, mock_logger_error):
    with pytest.raises(Exception) as err_info:
        extract_db_handler(None, None)
    mock_retrieve_entry.assert_called_once()
    mock_logger_error.assert_called_once_with(
        'An error occurred extracting the data: ERROR!')
