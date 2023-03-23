from pathlib import Path
from unittest.mock import patch
import pytest
from secret_manager.retrieve_entry import retrieve_entry
from extraction.saver import Saver
from extraction.extractor import Extractor
import json
import os


@pytest.fixture(scope='function')
def extractor():
    creds = (os.environ['OI_TOTESYS_SECRET_STRING']
             if 'OI_TOTESYS_SECRET_STRING' in os.environ
             else retrieve_entry('totesys_db'))
    return Extractor(**json.loads(creds))


@pytest.fixture(scope='function')
def saver():
    return Saver()


@pytest.fixture(scope='function')
def test_file():
    file_name = '_test_file.csv'
    yield file_name
    file = Path(file_name)
    if file.is_file():
        file.unlink()


@patch('extraction.extractor.Extractor.extract_address')
def test_save_data_in_file(mock_extract_address, extractor, saver, test_file):
    # Arrange
    mock_extract_address.return_value = [{
        'address_id': '1', 'address_line_1': 'abc', 'address_line_2': 'efg',
        'district': 'hij', 'city': 'klm', 'postal_code': 'nop',
        'country': 'qrs', 'phone': 'tuv', 'created_at': '2023-03-22',
        'last_updated': '2023-03-23'}]
    # Act
    addresses = extractor.extract_address()
    res = saver.save_data(addresses, test_file)
    file = Path(test_file)

    # Assert
    assert res
    mock_extract_address.assert_called_once()
    assert file.is_file()
    with open(file, 'r', encoding='utf-8') as f:
        assert set(f.readline()[:-1].split(',')) == {
            'address_id', 'address_line_1', 'address_line_2',
            'district', 'city', 'postal_code', 'country',
            'phone', 'created_at', 'last_updated'}


@patch('pandas.DataFrame.from_dict', side_effect=[
    Exception('ERROR CONVERTING FROM DICT TO PANDAS DATAFRAME')])
def test_returns_false_on_error(mock_from_dict, saver, extractor, test_file):
    addresses = extractor.extract_address()
    assert not saver.save_data(addresses, test_file)


def test_returns_false_given_invalid_arguments(saver):
    assert not saver.save_data('NOT A DICTIONARY', 'file.txt')
    assert not saver.save_data({}, 'file.txt')
    assert not saver.save_data(None, 'file.txt')
    assert not saver.save_data([{'a': 12}], None)
    assert not saver.save_data([{'a': 12}], '')
