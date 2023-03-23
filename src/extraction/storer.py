import boto3
from botocore.exceptions import ClientError, ParamValidationError
import logging


logger = logging.getLogger('MyLogger')
logger.setLevel(logging.INFO)


class Storer:

    def __init__(self, s3_bucket_name):
        self.s3_bucket_name = s3_bucket_name
        self.s3_client = boto3.client('s3')

    def store_file(self, file_name, key):
        if type(file_name) is not str or file_name in (None, ''):
            logger.error(f"Invalid 'file_name' ({file_name})")
            return False
        if type(key) is not str or key in (None, ''):
            logger.error(f"Invalid 'key' ({key})")
            return False
        try:
            self.s3_client.upload_file(file_name, self.s3_bucket_name, key)
        except ParamValidationError as e:
            logger.error(e)
            return False
        except ClientError as e:
            logger.error(e)
            return False
        except Exception as e:
            logger.error(e)
            return False
        return True
