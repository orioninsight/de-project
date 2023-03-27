from datetime import datetime
import logging
import boto3
import json
from botocore.exceptions import ClientError

logger = logging.getLogger('MyLogger')
logger.setLevel(logging.INFO)


class Monitor:
    DB_STATE_KEY = 'db_state'

    def __init__(self, s3_bucket_name, extractor):
        self.s3_bucket_name = s3_bucket_name
        self.s3_client = boto3.client('s3')
        self.current_state = None
        self.new_state = None
        self.extractor = extractor

    def has_state_changed(self):
        self.get_db_stats()
        return self.current_state != self.new_state

    def get_db_stats(self):
        self.new_state = self.extractor.extract_db_stats()

    def save_state(self):
        pass

    def get_current_state(self):
        stat_keys = ['tup_inserted', 'tup_updated', 'tup_deleted']
        try:
            db_stats = self.s3_client.get_object(Bucket=self.s3_bucket_name,
                                                 Key=Monitor.DB_STATE_KEY)
            stats = json.loads(db_stats['Body'].read())
            for stat_key in stat_keys:
                if not (stat_key in stats
                        and type(stats[stat_key]) is int):
                    raise Exception("S3 object db_state has missing/invalid"
                                    f' JSON entry: ({stat_key}:{stats[stat_key]})')
            self.current_state = stats
            self.current_state['retrieved_at'] = datetime.now().timestamp()
        except ClientError as e:
            if e.response['Error']['Code'] == 'NoSuchKey':
                logger.error(e)
                return -1
        except Exception as e:
            logger.error(e)
            return 0
        return 1
            
