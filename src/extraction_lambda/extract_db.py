import logging


def  extract_db_handler(event, context):
    logger = logging.getLogger('MyLogger')
    logger.setLevel(logging.INFO)

    logger.error('testing: error!')

    print('We think this works :)')