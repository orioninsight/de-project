import logging


def extract_db_handler(event, context):
    logger = logging.getLogger('MyLogger')
    logger.setLevel(logging.INFO)

    logger.info(f'testing: lambda has successfully ran')

    print('Hola')
