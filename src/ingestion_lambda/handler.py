import logging


def example_handler(event, context):
    logger = logging.getLogger('MyLogger')
    logger.setLevel(logging.INFO)

    logger.error(f'testing: lambda has successfully ran')

    print('We think this works :)')
