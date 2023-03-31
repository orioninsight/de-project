import logging


logger = logging.getLogger('MyLogger')
logger.setLevel(logging.INFO)


def load_lambda_handler(event, context):
    loader = Loader()
    loader.load()


class Loader:

    def __init__(self):
        pass
