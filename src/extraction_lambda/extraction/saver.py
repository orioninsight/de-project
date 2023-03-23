import pandas as pd
import logging

logger = logging.getLogger('MyLogger')
logger.setLevel(logging.INFO)


class Saver:
    """A class for saving data to a file."""

    def __init__(self):
        pass

    def save_data(self, data, file_name):
        """Saves the data to a file given file name."""
        if type(data) is not list or data in (None, []):
            logger.error(f"Argument 'data' ({data}) is invalid")
            return False
        if file_name in (None, ''):
            logger.error("Argument 'file_name' ({file_name}) is invalid")
            return False
        try:
            df = pd.DataFrame.from_dict(data, orient="columns", dtype=None)
            df.to_csv(file_name, index=False)
        except Exception as e:
            logger.error(e)
            return False
        return True
