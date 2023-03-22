import pandas as pd


class Saver:
    """A class for saving data to a file."""
    def __init__(self):
        pass
    def save(self, data, key):
        """Saves the data to a file with the key."""

    def __init__(self):
        pass

    def save(self, data, key):
        with open('key', 'wb') as file:
            file.write(data)

# pandas.DataFrame.from_dict: access a dictionary, keys will be used to label columns and the values will be added to column as data. 

"""Converts dictionary to DataFrame"""
converted_data = pd.DataFrame.from_dict(data, orient="columns", dtype=None) 

#we need to add our data as parameter here , i  used columns as default but can be changed.  i used dtype as None which is default.
# we can then use pandas to save to csv file

converted_data.to_csv("transfered data")
# or to json
converted_data.to_json("transfered data")