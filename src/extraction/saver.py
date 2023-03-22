
class Saver:

    def __init__(self):
        pass

    def save(self, data, key):
        with open('key', 'wb') as file:
            file.write(data)
