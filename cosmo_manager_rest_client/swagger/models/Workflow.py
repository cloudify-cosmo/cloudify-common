__author__ = 'ran'


class Workflow:

    def __init__(self):
        self.swaggerTypes = {
            'name': 'str',
            'createdAt': 'date-time'
        }

        self.name = None # str
        self.createdAt = None # date-time