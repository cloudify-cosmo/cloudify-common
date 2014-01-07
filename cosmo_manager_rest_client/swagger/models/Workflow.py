__author__ = 'ran'


class Workflow:

    def __init__(self):
        self.swaggerTypes = {
            'id': 'str',
            'deploymentId': 'str',
            'blueprintId': 'str',
            'createdAt': 'date-time'
        }

        self.id = None # str
        self.deploymentId = None # str
        self.blueprintId = None # str
        self.createdAt = None # date-time