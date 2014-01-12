__author__ = 'ran'


class Workflows:

    def __init__(self):
        self.swaggerTypes = {
            'workflows': 'list[Workflow]',
            'deploymentId': 'str',
            'blueprintId': 'str',
        }

        self.workflows = None  # list[Workflow]
        self.deploymentId = None  # str
        self.blueprintId = None  # str
