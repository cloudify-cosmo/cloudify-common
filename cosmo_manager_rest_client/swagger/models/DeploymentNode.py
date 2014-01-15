__author__ = 'dank'


class DeploymentNode:

    def __init__(self):
        self.swaggerTypes = {
            'id': 'str',
            'reachable': 'bool'
        }

        self.id = None  # str
        self.reachable = None  # bool
