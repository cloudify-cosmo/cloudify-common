__author__ = 'dank'


class DeploymentNodes:

    def __init__(self):
        self.swaggerTypes = {
            'deploymentId': 'str',
            'nodes': 'list[DeploymentNode]'
        }

        self.nodes = None  # list[DeploymentNode]
        self.deploymentId = None  # str
