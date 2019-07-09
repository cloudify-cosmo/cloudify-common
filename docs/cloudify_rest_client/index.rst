.. cloudify-cli documentation master file, created by
   sphinx-quickstart on Thu Jun 12 15:30:03 2014.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Welcome to cloudify-rest-client's documentation!
================================================

What is it?
-----------
This rest client provides access to the REST API exposed by a `Cloudify manager <https://docs.cloudify.co/latest/developer/apis/rest-service/>`_.


Basic Usage
------------
This client's API tries to be as consistent as possible when accessing different resource types. The example below will fetch the blueprints currently uploaded to the manager.

.. code-block:: python

   from cloudify_rest_client import CloudifyClient

   client = CloudifyClient('MANAGER_HOSTNAME_OR_IP')
   blueprints = client.blueprints.list()

   for blueprint in blueprints:
      print blueprint.id

Contents:

.. toctree::
   :maxdepth: 2

   blueprints
   clients
   deployments
   deployment_modifications
   deployment_updates
   events
   exceptions
   executions
   manager
   node_instances
   nodes
   evaluate
   tokens
   plugins_updates


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
