import warnings

from cloudify_rest_client import utils
from cloudify_rest_client.exceptions import CloudifyClientError
from cloudify_rest_client.responses import ListResponse


class NodeInstance(dict):
    """
    Cloudify node instance.
    """

    @property
    def id(self):
        """
        :return: The identifier of the node instance.
        """
        return self.get('id')

    @property
    def node_id(self):
        """
        :return: The identifier of the node whom this is in instance of.
        """
        return self.get('node_id')

    @property
    def relationships(self):
        """
        :return: The node instance relationships.
        """
        return self.get('relationships')

    @property
    def host_id(self):
        """
        :return: The node instance host_id.
        """
        return self.get('host_id')

    @property
    def deployment_id(self):
        """
        :return: The deployment id the node instance belongs to.
        """
        return self.get('deployment_id')

    @property
    def created_by(self):
        """
        :return: The name of the node instance creator.
        """
        return self.get('created_by')

    @property
    def runtime_properties(self):
        """
        :return: The runtime properties of the node instance.
        """
        return self.get('runtime_properties')

    @property
    def system_properties(self):
        """
        :return: The system properties of the node instance.
        """
        return self.get('system_properties')

    @property
    def state(self):
        """
        :return: The current state of the node instance.
        """
        return self.get('state')

    @property
    def version(self):
        """
        :return: The current version of the node instance
         (used for optimistic locking on update)
        """
        return self.get('version')

    @property
    def scaling_groups(self):
        """
        :return: Scaling group instances this node instance belongs to.
        """
        return self.get('scaling_groups', [])

    @property
    def index(self):
        """
        :return: The index of this node instance in
         relation to other node instances of the same node.
        """
        return self.get('index')

    @property
    def is_status_check_ok(self):
        """Has the last status check for this NI succeeded?

        This examines the result of the most recent check_status call
        on this node instance, and returns whether the call succeeded.

        If the result is missing, the result is succeeded.
        """
        return self.get('is_status_check_ok', True)

    @property
    def has_configuration_drift(self):
        """Has this NI's configuration drifted?

        This examines the result of the most recent check_drift call
        on this node instance, and returns whether there was any configuration
        drift reported.

        The instance is drifted if either the instance itself, or any of its
        relationships have drifted.
        """
        return self.get('has_configuration_drift', False)


class NodeInstancesClient(object):

    def __init__(self, api):
        self.api = api
        self._wrapper_cls = NodeInstance
        self._uri_prefix = 'node-instances'

    def create_many(self, deployment_id, node_instances):
        """Create multiple node-instances.

        :param deployment_id: the new instances belong to this deployment
        :param node_instances: list of dicts representing the instances
             to be created. Each node dict must contain at least the
             keys: id, node_id.
        :return: None
        """
        response = self.api.post(
            '/{self._uri_prefix}'.format(self=self),
            data={
                'deployment_id': deployment_id,
                'node_instances': node_instances
            },
            expected_status_code=(201, 204),
        )
        if response:
            return ListResponse(
                [self._wrapper_cls(item) for item in response['items']],
                response['metadata']
            )

    def get(self, node_instance_id, _include=None, evaluate_functions=False):
        """
        Returns the node instance for the provided node instance id.

        :param node_instance_id: The identifier of the node instance to get.
        :param _include: List of fields to include in response.
        :param evaluate_functions: Evaluate intrinsic functions
        :return: The retrieved node instance.
        """
        assert node_instance_id
        uri = '/{self._uri_prefix}/{id}'.format(self=self, id=node_instance_id)
        params = {'_evaluate_functions': evaluate_functions}
        response = self.api.get(uri, params=params, _include=_include)
        return self._wrapper_cls(response)

    def update(self,
               node_instance_id,
               state=None,
               runtime_properties=None,
               version=1,
               force=False,
               relationships=None,
               system_properties=None):
        """
        Update node instance with the provided state & runtime_properties.

        :param node_instance_id: The identifier of the node instance to update.
        :param state: The updated state.
        :param runtime_properties: The updated runtime properties.
        :param system_properties: Like runtime_properties, but only managed
            by Cloudify itself internally.
        :param version: Current version value of this node instance in
         Cloudify's storage (used for optimistic locking).
        :param force: ignore the version check - use with caution
        :param relationships: New relationships to set for the node-instance.
            This is only useful when called from deployment-update: otherwise,
            just setting relationships will not do anything else by itself
            (eg. it won't run the relationships establish operations)
        :return: The updated node instance.
        """
        assert node_instance_id
        uri = '/{self._uri_prefix}/{id}'.format(self=self, id=node_instance_id)
        data = {'version': version}
        if runtime_properties is not None:
            data['runtime_properties'] = runtime_properties
        if system_properties is not None:
            data['system_properties'] = system_properties
        if state is not None:
            data['state'] = state
        if relationships is not None:
            data['relationships'] = relationships
        params = {}
        if force:
            params['force'] = True
        response = self.api.patch(uri, params=params, data=data)
        return NodeInstance(response)

    def _create_filters(
            self,
            sort=None,
            is_descending=False,
            deployment_id=None,
            node_id=None,
            node_name=None,
            **kwargs
    ):
        params = {}
        if node_name:
            warnings.warn("'node_name' filtering capability is deprecated, use"
                          " 'node_id' instead", DeprecationWarning)
            params['node_id'] = node_name
        elif node_id:
            params['node_id'] = node_id
        if deployment_id:
            params['deployment_id'] = deployment_id

        params.update(kwargs)

        if sort:
            params['_sort'] = '-' + sort if is_descending else sort

        return params

    def list(self, _include=None, constraints=None, **kwargs):
        """
        Returns a list of node instances which belong to the deployment
        identified by the provided deployment id.

        :param deployment_id: Optional deployment id to list node instances
                              for.
        :param node_name: Optional node name to only fetch node instances with
                          this name. The node_name positional argument will be
                          deprecated as of the next rest-client version.
                          Use node_id instead.
        :param node_id: Equivalent to node_name.
        :param _include: List of fields to include in response.
        :param sort: Key for sorting the list.
        :param is_descending: True for descending order, False for ascending.
        :param constraints: A list of DSL constraints for node_instance data
                            type to filter the node_instances by.
        :param kwargs: Optional filter fields. for a list of available fields
               see the REST service's models.DeploymentNodeInstance.fields
        :return: Node instances.
        :rtype: list
        """

        params = self._create_filters(**kwargs)
        if constraints is None:
            response = self.api.get('/{self._uri_prefix}'.format(self=self),
                                    params=params,
                                    _include=_include)
        else:
            if _include:
                params['_include'] = ','.join(_include)
            response = self.api.post(
                '/searches/{self._uri_prefix}'.format(self=self),
                params=params,
                data={'constraints': constraints}
            )

        return ListResponse(
            [self._wrapper_cls(item) for item in response['items']],
            response['metadata']
        )

    def search(self, ids, all_tenants=False):
        """Search node instances by their IDs.

        :param ids: list of ids to search by
        :param all_tenants: search node-instances of all tenants
        :return: Node instances.
        :rtype: list
        """
        params = {}
        if all_tenants:
            params['_all_tenants'] = True
        response = self.api.post('/searches/node-instances', data={
            'filter_rules': [{
                'key': 'id',
                'values': ids,
                'operator': 'any_of',
                'type': 'attribute'
            }]
        }, params=params)
        return ListResponse(
            [self._wrapper_cls(item) for item in response['items']],
            response['metadata']
        )

    def delete(self, instance_id):
        """Delete a node-instance

        This is only useful from within deployment-update. You don't want
        to delete node-instances otherwise: the state of the deployment
        could be made inconsistent.

        :param instance_id: ID of the instance to be deleted
        """
        self.api.delete(
            '/{self._uri_prefix}/{instance_id}'
            .format(self=self, instance_id=instance_id),
            expected_status_code=204,
        )

    def dump(self, deployment_ids=None, get_broker_conf=None,
             node_instance_ids=None):
        """Generate node instances' attributes for a snapshot.

        :param deployment_ids: A list of deployments' identifiers used to
         select node instances to be dumped, should not be empty.
        :param get_broker_conf: A function used to retrieve broker
         configuration.
        :param node_instance_ids: A list of node instances' identifiers, if
         not empty, used to select specific node instances to be dumped.
        :returns: A generator of dictionaries, which describe node instances'
         attributes.
        """
        if not deployment_ids:
            return
        for deployment_id in deployment_ids:
            for entity in utils.get_all(
                    self.api.get,
                    f'/{self._uri_prefix}',
                    params={'deployment_id': deployment_id},
                    _include=['id', 'runtime_properties', 'state',
                              'relationships', 'system_properties',
                              'scaling_groups', 'host_id', 'index',
                              'visibility', 'node_id', 'created_by',
                              'has_configuration_drift', 'is_status_check_ok'],
            ):
                if get_broker_conf:
                    # for "agent" node instances, store broker config in
                    # runtime-props as well, so that during agent upgrade, we
                    # can connect to the old rabbitmq. This is later analyzed
                    # by snapshot_restore, _inject_broker_config, and by
                    # several calls in cloudify-agent/operations.py (related
                    # to creating the AMQP client there)
                    rp = entity.get('runtime_properties') or {}
                    if 'cloudify_agent' in rp:
                        broker_conf = get_broker_conf(entity)
                        rp['cloudify_agent'].update(broker_conf)
                if not node_instance_ids or entity['id'] in node_instance_ids:
                    yield {'__entity': entity, '__source_id': deployment_id}

    def restore(self, entities, logger, deployment_id,
                inject_broker_conf=None):
        """Restore node instances from a snapshot.

        :param entities: An iterable (e.g. a list) of dictionaries describing
         node instances to be restored.
        :param logger: A logger instance.
        :param deployment_id: A deployment identifier for the entities.
        :param inject_broker_conf: A function used to inject broker
         configuration for given node_instance's runtime_properties.
        """
        for entity in entities:
            entity['creator'] = entity.pop('created_by')
            if inject_broker_conf:
                inject_broker_conf(entity['runtime_properties'])
        try:
            self.create_many(deployment_id, entities)
        except CloudifyClientError as exc:
            logger.error(f'Error restoring node instances: {exc}')
