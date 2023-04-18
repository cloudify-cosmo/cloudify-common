import os
import json
import requests

from cloudify_rest_client.constants import VisibilityState

from cloudify import constants, utils
from cloudify.state import ctx, workflow_ctx, NotInContext
from cloudify.exceptions import (HttpException,
                                 NonRecoverableError)
from cloudify.cluster import CloudifyClusterClient


class NodeInstance(object):
    """
    Represents a deployment node instance.
    An instance of this class contains runtime information retrieved
    from Cloudify's runtime storage as well as the node's state.
    """
    def __init__(self,
                 node_instance_id,
                 node_id,
                 runtime_properties=None,
                 state=None,
                 version=None,
                 host_id=None,
                 relationships=None,
                 index=None,
                 scaling_groups=None,
                 system_properties=None):
        self.id = node_instance_id
        self._node_id = node_id
        self._runtime_properties = \
            DirtyTrackingDict((runtime_properties or {}).copy())
        self._state = state
        self._version = version
        self._host_id = host_id
        self._relationships = relationships
        self._index = index
        self._scaling_groups = scaling_groups
        self._system_properties = system_properties

    def get(self, key):
        return self._runtime_properties.get(key)

    def put(self, key, value):
        self._runtime_properties[key] = value

    def delete(self, key):
        del self._runtime_properties[key]

    __setitem__ = put

    __getitem__ = get

    __delitem__ = delete

    def __contains__(self, key):
        return key in self._runtime_properties

    @property
    def runtime_properties(self):
        """
        The node instance runtime properties.

        To update the properties, make changes on the returned dict and call
        ``update_node_instance`` with the modified instance.
        """
        return self._runtime_properties

    @runtime_properties.setter
    def runtime_properties(self, new_properties):
        # notify the old object of the changes - trigger a .modifiable check
        self._runtime_properties._set_changed()

        self._runtime_properties = DirtyTrackingDict(new_properties)
        self._runtime_properties._set_changed()

    @property
    def version(self):
        return self._version

    @property
    def state(self):
        """
        The node instance state.

        To update the node instance state, change this property value and
        call ``update_node_instance`` with the modified instance.
        """
        return self._state

    @state.setter
    def state(self, value):
        self._state = value

    @property
    def dirty(self):
        return self._runtime_properties.dirty

    @property
    def host_id(self):
        return self._host_id

    @property
    def node_id(self):
        return self._node_id

    @property
    def relationships(self):
        return self._relationships

    @property
    def index(self):
        return self._index

    @property
    def scaling_groups(self):
        return {g['id']: g['name'] for g in self._scaling_groups}

    @property
    def system_properties(self):
        return self._system_properties


def get_rest_client(tenant=None, api_token=None):
    """
    :param tenant: optional tenant name to connect as
    :param api_token: optional api_token to authenticate with (instead of
            using REST token)
    :returns: A REST client configured to connect to the manager in context
    :rtype: cloudify_rest_client.CloudifyClient
    """

    if not tenant:
        tenant = utils.get_tenant_name()

    # Handle maintenance mode
    headers = {}
    if utils.get_is_bypass_maintenance():
        headers['X-BYPASS-MAINTENANCE'] = 'True'

    # If api_token or execution_token was provided no need to use REST token
    token = None
    execution_token = utils.get_execution_token()
    if execution_token:
        headers[constants.CLOUDIFY_EXECUTION_TOKEN_HEADER] = execution_token
    elif api_token:
        token = api_token
    else:
        token = utils.get_rest_token()

    return CloudifyClusterClient(
        headers=headers,
        host=utils.get_manager_rest_service_host(),
        port=utils.get_manager_rest_service_port(),
        tenant=tenant,
        token=token,
        protocol=utils.get_manager_rest_service_protocol(),
        cert=utils.get_local_rest_certificate(),
        kerberos_env=utils.get_kerberos_indication(
            os.environ.get(constants.KERBEROS_ENV_KEY))
    )


def _save_resource(logger, resource, resource_path, target_path):
    if not target_path:
        target_path = os.path.join(utils.create_temp_folder(),
                                   os.path.basename(resource_path))
    with open(target_path, 'wb') as f:
        f.write(resource)
    logger.info("Downloaded %s to %s" % (resource_path, target_path))
    return target_path


def download_resource_from_manager(resource_path, logger, target_path=None):
    """
    Download resource from the manager file server.

    :param resource_path: path to resource on the file server
    :param logger: logger to use for info output
    :param target_path: optional target path for the resource
    :returns: path to the downloaded resource
    """
    resource = get_resource_from_manager(resource_path)
    return _save_resource(logger, resource, resource_path, target_path)


def download_resource(blueprint_id,
                      deployment_id,
                      tenant_name,
                      resource_path,
                      logger,
                      target_path=None):
    """
    Download resource from the manager file server with path relative to
    the deployment or blueprint denoted by ``deployment_id`` or
    ``blueprint_id``

    An attempt will first be made for getting the resource from the deployment
    folder. If not found, an attempt will be made for getting the resource
    from the blueprint folder.

    :param blueprint_id: the blueprint id of the blueprint to download the
                         resource from
    :param deployment_id: the deployment id of the deployment to download the
                          resource from
    :param tenant_name: the resource's tenant
    :param resource_path: path to resource relative to blueprint or deployment
                          folder
    :param logger: logger to use for info output
    :param target_path: optional target path for the resource
    :returns: path to the downloaded resource
    """
    if _is_resource_origin_from_imported_blueprint(resource_path):
        # If from a local blueprint or loaded blueprint import
        namespace, resource_path = _extract_resource_parts(resource_path)

        client = get_rest_client()
        namespaces_mapping = client.blueprints.get(
            blueprint_id, ['plan']).plan['namespaces_mapping']
        if namespace in namespaces_mapping:
            blueprint_id = namespaces_mapping[namespace]

    resource = get_resource(blueprint_id,
                            deployment_id,
                            tenant_name,
                            resource_path)
    return _save_resource(logger, resource, resource_path, target_path)


def _is_resource_origin_from_imported_blueprint(resource_path):
    return constants.NAMESPACE_BLUEPRINT_IMPORT_DELIMITER in resource_path


def _extract_resource_parts(resource_path):
    """
    A resource path can be with namespace, which will create the following
    structure: <namespace><namespace-delimiter><the actual path>. So this
    function will separate to the individual components.
    """
    namespace, _, resource_path =\
        resource_path.rpartition(
            constants.NAMESPACE_BLUEPRINT_IMPORT_DELIMITER)
    return namespace, resource_path


def get_resource_from_manager(resource_path,
                              base_url=None,
                              base_urls=None):
    """Get resource from the manager file server.

    :param resource_path: path to resource on the file server
    :param base_url: The base URL to manager file server. Deprecated.
    :param base_urls: A list of base URL to cluster manager file servers.
    :param resource_path: path to resource on the file server.
    :returns: resource content
    """
    base_urls = base_urls or []
    base_urls += utils.get_manager_file_server_url()
    if base_url is not None:
        base_urls.insert(0, base_url)

    # if we have multiple managers to try, set connect_timeout so that
    # we're not waiting forever for a single non-responding manager
    if len(base_urls) > 1:
        timeout = (10, None)
    else:
        timeout = None

    verify = utils.get_local_rest_certificate()
    headers = {}
    try:
        headers[constants.CLOUDIFY_EXECUTION_TOKEN_HEADER] = \
            ctx.execution_token
    except NotInContext:
        headers[constants.CLOUDIFY_EXECUTION_TOKEN_HEADER] = \
            workflow_ctx.execution_token

    for ix, next_url in enumerate(base_urls):
        url = '{0}/{1}'.format(next_url.rstrip('/'), resource_path.lstrip('/'))
        try:
            response = requests.get(
                url, verify=verify, headers=headers, timeout=timeout)
        except requests.ConnectionError:
            continue
        if not response.ok:
            is_last = (ix == len(base_urls) - 1)
            if not is_last:
                # if there's more managers to try, try them: due to filesystem
                # replication lag, they might have files that the previous
                # manager didn't
                continue
            raise HttpException(url, response.status_code, response.reason)
        return response.content

    raise NonRecoverableError(
        'Failed to download {0}: unable to connect to any manager (tried: {1})'
        .format(resource_path, ', '.join(base_urls))
    )


def _resource_paths(blueprint_id, deployment_id, tenant_name, resource_path,
                    use_global=True):
    """For the given resource_path, generate all firesever paths to try.

    Eg. for path of "foo.txt", generate:
        - /resources/deployments/default_tenant/dep1/foo.txt
        - /resources/blueprints/default_tenant/bp1/foo.txt
        - /foo.txt
    """
    if deployment_id:
        yield os.path.join(
            constants.FILE_SERVER_DEPLOYMENTS_FOLDER,
            tenant_name,
            deployment_id,
            resource_path
        ).replace('\\', '/')

    if blueprint_id:
        client = get_rest_client()
        blueprint = client.blueprints.get(blueprint_id)
        if blueprint['visibility'] == VisibilityState.GLOBAL:
            tenant_name = blueprint['tenant_name']

        yield os.path.join(
            constants.FILE_SERVER_BLUEPRINTS_FOLDER,
            tenant_name,
            blueprint_id,
            resource_path
        ).replace('\\', '/')

    if use_global:
        yield resource_path


def get_resource(blueprint_id, deployment_id, tenant_name, resource_path):
    """
    Get resource from the manager file server with path relative to
    the deployment or blueprint denoted by ``deployment_id`` or
    ``blueprint_id``.

    An attempt will first be made for getting the resource from the deployment
    folder. If not found, an attempt will be made for getting the resource
    from the blueprint folder.

    :param blueprint_id: the blueprint id of the blueprint to download
                         the resource from
    :param deployment_id: the deployment id of the deployment to download the
                          resource from
    :param tenant_name: tenant name
    :param resource_path: path to resource relative to blueprint folder
    :returns: resource content
    """
    tried_paths = []
    for path in _resource_paths(
            blueprint_id, deployment_id, tenant_name, resource_path):
        try:
            return get_resource_from_manager(path)
        except NonRecoverableError:
            tried_paths.append(path)
        except HttpException as e:
            if e.code != 404:
                raise
            tried_paths.append(path)

    raise HttpException(','.join(tried_paths), 404, 'Resource not found: {0}'
                        .format(resource_path))


def get_resource_directory_index(
        blueprint_id, deployment_id, tenant_name, resource_path):
    tried_paths = []
    resource_files = set()
    for path in _resource_paths(
            blueprint_id, deployment_id, tenant_name, resource_path,
            use_global=False):
        try:
            directory_index = get_resource_from_manager(path)
            directory_index_dict = json.loads(directory_index)
            for f in directory_index_dict.keys():
                resource_files.add(f)
        except (ValueError, KeyError, NonRecoverableError):
            tried_paths.append(path)
        except HttpException as e:
            if e.code != 404:
                raise

    if not resource_files:
        raise HttpException(','.join(tried_paths), 404,
                            'No valid resource directory listing at found: {0}'
                            .format(resource_path))
    return list(resource_files)


def get_node_instance(node_instance_id, evaluate_functions=False, client=None):
    """
    Read node instance data from the storage.

    :param node_instance_id: the node instance id
    :param evaluate_functions: Evaluate intrinsic functions
    :param client: a REST client to use
    :rtype: NodeInstance
    """
    if client is None:
        client = get_rest_client()
    instance = client.node_instances.get(
        node_instance_id,
        evaluate_functions=evaluate_functions
    )
    return NodeInstance(node_instance_id,
                        instance.node_id,
                        runtime_properties=instance.runtime_properties,
                        state=instance.state,
                        version=instance.version,
                        host_id=instance.host_id,
                        relationships=instance.relationships,
                        index=instance.index,
                        scaling_groups=instance.scaling_groups,
                        system_properties=instance.system_properties)


def update_node_instance(node_instance, client=None):
    """
    Update node instance data changes in the storage.

    :param node_instance: the node instance with the updated data
    :param client: a REST client to use
    """
    if client is None:
        client = get_rest_client()
    client.node_instances.update(
        node_instance.id,
        state=node_instance.state,
        runtime_properties=node_instance.runtime_properties,
        version=node_instance.version)


def get_node_instance_ip(node_instance_id, client=None):
    """
    Get the IP address of the host the node instance denoted by
    ``node_instance_id`` is contained in.
    """
    if client is None:
        client = get_rest_client()
    instance = client.node_instances.get(node_instance_id)
    if instance.host_id is None:
        raise NonRecoverableError('node instance: {0} is missing host_id'
                                  'property'.format(instance.id))
    if node_instance_id != instance.host_id:
        instance = client.node_instances.get(instance.host_id)
    if instance.runtime_properties.get('ip'):
        return instance.runtime_properties['ip']
    node = client.nodes.get(instance.deployment_id, instance.node_id)
    if node.properties.get('ip'):
        return node.properties['ip']
    raise NonRecoverableError('could not find ip for node instance: {0} with '
                              'host id: {1}'.format(node_instance_id,
                                                    instance.id))


def update_execution_status(execution_id, status, error=None, client=None):
    """
    Update the execution status of the execution denoted by ``execution_id``.

    :returns: The updated status
    """
    if client is None:
        client = get_rest_client()
    return client.executions.update(execution_id, status, error)


def get_bootstrap_context(client=None):
    """Read the manager bootstrap context."""
    if client is None:
        client = get_rest_client()
    context = client.manager.get_context()['context']
    context = context.get('cloudify', {})
    context.setdefault('workflows', {}).update(
        (c.name, c.value) for c in client.manager.get_config(scope='workflow')
    )
    return context


def get_provider_context(client=None):
    """Read the manager provider context."""
    if client is None:
        client = get_rest_client()
    context = client.manager.get_context()
    return context['context']


class DirtyTrackingDict(dict):

    def __init__(self, *args, **kwargs):
        super(DirtyTrackingDict, self).__init__(*args, **kwargs)
        self.modifiable = True
        self.dirty = False

    def __setitem__(self, key, value):
        r = super(DirtyTrackingDict, self).__setitem__(key, value)
        self._set_changed()
        return r

    def __delitem__(self, key):
        r = super(DirtyTrackingDict, self).__delitem__(key)
        self._set_changed()
        return r

    def update(self, E=None, **F):
        r = super(DirtyTrackingDict, self).update(E, **F)
        self._set_changed()
        return r

    def clear(self):
        r = super(DirtyTrackingDict, self).clear()
        self._set_changed()
        return r

    def pop(self, k, d=None):
        r = super(DirtyTrackingDict, self).pop(k, d)
        self._set_changed()
        return r

    def popitem(self):
        r = super(DirtyTrackingDict, self).popitem()
        self._set_changed()
        return r

    def _set_changed(self):
        # python 2.6 doesn't have modifiable during copy.deepcopy
        if hasattr(self, 'modifiable') and not self.modifiable:
            raise NonRecoverableError('Cannot modify runtime properties of'
                                      ' relationship node instances')
        self.dirty = True


def _get_workdir_path(deployment_id, tenant):
    return os.path.join('/opt', 'manager', 'resources', 'deployments',
                        tenant, deployment_id)
