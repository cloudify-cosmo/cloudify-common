########
# Copyright (c) 2014 GigaSpaces Technologies Ltd. All rights reserved
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
#    * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    * See the License for the specific language governing permissions and
#    * limitations under the License.

import errno
import os
import warnings
from contextlib import contextmanager

from cloudify_rest_client.exceptions import CloudifyClientError
from cloudify.endpoint import ManagerEndpoint, LocalEndpoint
from cloudify.logs import init_cloudify_logger
from cloudify import constants
from cloudify import exceptions
from cloudify import utils
from cloudify.constants import DEPLOYMENT, NODE_INSTANCE, RELATIONSHIP_INSTANCE


class ContextCapabilities(object):
    """Maps from instance relationship target ids to their respective
    runtime properties
    """
    def __init__(self, endpoint, instance):
        self._endpoint = endpoint
        self.instance = instance
        self._relationship_runtimes = None

    def _find_item(self, key):
        """
        Returns the capability for the provided key by iterating through all
        dependency nodes available capabilities.
        """
        ls = [caps for caps in self._capabilities.values() if key in caps]
        if len(ls) == 0:
            return False, None
        if len(ls) > 1:
            raise exceptions.NonRecoverableError(
                "'{0}' capability ambiguity [capabilities={1}]".format(
                    key, self._capabilities))
        return True, ls[0][key]

    def __getitem__(self, key):
        found, value = self._find_item(key)
        if not found:
            raise exceptions.NonRecoverableError(
                "capability '{0}' not found [capabilities={1}]".format(
                    key, self._capabilities))
        return value

    def __contains__(self, key):
        found, _ = self._find_item(key)
        return found

    def get_all(self):
        """Returns all capabilities as dict."""
        return self._capabilities

    def __str__(self):
        return ('<' + self.__class__.__name__ + ' ' +
                str(self._capabilities) + '>')

    @property
    def _capabilities(self):
        if self._relationship_runtimes is None:
            self._relationship_runtimes = {}
            for relationship in self.instance.relationships:
                self._relationship_runtimes.update({
                    relationship.target.instance.id:
                    relationship.target.instance.runtime_properties
                })
        return self._relationship_runtimes


class DeploymentWorkdirMixin:
    def download_deployment_workdir(self):
        """Download a copy of deployment-in-context's working directory from
        the manager, but only if these files are not available locally.
        """
        if not deployment_workdirs_sync_required():
            return

        local_dir = self.local_deployment_workdir()
        if not local_dir:
            return

        return self._endpoint.download_deployment_workdir(self.deployment.id,
                                                          local_dir)

    def upload_deployment_workdir(self):
        """Upload a local copy of deployment-in-context's working directory to
        the manager, but only if these files are not available locally.
        """
        if not deployment_workdirs_sync_required():
            return

        local_dir = self.local_deployment_workdir()
        if not local_dir:
            return

        return self._endpoint.upload_deployment_workdir(self.deployment.id,
                                                        local_dir)

    def sync_deployment_workdir(self):
        """Sync a local copy of deployment-in-context's working directory to
        the manager, but only if these files are not available locally.  The
        method returns a contextmanager, so it should be used like:

        ```
        with ctx.sync_deployment_workdir():
            do_something()
        ```
        """
        if not deployment_workdirs_sync_required():
            return utils.nullcontext()

        local_dir = self.local_deployment_workdir()
        if not local_dir:
            return utils.nullcontext()

        return self._endpoint.sync_deployment_workdir(self.deployment.id,
                                                      local_dir)

    def local_deployment_workdir(self):
        """Generate absolute path to deployment's local working directory.
        The directory is a local copy of relevant directory on the manager.
        """
        if not self.deployment.id or not self.deployment.tenant_name:
            return

        local_resources_root = utils.get_local_resources_root()
        if local_resources_root:
            return os.path.join(
                local_resources_root,
                'deployments',
                self.deployment.tenant_name,
                self.deployment.id,
            )

        raise exceptions.NonRecoverableError(
            'Local resources root directory not defined, is '
            f'{constants.LOCAL_RESOURCES_ROOT_ENV_KEY} environment variable '
            'set?')

    def upload_deployment_file(
        self,
        target_file_path,
        src_file,
        src_file_mtime=None,
    ):
        return self._endpoint.upload_deployment_file(
            deployment_id=self.deployment.id,
            target_file_path=target_file_path,
            src_file=src_file,
            src_file_mtime=src_file_mtime,
        )

    def delete_deployment_file(self, file_path):
        return self._endpoint.delete_deployment_file(
            deployment_id=self.deployment.id,
            file_path=file_path,
        )


class CommonContext(DeploymentWorkdirMixin):

    def __init__(self, ctx=None):
        self._context = ctx or {}
        self._local = ctx.get('local', False)
        if self._local:
            # there are times when this instance is instantiated merely for
            # accessing the attributes so we can tolerate no storage (such is
            # the case in logging)
            self._endpoint = LocalEndpoint(self, ctx.get('storage'))
        else:
            self._endpoint = ManagerEndpoint(self)

        self.blueprint = BlueprintContext(self._context)
        self.deployment = DeploymentContext(self._context)


class BootstrapContext(object):
    """
    Holds bootstrap context that was posted to the rest service. (usually
    during the bootstrap process).
    """

    class PolicyEngine(object):
        """Cloudify policy engine related configuration"""

        def __init__(self, policy_engine):
            self._policy_engine = policy_engine

        @property
        def start_timeout(self):
            """
            Returns the number of seconds to wait for the policy engine
            to start
            """
            return self._policy_engine.get('start_timeout')

    class CloudifyAgent(object):
        """Cloudify agent related bootstrap context properties."""

        def __init__(self, cloudify_agent):
            self._cloudify_agent = cloudify_agent

        @property
        def min_workers(self):
            """Returns the minimum number of workers for agent hosts."""
            return self._cloudify_agent.get('min_workers')

        @property
        def max_workers(self):
            """Returns the maximum number of workers for agent hosts."""
            return self._cloudify_agent.get('max_workers')

        @property
        def user(self):
            """
            Returns the username used when SSH-ing during agent
            installation.
            """
            return self._cloudify_agent.get('user')

        @property
        def remote_execution_port(self):
            """
            Returns the port used when SSH-ing during agent
            installation.
            """
            return self._cloudify_agent.get('remote_execution_port')

        @property
        def agent_key_path(self):
            """
            Returns the path to the key file on the management machine
            used when SSH-ing during agent installation.
            """
            return self._cloudify_agent.get('agent_key_path')

        @property
        def broker_ip(self):
            """
            Returns the host name or IP of the rabbit server.
            An empty string should result in clients using the manager IP.
            """
            return self._cloudify_agent.get('broker_ip')

        @property
        def broker_user(self):
            """
            Returns the username for connecting to rabbit.
            """
            return self._cloudify_agent.get('broker_user')

        @property
        def broker_pass(self):
            """
            Returns the password for connecting to rabbit.
            """
            return self._cloudify_agent.get('broker_pass')

        @property
        def broker_vhost(self):
            """
            Returns the vhost for connecting to rabbit.
            """
            return self._cloudify_agent.get('broker_vhost')

        @property
        def broker_ssl_enabled(self):
            """
            Returns whether SSL is enabled for connecting to rabbit.
            """
            return self._cloudify_agent.get('broker_ssl_enabled')

        @property
        def cluster(self):
            """
            Returns the cluster configuration.
            """
            return self._cloudify_agent.get('cluster')

        @property
        def networks(self):
            return self._cloudify_agent.get('networks')

    def __init__(self, bootstrap_context):
        self._bootstrap_context = bootstrap_context

        cloudify_agent = bootstrap_context.get('cloudify_agent', {})
        policy_engine = bootstrap_context.get('policy_engine', {})
        self._cloudify_agent = self.CloudifyAgent(cloudify_agent)
        self._policy_engine = self.PolicyEngine(policy_engine)

    @property
    def cloudify_agent(self):
        """
        Returns Cloudify agent related bootstrap context data

        :rtype: CloudifyAgent
        """
        return self._cloudify_agent

    @property
    def policy_engine(self):
        """
        Returns Cloudify policy engine related bootstrap context data

        :rtype: PolicyEngine
        """
        return self._policy_engine

    @property
    def resources_prefix(self):
        """
        Returns the resources prefix that was configured during bootstrap.
        An empty string is returned if the resources prefix was not configured.
        """
        return self._bootstrap_context.get('resources_prefix', '')

    def broker_config(self, *args, **kwargs):
        """
        Returns dictionary containing broker configuration.
        """
        attributes = {}
        bootstrap_agent = self.cloudify_agent
        broker_user, broker_pass, broker_vhost = \
            utils.internal.get_broker_credentials(bootstrap_agent)

        attributes['broker_ip'] = bootstrap_agent.broker_ip
        attributes['broker_ssl_enabled'] = bootstrap_agent.broker_ssl_enabled
        attributes['broker_user'] = broker_user
        attributes['broker_pass'] = broker_pass
        attributes['broker_vhost'] = broker_vhost
        attributes['cluster'] = bootstrap_agent.cluster
        return attributes


class EntityContext(object):

    def __init__(self, context, **_):
        self._context = context


class BlueprintContext(EntityContext):

    @property
    def id(self):
        """The blueprint id the plugin invocation belongs to."""
        return self._context.get('blueprint_id')


class DeploymentContext(EntityContext):

    @property
    def id(self):
        """The deployment id the plugin invocation belongs to."""
        return self._context.get('deployment_id')

    @property
    def tenant_name(self):
        """The deployment tenant's name."""
        return self._context.get('tenant', {}).get('name')

    @property
    def runtime_only_evaluation(self):
        return self._context.get('runtime_only_evaluation')

    @property
    def display_name(self):
        """The deployment's display_name used e.g. in Cloudify UI."""
        return self._context.get('deployment_display_name')

    @property
    def creator(self):
        """The name of the user who owns the deployment."""
        return self._context.get('deployment_creator')

    @property
    def resource_tags(self):
        """Resource tags associated with this deployment."""
        return self._context.get('deployment_resource_tags')


class NodeContext(EntityContext):

    def __init__(self, *args, **kwargs):
        super(NodeContext, self).__init__(*args, **kwargs)
        self._endpoint = kwargs['endpoint']
        self._node = None

    def _get_node_if_needed(self):
        if self._node is None:
            self._node = self._endpoint.get_node(
                self.id,
                instance_context=self._context.get('node_id'),
            )
            props = self._node.get('properties', {})
            self._node['properties'] = ImmutableProperties(props)

    @property
    def id(self):
        """The node's id"""
        return self.name

    @property
    def name(self):
        """The node's name"""
        return self._context.get('node_name')

    @property
    def properties(self):
        """The node properties as dict (read-only).
        These properties are the properties specified in the blueprint.
        """
        self._get_node_if_needed()
        return self._node.properties

    @property
    def type(self):
        """The node's type"""
        self._get_node_if_needed()
        return self._node.type

    @property
    def type_hierarchy(self):
        """The node's type hierarchy"""
        self._get_node_if_needed()
        return self._node.type_hierarchy

    @property
    def number_of_instances(self):
        """The number of instances of that node."""
        self._get_node_if_needed()
        return self._node.number_of_instances


class NodeInstanceContext(EntityContext):
    def __init__(self, *args, **kwargs):
        super(NodeInstanceContext, self).__init__(*args, **kwargs)
        self._endpoint = kwargs['endpoint']
        self._node = kwargs['node']
        self._modifiable = kwargs['modifiable']
        self._node_instance = None
        self._host_ip = None
        self._relationships = None
        self._scaling_groups = []

    def _get_node_instance(self):
        self._node_instance = self._endpoint.get_node_instance(self.id)
        self._node_instance.runtime_properties.modifiable = \
            self._modifiable

    def _get_node_instance_if_needed(self):
        if self._node_instance is None:
            self._get_node_instance()

    @property
    def id(self):
        """The node instance id."""
        return self._context.get('node_id')

    @property
    def node(self):
        """The node that this instance was created from."""
        return NodeContext(self._node)

    @property
    def runtime_properties(self):
        """The node instance runtime properties as a dict (read-only).

        Runtime properties are properties set during the node instance's
        lifecycle.
        Retrieving runtime properties involves a call to Cloudify's storage.
        """
        self._get_node_instance_if_needed()
        return self._node_instance.runtime_properties

    @property
    def scaling_groups(self):
        """The list of scaling group instances this node instance belongs to"""
        self._get_node_instance_if_needed()
        return self._node_instance.scaling_groups

    @runtime_properties.setter
    def runtime_properties(self, new_properties):
        self._get_node_instance_if_needed()
        self._node_instance.runtime_properties = new_properties

    @utils.keep_trying_http(total_timeout_sec=None)
    def update(self, on_conflict=None):
        """
        Stores new/updated runtime properties for the node instance in context
        in Cloudify's storage.

        This method should be invoked only if its necessary to immediately
        update Cloudify's storage with changes. Otherwise, the method is
        automatically invoked as soon as the task execution is over.

        Updating the runtime properties might fail due to concurrent writes:
        use a handler function to merge properties, to retry quickly.

        :param on_conflict: Optional function returning the runtime properties
                        to store. It will be called with two arguments: locally
                        modified runtime properties, and runtime properties
                        refetched from storage. If the update raises a
                        version conflict error (due to concurrent writes),
                        the function will be called again, with the same
                        locally modified runtime properties, and the newest
                        runtime properties from storage.
        :type on_conflict: function(dict, dict) -> dict
        """
        if on_conflict is not None:
            # copy the locally modified runtime properties so that we can pass
            # the same "before" state to each on_conflict invocation
            props = self.runtime_properties.copy()

            # Don't refetch yet - assume that the freshest version in storage
            # is the same that we have (optimistic concurrency control)
            latest_props = props

            while True:
                self.runtime_properties = on_conflict(props, latest_props)

                try:
                    self._endpoint.update_node_instance(self._node_instance)
                except CloudifyClientError as e:
                    if e.status_code != 409:
                        raise
                    # storage has a newer version of the node instance:
                    # let's fetch it and update our copy
                    self.refresh(force=True)
                    latest_props = self.runtime_properties.copy()
                else:
                    break
        else:
            if self._node_instance is not None and self._node_instance.dirty:
                self._endpoint.update_node_instance(self._node_instance)
        self._node_instance = None

    def refresh(self, force=False):
        """Force fetching up-to-date instance data.

        Useful for scripts that must reliably work in parallel, with each
        updating runtime properties.

        :param force: Overwrite local changes
        """
        if not force and self._node_instance and self._node_instance.dirty:
            raise exceptions.NonRecoverableError(
                'runtime_properties are dirty: refreshing now would destroy '
                'local changes')
        self._get_node_instance()

    def _get_node_instance_ip_if_needed(self):
        self._get_node_instance_if_needed()
        if self._host_ip is None:
            if self.id == self._node_instance.host_id:
                self._host_ip = self._endpoint.get_host_node_instance_ip(
                    host_id=self.id,
                    properties=self._node.properties,
                    runtime_properties=self.runtime_properties)
            else:
                self._host_ip = self._endpoint.get_host_node_instance_ip(
                    host_id=self._node_instance.host_id)

    @property
    def host_ip(self):
        """
        Returns the node instance host ip address.

        This values is derived by reading the ``host_id`` from the relevant
        node instance and then reading its ``ip`` runtime property or its
        node_state ``ip`` property.
        """

        self._get_node_instance_ip_if_needed()
        return self._host_ip

    @property
    def relationships(self):
        """Returns a list of this instance relationships

        :return: list of RelationshipContext
        :rtype: list
        """
        self._get_node_instance_if_needed()
        if self._relationships is None:
            self._relationships = [
                RelationshipContext(relationship, self._endpoint, self._node)
                for relationship in self._node_instance.relationships]
        return self._relationships

    @property
    def index(self):
        self._get_node_instance_if_needed()
        return self._node_instance.index

    @property
    def drift(self):
        self._get_node_instance_if_needed()
        drift = self._node_instance.system_properties\
            .get('configuration_drift')
        return drift.get('result') if drift.get('ok') else None


class RelationshipContext(EntityContext):
    """Holds relationship instance data"""

    def __init__(self, relationship_context, endpoint, node):
        super(RelationshipContext, self).__init__(relationship_context)
        self._node = node
        target_context = {
            'node_name': relationship_context['target_name'],
            'node_id': relationship_context['target_id']
        }
        self._target = RelationshipSubjectContext(target_context, endpoint,
                                                  modifiable=False)
        self._type_hierarchy = None

    @property
    def target(self):
        """Returns a holder for target node and target instance

        :rtype: RelationshipSubjectContext
        """
        return self._target

    @property
    def type(self):
        """The relationship type"""
        return self._context.get('type')

    @property
    def type_hierarchy(self):
        """The relationship type hierarchy"""
        if self._type_hierarchy is None:
            self._node._get_node_if_needed()
            node_relationships = self._node._node.relationships
            self._type_hierarchy = [
                r for r in node_relationships if
                r['type'] == self.type][0]['type_hierarchy']
        return self._type_hierarchy


class RelationshipSubjectContext(object):
    """Holds reference to node and node instance.

    Obtained in relationship operations by `ctx.source` and `ctx.target`, and
    by iterating instance relationships and for each relationship, reading
    `relationship.target`
    """

    def __init__(self, context, endpoint, modifiable):
        self._context = context
        self.node = NodeContext(context,
                                endpoint=endpoint)
        self.instance = NodeInstanceContext(context,
                                            endpoint=endpoint,
                                            node=self.node,
                                            modifiable=modifiable)


class CloudifyContext(CommonContext):

    """
    A context object passed to plugins tasks invocations.
    The context object is used in plugins when interacting with
    the Cloudify environment::

        from cloudify import ctx

        @operation
        def my_start(**kwargs):
            # port is a property that was configured on the current instance's
            # node
            port = ctx.node.properties['port']
            start_server(port=port)

    """
    def __init__(self, ctx=None):
        super(CloudifyContext, self).__init__(ctx=ctx)

        self._logger = None
        self._provider_context = None
        self._bootstrap_context = None
        self._rest_host = self._context.get('rest_host')
        self._rest_ssl_cert = self._context.get('rest_ssl_cert')
        self._node = None
        self._instance = None
        self._source = None
        self._target = None
        self._operation = OperationContext(self._context.get('operation', {}))
        self._agent = CloudifyAgentContext(self)
        self._tenant = None

        capabilities_node_instance = None
        if 'related' in self._context:
            if self._context['related']['is_target']:
                source_context = self._context
                target_context = self._context['related']
            else:
                source_context = self._context['related']
                target_context = self._context
            self._source = RelationshipSubjectContext(source_context,
                                                      self._endpoint,
                                                      modifiable=True)
            self._target = RelationshipSubjectContext(target_context,
                                                      self._endpoint,
                                                      modifiable=True)
            if self._context['related']['is_target']:
                capabilities_node_instance = self._source.instance
            else:
                capabilities_node_instance = self._target.instance

        elif self._context.get('node_id'):
            self._node = NodeContext(self._context,
                                     endpoint=self._endpoint)
            self._instance = NodeInstanceContext(self._context,
                                                 endpoint=self._endpoint,
                                                 node=self._node,
                                                 modifiable=True)
            capabilities_node_instance = self._instance

        self._capabilities = ContextCapabilities(self._endpoint,
                                                 capabilities_node_instance)

        plugin = self._context.get('plugin', {})
        # Because we inherit from str, we can't really change the constructor
        # only augment it.
        self._plugin = PluginContext(plugin.get('name', ''))
        self._plugin._plugin_context = plugin
        self._plugin._deployment_id = self.deployment.id
        self._plugin._tenant_name = self.tenant_name
        self._plugin._endpoint = self._endpoint

    def _verify_in_node_context(self):
        if self.type != NODE_INSTANCE:
            raise exceptions.NonRecoverableError(
                'ctx.node/ctx.instance can only be used in a {0} context but '
                'used in a {1} context.'.format(NODE_INSTANCE, self.type))

    def _verify_in_relationship_context(self):
        if self.type != RELATIONSHIP_INSTANCE:
            raise exceptions.NonRecoverableError(
                'ctx.source/ctx.target can only be used in a {0} context but '
                'used in a {1} context.'.format(RELATIONSHIP_INSTANCE,
                                                self.type))

    def _verify_in_node_or_relationship_context(self):
        if self.type not in [NODE_INSTANCE, RELATIONSHIP_INSTANCE]:
            raise exceptions.NonRecoverableError(
                'capabilities can only be used in a {0}/{1} context but '
                'used in a {2} context.'.format(NODE_INSTANCE,
                                                RELATIONSHIP_INSTANCE,
                                                self.type))

    def get_managers(self, network='default'):
        """The managers, optionally for a non-default network."""
        return self._endpoint.get_managers(network)

    def get_brokers(self, network='default'):
        """The brokers, optionally for a non-default network."""
        return self._endpoint.get_brokers(network)

    @property
    def instance(self):
        """The node instance the operation is executed for.

        This property is only relevant for NODE_INSTANCE context operations.
        """
        self._verify_in_node_context()
        return self._instance

    @property
    def node(self):
        """The node the operation is executed for.

        This property is only relevant for NODE_INSTANCE context operations.
        """
        self._verify_in_node_context()
        return self._node

    @property
    def source(self):
        """Provides access to the relationship's operation source node and
        node instance.

        This property is only relevant for relationship operations.
        """
        self._verify_in_relationship_context()
        return self._source

    @property
    def target(self):
        """Provides access to the relationship's operation target node and
        node instance.

        This property is only relevant for relationship operations.
        """
        self._verify_in_relationship_context()
        return self._target

    @property
    def task_type(self):
        """The kind of task this context is for

        Possible values include: operation, workflow, or hook.
        """
        return self._context['type']

    @property
    def type(self):
        """The type of this context.

        Available values:

        - DEPLOYMENT
        - NODE_INSTANCE
        - RELATIONSHIP_INSTANCE
        """
        if self._source:
            return RELATIONSHIP_INSTANCE
        if self._instance:
            return NODE_INSTANCE
        return DEPLOYMENT

    @property
    def timeout(self):
        """After this many seconds, the operation process will be killed"""
        return self._context.get('timeout')

    @property
    def timeout_recoverable(self):
        """If set, the operation timeouting will raise a RecoverableError

        By default, if the operation is killed by a timeout,
        NonRecoverableError is raised instead.
        Recoverable allows for retries.
        """
        return self._context.get('timeout_recoverable', False)

    @property
    def execution_env(self):
        """Additional envvars for the operation subprocess"""
        return self._context.get('execution_env') or {}

    @property
    def execution_id(self):
        """
        The workflow execution id the plugin invocation was requested from.
        This is a unique value which identifies a specific workflow execution.
        """
        return self._context.get('execution_id')

    @property
    def execution_token(self):
        """The token of the current execution"""
        return self._context.get('execution_token')

    @property
    def workflow_id(self):
        """
        The workflow id the plugin invocation was requested from.
        For example:

         ``install``, ``uninstall`` etc...
        """
        return self._context.get('workflow_id')

    @property
    def rest_host(self):
        """REST host"""
        return self._rest_host

    @property
    def rest_port(self):
        """REST port"""
        return self._context.get('rest_port')

    @property
    def rest_token(self):
        """REST service token"""
        return self._context.get('rest_token')

    @property
    def rest_ssl_cert(self):
        """REST SSL Certificate Content"""
        return self._rest_ssl_cert

    @property
    def bypass_maintenance(self):
        """If true, all requests sent bypass maintenance mode."""
        return self._context.get('bypass_maintenance', False)

    @property
    def tenant_name(self):
        """Cloudify tenant name"""
        return self._context.get('tenant', {}).get('name')

    @property
    def tenant(self):
        """Full Cloudify tenant.

        This will go out to the REST API and fetch all the tenant details
        that the current user is allowed to obtain.
        """
        if self._tenant is None:
            self._tenant = self._context.get('tenant', {}).copy()
            self._tenant.update(utils.get_tenant())
        return self._tenant

    @property
    def task_id(self):
        """The plugin's task invocation unique id."""
        return self._context.get('task_id')

    @property
    def task_name(self):
        """The full task name of the invoked task."""
        return self._context.get('task_name')

    @property
    def task_target(self):
        """The task target (agent worker name)."""
        return self._context.get('task_target')

    @property
    def task_queue(self):
        """The task target (agent queue name)."""
        return self._context.get('task_queue')

    @property
    def plugin(self):
        """The plugin context."""
        return self._plugin

    @property
    def operation(self):
        """
        The current operation context.
        """
        return self._operation

    @property
    def agent(self):
        self._verify_in_node_context()
        if constants.COMPUTE_NODE_TYPE not in self.node.type_hierarchy:
            raise exceptions.NonRecoverableError(
                'ctx.agent can only be used with compute nodes but current '
                'node is of type: {0}'.format(self.node.type))
        return self._agent

    @property
    def capabilities(self):
        """Maps from instance relationship target ids to their respective
        runtime properties

        NOTE: This feature is deprecated, use 'instance.relationships' instead.
        """
        self._verify_in_node_or_relationship_context()
        warnings.warn('capabilities is deprecated, use instance.relationships'
                      'instead', DeprecationWarning)
        return self._capabilities

    @property
    def logger(self):
        """
        A Cloudify context aware logger.

        Use this logger in order to index logged messages in ElasticSearch
        using logstash.
        """
        if self._logger is None:
            self._logger = self._init_cloudify_logger()
        return self._logger

    @property
    def bootstrap_context(self):
        """
        System context provided during the bootstrap process

        :rtype: BootstrapContext
        """
        if self._bootstrap_context is None:
            context = self._endpoint.get_bootstrap_context()
            self._bootstrap_context = BootstrapContext(context)
        return self._bootstrap_context

    @property
    def resume(self):
        """This run of the operation is a resume of an interrupted run"""
        return self._context.get('resume', False)

    @property
    def execution_creator_username(self):
        return self._context.get('execution_creator_username')

    @resume.setter
    def resume(self, value):
        self._context['resume'] = value

    def get_config(self, name=None, scope=None):
        """Get a subset of the stored configuration"""
        return self._endpoint.get_config(name=name, scope=scope)

    def send_event(self, event):
        """
        Send an event to rabbitmq

        :param event: the event message
        """
        self._endpoint.send_plugin_event(message=event)

    @property
    def provider_context(self):
        """Gets provider context which contains provider specific metadata."""
        if self._provider_context is None:
            self._provider_context = self._endpoint.get_provider_context()
        return self._provider_context

    @property
    def workflow_parameters(self):
        """Get workflow parameters associated with this context."""
        return self._context.get('workflow_parameters', {})

    def get_operation(self):
        """Get the operation object for the currently-executed task."""
        return self._endpoint.get_operation(self.task_id)

    def get_execution(self, execution_id=None):
        """
        Ge the execution object for the current execution
        :param execution_id: The Id of the execution object
        :return: Instance of `Execution` object which holds all the needed info
        """
        if not execution_id:
            execution_id = self.execution_id
        return self._endpoint.get_execution(execution_id)

    @utils.keep_trying_http(total_timeout_sec=None)
    def update_operation(self, state):
        """Update current operation state.

        :param state: New operation state
        """
        self._endpoint.update_operation(self.task_id, state)

    def get_resource(self, resource_path):
        """
        Retrieves a resource bundled with the blueprint as a string.

        :param resource_path: the path to the resource. Note that this path is
                              relative to the blueprint file which was
                              uploaded.
        """

        return self._endpoint.get_resource(
            blueprint_id=self.blueprint.id,
            deployment_id=self.deployment.id,
            resource_path=resource_path)

    def get_resource_and_render(self,
                                resource_path,
                                template_variables=None):
        """
        Like get_resource, but also renders the resource according
        to template_variables.
        This context is added to template_variables.

        :param template_variables: according to this dict the
                                   resource will be rendered.
        """

        template_variables = self._add_context_to_template_variables(
            template_variables)
        return self._endpoint.get_resource(
            blueprint_id=self.blueprint.id,
            deployment_id=self.deployment.id,
            resource_path=resource_path,
            template_variables=template_variables)

    def download_directory(self,
                           directory_path,
                           target_path=None):
        return self._endpoint.download_directory(
            blueprint_id=self.blueprint.id,
            deployment_id=self.deployment.id,
            resource_path=directory_path,
            logger=self.logger,
            target_path=target_path)

    def download_resource(self,
                          resource_path,
                          target_path=None):
        """
        Retrieves a resource bundled with the blueprint and saves it under a
        local file.

        :param resource_path: the path to the resource. Note that this path is
                              relative to the blueprint file which was
                              uploaded.

        :param target_path: optional local path (including filename) to store
                            the resource at on the local file system.
                            If missing, the location will be a tempfile with a
                            generated name.

        :returns: The path to the resource on the local file system (identical
                  to target_path parameter if used).

                  raises an ``cloudify.exceptions.HttpException``

        :raises: ``cloudify.exceptions.HttpException`` on any kind
                 of HTTP Error.

        :raises: ``IOError`` if the resource
                 failed to be written to the local file system.

        """

        return self._endpoint.download_resource(
            blueprint_id=self.blueprint.id,
            deployment_id=self.deployment.id,
            resource_path=resource_path,
            logger=self.logger,
            target_path=target_path)

    def download_resource_and_render(self,
                                     resource_path,
                                     target_path=None,
                                     template_variables=None):
        """
        Like download_resource, but also renders the resource according
        to template_variables.
        This context is added to template_variables.

        :param template_variables: according to this dict the resource
                                   will be rendered.

        """

        template_variables = self._add_context_to_template_variables(
            template_variables)
        return self._endpoint.download_resource(
            blueprint_id=self.blueprint.id,
            deployment_id=self.deployment.id,
            resource_path=resource_path,
            logger=self.logger,
            target_path=target_path,
            template_variables=template_variables)

    def _init_cloudify_logger(self):
        logger_name = self.task_id if self.task_id is not None \
            else 'cloudify_plugin'
        handler = self._endpoint.get_logging_handler()
        return init_cloudify_logger(handler, logger_name)

    def _add_context_to_template_variables(self, template_variables):

        if template_variables:
            if 'ctx' in template_variables:
                raise exceptions.NonRecoverableError(
                    'Key not allowed - a key named '
                    'ctx is in template_variables')
        else:
            template_variables = {}

        template_variables['ctx'] = self
        return template_variables


class OperationContext(object):

    def __init__(self, operation_context):
        self._operation_context = operation_context or {}
        if not isinstance(self._operation_context, dict):
            raise exceptions.NonRecoverableError(
                'operation_context is expected to be a dict but is:'
                '{0}'.format(self._operation_context))
        self._operation_retry = None

    @property
    def name(self):
        """The name of the operation."""
        return self._operation_context.get('name')

    @property
    def retry_number(self):
        """The retry number (relevant for retries and recoverable errors)."""
        return self._operation_context.get('retry_number')

    @property
    def max_retries(self):
        """The maximum number of retries the operation can have."""
        return self._operation_context.get('max_retries')

    @property
    def relationship(self):
        return self._operation_context.get('relationship')

    def retry(self, message=None, retry_after=None):
        """Specifies that this operation should be retried.

        Usage:
          return ctx.operation.retry(message='...', retry_after=1000)

        :param message A text message containing information about the reason
                       for retrying the operation.
        :param retry_after How many seconds should the workflow engine wait
                           before re-executing the operation.
        """
        self._operation_retry = exceptions.OperationRetry(
            message=message,
            retry_after=retry_after)


class CloudifyAgentContext(object):

    def __init__(self, context):
        self.context = context
        self._script_path = None

    @property
    def script_path(self):
        return self._script_path

    @script_path.setter
    def script_path(self, value):
        self._script_path = value

    def clean_script(self):
        if self.script_path and os.path.exists(self.script_path):
            os.remove(self.script_path)
        self.script_path = None

    @staticmethod
    def _validate_agent_env():
        try:
            from cloudify_agent.installer import script  # NOQA
        except ImportError as e:
            raise exceptions.NonRecoverableError(
                'init_script/install_script cannot be used '
                'outside of an agent environment: ImportError: {0}'.format(e))

    def init_script(self, agent_config=None):
        if (utils.internal.get_install_method(
                self.context.node.properties) not in
                constants.AGENT_INSTALL_METHODS_SCRIPTS):
            return None
        self._validate_agent_env()
        from cloudify_agent.installer import script
        return script.init_script(cloudify_agent=agent_config)

    @contextmanager
    def install_script_download_link(self, agent_config=None, clean=True):
        self._validate_agent_env()
        from cloudify_agent.installer import script
        try:
            script_path, script_url = script.install_script_download_link(
                cloudify_agent=agent_config
            )
            self.script_path = script_path
            yield script_url
        finally:
            if clean:
                self.clean_script()


# inherits from `str` to maintain backwards compatibility
# with plugins that assume ctx.plugin will return the plugin name
# ctx.plugin == name should be deprecated and this workaround should
# be removed at some point
class PluginContext(str):

    def __init__(self, other=''):
        # These are set explicitly after PluginContext is instantiated
        self._plugin_context = {}
        self._deployment_id = None
        self._tenant_name = None
        self._endpoint = None

    @property
    def name(self):
        """The plugin name."""
        return self._plugin_context.get('name', '')

    @property
    def package_name(self):
        """The plugin package name."""
        return self._plugin_context.get('package_name')

    @property
    def package_version(self):
        """The plugin package version."""
        return self._plugin_context.get('package_version')

    @property
    def source(self):
        """The source package of the plugin."""
        return self._plugin_context.get('source')

    @property
    def properties(self):
        """The properties of the plugin."""
        return self._plugin_context.get('properties')

    @property
    def prefix(self):
        """The plugin prefix."""
        return utils.plugin_prefix(
            name=self.package_name or self.name,
            version=self.package_version,
            deployment_id=self._deployment_id,
            tenant_name=self._tenant_name)

    @property
    def workdir(self):
        """The plugin workdir.

        This directory is unique for each (deployment, plugin) combination.

        Note: if this operation is executed not as part of a deployment or
        a plugin, None is returned.
        """
        if not self.name:
            return None
        workdir = self._endpoint.get_workdir()
        plugin_workdir = os.path.join(workdir, 'plugins', self.name)
        if not os.path.exists(plugin_workdir):
            try:
                os.makedirs(plugin_workdir)
            except IOError as e:
                if e.errno == errno.EEXIST:
                    pass
                else:
                    raise
        return plugin_workdir


class ImmutableProperties(dict):
    """
    Of course this is not actually immutable, but it is good enough to provide
    an API that will tell you you're doing something wrong if you try updating
    the static node properties in the normal way.
    """

    @staticmethod
    def _raise():
        raise exceptions.NonRecoverableError(
            'Cannot override read only properties')

    def __setitem__(self, key, value):
        self._raise()

    def __delitem__(self, key):
        self._raise()

    def update(self, E=None, **F):
        self._raise()

    def clear(self):
        self._raise()

    def pop(self, k, d=None):
        self._raise()

    def popitem(self):
        self._raise()


def deployment_workdirs_sync_required():
    """Returns `True` if mgmtworker and manager are running on different
    hosts.  The test is just a comparison of environment variables:
    `CFY_RESOURCES_ROOT` and `MANAGER_FILE_SERVER_ROOT`.
    """
    local_resources_root =\
        os.environ.get(constants.LOCAL_RESOURCES_ROOT_ENV_KEY)
    file_manager_root =\
        os.environ.get(constants.MANAGER_FILE_SERVER_ROOT_KEY)
    return (local_resources_root
            and (local_resources_root != file_manager_root))
