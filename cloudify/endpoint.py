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

import os

from cloudify import constants
from cloudify import manager
from cloudify import logs
from cloudify import utils
from cloudify.logs import CloudifyPluginLoggingHandler
from cloudify.exceptions import NonRecoverableError

from cloudify_rest_client.manager import RabbitMQBrokerItem, ManagerItem
from cloudify_rest_client.executions import Execution


class Endpoint(object):

    def __init__(self, ctx):
        self.ctx = ctx

    def get_node(self, node_id, instance_context=None):
        raise NotImplementedError('Implemented by subclasses')

    def get_node_instance(self, node_instance_id):
        raise NotImplementedError('Implemented by subclasses')

    def update_node_instance(self, node_instance):
        raise NotImplementedError('Implemented by subclasses')

    def get_resource(self,
                     blueprint_id,
                     deployment_id,
                     resource_path,
                     template_variables=None):
        raise NotImplementedError('Implemented by subclasses')

    def get_config(self, name=None, scope=None):
        raise NotImplementedError('Implemented by subclasses')

    def download_resource(self,
                          blueprint_id,
                          deployment_id,
                          resource_path,
                          logger,
                          target_path=None,
                          template_variables=None):
        raise NotImplementedError('Implemented by subclasses')

    def download_directory(self,
                           blueprint_id,
                           deployment_id,
                           resource_path,
                           logger,
                           target_path=None,
                           preview_only=False):
        raise NotImplementedError('Implemented by subclasses')

    def _render_resource_if_needed(self,
                                   resource,
                                   template_variables,
                                   download=False):
        if not template_variables:
            return resource

        resource_path = resource
        if download:
            with open(resource_path, 'rb') as f:
                resource = f.read()
        if (
            b'{{' in resource
            or b'{#' in resource
            or b'{%' in resource
        ):
            # resource contains jinja2 template control strings - we need
            # to render it
            # only import jinja2 if necessary - it's a very big module
            import jinja2
            template = jinja2.Template(resource.decode('utf-8'))
            resource = template.render(template_variables).encode('utf-8')

        if download:
            with open(resource_path, 'wb') as f:
                f.write(resource)
            return resource_path
        else:
            return resource

    def get_provider_context(self):
        raise NotImplementedError('Implemented by subclasses')

    def get_bootstrap_context(self):
        raise NotImplementedError('Implemented by subclasses')

    def get_logging_handler(self):
        raise NotImplementedError('Implemented by subclasses')

    def send_plugin_event(self,
                          message=None,
                          args=None,
                          additional_context=None):
        raise NotImplementedError('Implemented by subclasses')

    def get_host_node_instance_ip(self,
                                  host_id,
                                  properties=None,
                                  runtime_properties=None):
        """
        See ``manager.get_node_instance_ip``
        (this method duplicates its logic for the
        sake of some minor optimizations and so that it can be used in local
        context).
        """
        # properties and runtime_properties are either both None or
        # both not None
        if not host_id:
            raise NonRecoverableError('host_id missing')
        if runtime_properties is None:
            instance = self.get_node_instance(host_id)
            runtime_properties = instance.runtime_properties
        if runtime_properties.get('ip'):
            return runtime_properties['ip']
        if properties is None:
            # instance is not None (see comment above)
            node = self.get_node(instance.node_id)
            properties = node.properties
        if properties.get('ip'):
            return properties['ip']
        raise NonRecoverableError('could not find ip for host node instance: '
                                  '{0}'.format(host_id))

    def evaluate_functions(self, payload):
        raise NotImplementedError('Implemented by subclasses')

    def _evaluate_functions_impl(self,
                                 payload,
                                 evaluate_functions_method):
        evaluation_context = {}
        if self.ctx.type == constants.NODE_INSTANCE:
            evaluation_context['self'] = self.ctx.instance.id
        elif self.ctx.type == constants.RELATIONSHIP_INSTANCE:
            evaluation_context.update({
                'source': self.ctx.source.instance.id,
                'target': self.ctx.target.instance.id,
                'source_node': self.ctx.source.node.id,
                'target_node': self.ctx.target.node.id
            })
        return evaluate_functions_method(deployment_id=self.ctx.deployment.id,
                                         context=evaluation_context,
                                         payload=payload)

    def get_workdir(self):
        raise NotImplementedError('Implemented by subclasses')

    def get_execution(self, execution_id):
        raise NotImplementedError('Implemented by subclasses')

    def download_deployment_workdir(self, deployment_id, local_dir):
        raise NotImplementedError('Implemented by subclasses')

    def upload_deployment_workdir(self, deployment_id, local_dir):
        raise NotImplementedError('Implemented by subclasses')

    def sync_deployment_workdir(self, deployment_id, local_dir):
        raise NotImplementedError('Implemented by subclasses')

    def upload_deployment_file(
        self, target_file_path, src_file, src_file_mtime=None
    ):
        raise NotImplementedError('Implemented by subclasses')

    def delete_deployment_file(self, file_path):
        raise NotImplementedError('Implemented by subclasses')


class ManagerEndpoint(Endpoint):
    def __init__(self, *args, **kwargs):
        super(ManagerEndpoint, self).__init__(*args, **kwargs)
        self._rest_client = None

    @property
    def rest_client(self):
        if self._rest_client is None:
            self._rest_client = manager.get_rest_client(self.ctx.tenant_name)
        return self._rest_client

    def get_node(self, node_id, instance_context=None):
        return self.rest_client.nodes.get(
            self.ctx.deployment.id,
            node_id,
            evaluate_functions=True,
            instance_context=instance_context,
        )

    def get_node_instance(self, node_instance_id):
        return manager.get_node_instance(node_instance_id,
                                         evaluate_functions=True,
                                         client=self.rest_client)

    def get_managers(self, network='default'):
        return [m for m in self.rest_client.manager.get_managers()
                if network in m.networks]

    def get_brokers(self, network='default'):
        return [broker for broker in self.rest_client.manager.get_brokers()
                if network in broker.networks]

    def update_node_instance(self, node_instance):
        return manager.update_node_instance(
            node_instance, client=self.rest_client)

    def get_resource(self,
                     blueprint_id,
                     deployment_id,
                     resource_path,
                     template_variables=None):
        resource = manager.get_resource(blueprint_id=blueprint_id,
                                        deployment_id=deployment_id,
                                        tenant_name=self.ctx.tenant_name,
                                        resource_path=resource_path)
        return self._render_resource_if_needed(
            resource=resource,
            template_variables=template_variables)

    def download_directory(self,
                           blueprint_id,
                           deployment_id,
                           resource_path,
                           logger,
                           target_path=None,
                           preview_only=False):
        resource_files = manager.get_resource_directory_index(
            blueprint_id, deployment_id, self.ctx.tenant_name, resource_path)
        resource_files = [os.path.join(resource_path, fp)
                          for fp in resource_files]

        logger.debug(">> Collected %s", resource_files)
        if preview_only:
            return resource_files

        top_dir = None
        target_dir = utils.create_temp_folder()
        for file_path in resource_files:
            top_dir = top_dir or file_path.split(os.sep)[0]
            target_path = os.path.join(target_dir,
                                       os.path.relpath(file_path, top_dir))
            os.makedirs(os.path.dirname(target_path), exist_ok=True)
            self.download_resource(
                blueprint_id,
                deployment_id,
                file_path,
                logger,
                target_path=target_path)
        return target_dir

    def download_resource(self,
                          blueprint_id,
                          deployment_id,
                          resource_path,
                          logger,
                          target_path=None,
                          template_variables=None):
        resource = manager.download_resource(
            blueprint_id=blueprint_id,
            deployment_id=deployment_id,
            tenant_name=self.ctx.tenant_name,
            resource_path=resource_path,
            logger=logger,
            target_path=target_path)
        return self._render_resource_if_needed(
            resource=resource,
            template_variables=template_variables,
            download=True)

    def get_provider_context(self):
        return manager.get_provider_context(client=self.rest_client)

    def get_bootstrap_context(self):
        return manager.get_bootstrap_context(client=self.rest_client)

    def get_logging_handler(self):
        return CloudifyPluginLoggingHandler(self.ctx,
                                            out_func=logs.manager_log_out)

    def send_plugin_event(self,
                          message=None,
                          args=None,
                          additional_context=None):
        logs.send_plugin_event(self.ctx,
                               message,
                               args,
                               additional_context,
                               out_func=logs.manager_event_out)

    def evaluate_functions(self, payload):
        def evaluate_functions_method(deployment_id, context, payload):
            return self.rest_client.evaluate.functions(
                deployment_id, context, payload)['payload']
        return self._evaluate_functions_impl(payload,
                                             evaluate_functions_method)

    def get_workdir(self):
        if not self.ctx.deployment.id:
            raise NonRecoverableError(
                'get_workdir is only implemented for operations that are '
                'invoked as part of a deployment.')
        base_workdir = os.environ['AGENT_WORK_DIR']
        deployments_workdir = os.path.join(
            base_workdir, 'deployments', self.ctx.tenant_name)
        # Exists on management worker, doesn't exist on host agents
        if os.path.exists(deployments_workdir):
            return os.path.join(deployments_workdir, self.ctx.deployment.id)
        else:
            return base_workdir

    def get_config(self, name=None, scope=None):
        return self.rest_client.manager.get_config(name=name, scope=scope)

    def get_operation(self, operation_id):
        return self.rest_client.operations.get(operation_id)

    def get_execution(self, execution_id):
        return self.rest_client.executions.get(execution_id)

    def update_operation(self, operation_id, state):
        self.rest_client.operations.update(
            operation_id,
            state=state,
            agent_name=utils.get_daemon_name(),
            manager_name=utils.get_manager_name(),
        )

    def download_deployment_workdir(self, deployment_id, local_dir):
        return self.rest_client.resources.download_deployment_workdir(
            deployment_id, local_dir)

    def upload_deployment_workdir(self, deployment_id, local_dir):
        return self.rest_client.resources.upload_deployment_workdir(
            deployment_id, local_dir)

    def sync_deployment_workdir(self, deployment_id, local_dir):
        return self.rest_client.resources.sync_deployment_workdir(
            deployment_id, local_dir)

    def upload_deployment_file(
        self, deployment_id, target_file_path, src_file, src_file_mtime=None,
    ):
        return self.rest_client.resources.upload_deployment_file(
            deployment_id=deployment_id,
            target_file_path=target_file_path,
            src_file=src_file,
            src_file_mtime=src_file_mtime,
        )

    def delete_deployment_file(self, deployment_id, file_path):
        return self.rest_client.resources.delete_deployment_file(
            deployment_id=deployment_id,
            file_path=file_path,
        )


class LocalEndpoint(Endpoint):

    def __init__(self, ctx, storage):
        super(LocalEndpoint, self).__init__(ctx)
        self.storage = storage

    def get_node(self, node_id, instance_context=None):
        return self.storage.get_node(node_id)

    def get_node_instance(self, node_instance_id):
        instance = self.storage.get_node_instance(node_instance_id)
        return manager.NodeInstance(
            node_instance_id,
            instance.node_id,
            runtime_properties=instance.runtime_properties,
            state=instance.state,
            version=instance.version,
            host_id=instance.host_id,
            relationships=instance.relationships)

    def update_node_instance(self, node_instance):
        return self.storage.update_node_instance(
            node_instance.id,
            runtime_properties=node_instance.runtime_properties,
            state=None,
            version=node_instance.version)

    def get_brokers(self, network='default'):
        # This is only used because some tests use a local context despite
        # current agents making absolutely no sense in a local context
        return [
            RabbitMQBrokerItem({
                'host': '127.0.0.1',
                'networks': {
                    'default': '127.0.0.1'
                },
                'ca_cert_content': '',
                'management_host': '127.0.0.1',
            })
        ]

    def get_managers(self, network='default'):
        # same issue as get_brokers
        return [
            ManagerItem({
                'private_ip': '127.0.0.1',
                'networks': {
                    'default': '127.0.0.1'
                },
            })
        ]

    def get_resource(self,
                     blueprint_id,
                     deployment_id,
                     resource_path,
                     template_variables=None):
        resource = self.storage.get_resource(resource_path)
        return self._render_resource_if_needed(
            resource=resource,
            template_variables=template_variables)

    def download_resource(self,
                          blueprint_id,
                          deployment_id,
                          resource_path,
                          logger,
                          target_path=None,
                          template_variables=None):
        resource = self.storage.download_resource(resource_path=resource_path,
                                                  target_path=target_path)
        return self._render_resource_if_needed(
            resource=resource,
            template_variables=template_variables,
            download=True)

    def get_provider_context(self):
        return self.storage.get_provider_context()

    def get_bootstrap_context(self):
        return self.get_provider_context().get('cloudify', {})

    def get_logging_handler(self):
        return CloudifyPluginLoggingHandler(self.ctx,
                                            out_func=logs.stdout_log_out)

    def send_plugin_event(self,
                          message=None,
                          args=None,
                          additional_context=None):
        logs.send_plugin_event(self.ctx,
                               message,
                               args,
                               additional_context,
                               out_func=logs.stdout_event_out)

    def evaluate_functions(self, payload):
        def evaluate_functions_method(deployment_id, context, payload):
            return self.storage.env.evaluate_functions(payload=payload,
                                                       context=context)
        return self._evaluate_functions_impl(
            payload, evaluate_functions_method)

    def get_workdir(self):
        return self.storage.get_workdir()

    def get_config(self, name=None, scope=None):
        # TODO implement stored config for cfy local
        return []

    def update_operation(self, operation_id, state):
        # operation storage is not supported for local
        return None

    def get_operation(self, operation_id):
        return None

    def get_execution(self, execution_id):
        # same issue as get_brokers
        return Execution({'id': execution_id, 'status': 'started'})

    def download_deployment_workdir(self, deployment_id, local_dir):
        pass

    def upload_deployment_workdir(self, deployment_id, local_dir):
        pass

    def sync_deployment_workdir(self, deployment_id, local_dir):
        return utils.nullcontext()

    def upload_deployment_file(
        self, target_file_path, src_file, src_file_mtime=None
    ):
        pass

    def delete_deployment_file(self, file_path):
        pass
