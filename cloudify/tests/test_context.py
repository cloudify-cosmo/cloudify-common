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

import logging
import mock
import sys
import os
import shutil
import tempfile
import unittest

import pytest
import requests
import testtools
from mock import patch, Mock

from cloudify_rest_client.exceptions import CloudifyClientError

from cloudify.utils import create_temp_folder
from cloudify.decorators import operation
from cloudify.manager import (
    NodeInstance,
    get_resource,
    get_resource_from_manager
)
from cloudify.workflows import local
from cloudify import constants, state, context, exceptions, conflict_handlers

from cloudify.test_utils import workflow_test


class CloudifyContextTest(testtools.TestCase):
    @classmethod
    def setUpClass(cls):
        state.current_ctx.set(context.CloudifyContext({}), {})

        os.environ[constants.REST_PORT_KEY] = '53333'
        os.environ[constants.REST_HOST_KEY] = "localhost"
        os.environ[constants.MANAGER_FILE_SERVER_SCHEME] = "http"
        _, os.environ[constants.LOCAL_REST_CERT_FILE_KEY] = tempfile.mkstemp()

    def setUp(self):
        super(CloudifyContextTest, self).setUp()
        self.context = context.CloudifyContext({
            'blueprint_id': '',
            'tenant': {'name': 'default_tenant'}
        })
        # the context logger will try to publish messages to rabbit, which is
        # not available here. instead, we redirect the output to stdout.
        self.redirect_log_to_stdout(self.context.logger)

    @classmethod
    def tearDownClass(cls):
        state.current_ctx.clear()
        os.environ.pop(constants.REST_HOST_KEY)
        os.environ.pop(constants.REST_PORT_KEY)
        os.environ.pop(constants.MANAGER_FILE_SERVER_SCHEME)
        os.environ.pop(constants.LOCAL_REST_CERT_FILE_KEY)

    def setup_tenant_context(self):
        self.context = context.CloudifyContext(
            {'blueprint_id': 'test_blueprint',
             'tenant': {'name': 'default_tenant'}})
        self.redirect_log_to_stdout(self.context.logger)

    @staticmethod
    def redirect_log_to_stdout(logger):
        stdout_log_handler = logging.StreamHandler(sys.stdout)
        stdout_log_handler.setLevel(logging.DEBUG)
        logger.handlers = [stdout_log_handler]

    @mock.patch('cloudify.manager.get_rest_client')
    def test_get_resource(self, _):
        with patch('requests.get',
                   return_value=Mock(ok=True, content=b'Hello from test')):
            resource = self.context.get_resource(
                resource_path='for_test_bp_resource.txt')
        self.assertEquals(resource, b'Hello from test')

    @mock.patch('cloudify.manager.get_rest_client')
    def test_download_resource(self, _):
        with patch('requests.get',
                   return_value=Mock(ok=True, content=b'')) as mock_get:
            resource_path = self.context.download_resource(
                resource_path='for_test.txt')

        self.assertEqual(len(mock_get.mock_calls), 1)
        _, args, kwargs = mock_get.mock_calls[0]
        url = args[0]

        expected_url_prefix = '{0}://{1}:{2}/'.format(
            os.environ[constants.MANAGER_FILE_SERVER_SCHEME],
            os.environ[constants.REST_HOST_KEY],
            os.environ[constants.REST_PORT_KEY],
        )
        self.assertTrue(url.startswith(expected_url_prefix))
        self.assertEqual(kwargs['verify'],
                         os.environ[constants.LOCAL_REST_CERT_FILE_KEY])

        self.assertIsNotNone(resource_path)
        self.assertTrue(os.path.exists(resource_path))

    @mock.patch('cloudify.manager.get_rest_client')
    def test_download_blueprint_from_tenant(self, _):
        self.setup_tenant_context()
        with patch('requests.get',
                   return_value=Mock(ok=True, content=b'')) as mock_get:
            resource_path = self.context.download_resource(
                resource_path='blueprint.yaml')

        self.assertEqual(len(mock_get.mock_calls), 1)
        _, args, kwargs = mock_get.mock_calls[0]
        url = args[0]

        expected_url_prefix = '{0}://{1}:{2}/resources/blueprints/{3}'.format(
            os.environ[constants.MANAGER_FILE_SERVER_SCHEME],
            os.environ[constants.REST_HOST_KEY],
            os.environ[constants.REST_PORT_KEY],
            self.context.tenant_name
        )
        self.assertTrue(url.startswith(expected_url_prefix))

        self.assertIsNotNone(resource_path)
        self.assertTrue(os.path.exists(resource_path))

    @mock.patch('cloudify.manager.get_rest_client')
    def test_download_resource_to_specific_file(self, _):
        target_path = "{0}/for_test_custom.log".format(create_temp_folder())
        with patch('requests.get', return_value=Mock(ok=True, content=b'')):
            resource_path = self.context.download_resource(
                resource_path='for_test.txt',
                target_path=target_path)
        self.assertEqual(target_path, resource_path)
        self.assertTrue(os.path.exists(resource_path))

    @mock.patch('cloudify.manager.get_rest_client')
    def test_get_non_existing_resource(self, _):
        self.assertRaises(exceptions.NonRecoverableError,
                          self.context.get_resource,
                          'non_existing.log')

    @mock.patch('cloudify.manager.get_rest_client')
    def test_download_resource_tried_urls(self, _):
        # check that download_resource tries the urls:
        # - http://...//resources/deployments/default_tenant/dep/file.txt
        #   (deployment resource)
        # - http://.../resources/blueprints/default_tenant/bp/file.txt
        #   (blueprint resource)
        # - http://.../resources/file.txt'
        #   (global resource)

        self.context._context['blueprint_id'] = 'bp'
        self.context._context['deployment_id'] = 'dep'
        filename = 'file.txt'

        not_found_err = exceptions.HttpException('', 404, 'Not found')
        with mock.patch('requests.get', side_effect=not_found_err) as mock_get:
            self.assertRaises(
                exceptions.HttpException, self.context.get_resource, filename)
        self.assertEqual(len(mock_get.mock_calls), 3)

        base_url = '{0}://{1}:{2}'.format(
            os.environ[constants.MANAGER_FILE_SERVER_SCHEME],
            os.environ[constants.REST_HOST_KEY],
            os.environ[constants.REST_PORT_KEY]
        )
        self.assertEqual([args[0] for _c, args, _k in mock_get.mock_calls], [
            '{0}/resources/deployments/{1}/{2}/{3}'.format(
                base_url,
                self.context.tenant_name,
                self.context.deployment.id,
                filename),
            '{0}/resources/blueprints/{1}/{2}/{3}'.format(
                base_url,
                self.context.tenant_name,
                self.context.blueprint.id,
                filename),
            '{0}/resources/{1}'.format(base_url, filename)
        ])

    @mock.patch('cloudify.manager.get_rest_client')
    def test_get_from_multiple_managers(self, _):
        # get_resource tries all the managers in a cluster
        with mock.patch(
            'cloudify.utils.get_manager_file_server_url', return_value=[
                'http://server1', 'http://server2']):

            # can't connect to any managers - thats NonRecoverable
            with mock.patch('requests.get', side_effect=[
                requests.ConnectionError(),
                requests.ConnectionError(),
            ]) as mock_get:
                pytest.raises(
                    exceptions.NonRecoverableError,
                    get_resource_from_manager,
                    'resource.txt')
            assert len(mock_get.mock_calls) == 2

            # can't connect to the first, but second is OK
            with mock.patch('requests.get', side_effect=[
                requests.ConnectionError(),
                mock.Mock(ok=True, content='content'),
            ]) as mock_get:
                response = get_resource_from_manager('resource.txt')
            assert response == 'content'
            assert len(mock_get.mock_calls) == 2

            # first is OK already, second not tried
            with mock.patch('requests.get', side_effect=[
                mock.Mock(ok=True, content='content'),
            ]) as mock_get:
                response = get_resource_from_manager('resource.txt')
            assert response == 'content'
            assert len(mock_get.mock_calls) == 1

            # first is 404, but second is OK. First must've not replicated
            # the files yet.
            with mock.patch('requests.get', side_effect=[
                mock.Mock(ok=False, status_code=404, reason='Not found'),
                mock.Mock(ok=True, content='content'),
            ]) as mock_get:
                response = get_resource_from_manager('resource.txt')
            assert response == 'content'
            assert len(mock_get.mock_calls) == 2

            # both are 404 - the exception is reraised
            with mock.patch('requests.get', side_effect=[
                mock.Mock(ok=False, status_code=404, reason='Not found'),
                mock.Mock(ok=False, status_code=404, reason='Not found'),
            ]) as mock_get:
                pytest.raises(
                    exceptions.HttpException,
                    get_resource_from_manager,
                    'resource.txt')
            assert len(mock_get.mock_calls) == 2

    @mock.patch('cloudify.manager.get_rest_client')
    def test_get_resource_raise_http(self, _):
        # get_resource tries 3 paths per manager:
        # - deployment path
        # - blueprint path
        # - global path
        # ...and if it fails, returns a httpexception

        cases_to_try = [
            # all calls return 404 - all tried, httpexception is raised
            [mock.Mock(ok=False, status_code=404, reason='Not found')] * 6,

            # 5x 404 + connectionerror - still wraps with httpexception
            [mock.Mock(ok=False, status_code=404, reason='Not found')] * 5 + \
            [requests.ConnectionError()],

            # all calls return connectionerror
            [requests.ConnectionError()] * 6,

            # 5 connectionerror, and then 404 - all paths are tried
            [requests.ConnectionError()] * 5 + \
            [mock.Mock(ok=False, status_code=404, reason='Not found')]
        ]
        for case in cases_to_try:
            with mock.patch(
                'cloudify.utils.get_manager_file_server_url', return_value=[
                    'http://server1', 'http://server2']):

                with mock.patch('requests.get', side_effect=case) as mock_get:
                    pytest.raises(
                        exceptions.HttpException,
                        get_resource,
                        'blueprint-id', 'deployment-id', 'tenant-name',
                        'resource.txt')
                assert len(mock_get.mock_calls) == 6

    def test_ctx_instance_in_relationship(self):
        ctx = context.CloudifyContext({
            'node_id': 'node-instance-id',
            'related': {
                'node_id': 'related-instance-id',
                'is_target': True
            },
            'relationships': ['related-instance-id']
        })
        self.assertEqual('node-instance-id', ctx.source.instance.id)
        self.assertEqual('related-instance-id', ctx.target.instance.id)
        e = self.assertRaises(exceptions.NonRecoverableError,
                              lambda: ctx.node)
        self.assertIn('ctx.node/ctx.instance can only be used in a '
                      'node-instance context but used in a '
                      'relationship-instance context.', str(e))
        e = self.assertRaises(exceptions.NonRecoverableError,
                              lambda: ctx.instance)
        self.assertIn('ctx.node/ctx.instance can only be used in a '
                      'node-instance context but used in a '
                      'relationship-instance context.', str(e))

    def test_source_target_not_in_relationship(self):
        ctx = context.CloudifyContext({})
        e = self.assertRaises(exceptions.NonRecoverableError,
                              lambda: ctx.source)
        self.assertIn('ctx.source/ctx.target can only be used in a '
                      'relationship-instance context but used in a '
                      'deployment context.', str(e))
        e = self.assertRaises(exceptions.NonRecoverableError,
                              lambda: ctx.target)
        self.assertIn('ctx.source/ctx.target can only be used in a '
                      'relationship-instance context but used in a '
                      'deployment context.', str(e))

    def test_ctx_type(self):
        ctx = context.CloudifyContext({})
        self.assertEqual(constants.DEPLOYMENT, ctx.type)
        ctx = context.CloudifyContext({'node_id': 'node-instance-id'})
        self.assertEqual(constants.NODE_INSTANCE, ctx.type)
        ctx = context.CloudifyContext({
            'node_id': 'node-instance-id',
            'related': {
                'node_id': 'related-instance-id',
                'is_target': True
            },
            'relationships': ['related-instance-id']
        })
        self.assertEqual(constants.RELATIONSHIP_INSTANCE, ctx.type)


class NodeContextTests(testtools.TestCase):

    test_blueprint_path = os.path.join(
        os.path.dirname(os.path.realpath(__file__)),
        "resources/blueprints/test-context-node.yaml")

    @workflow_test(blueprint_path=test_blueprint_path)
    def test_node_type(self, cfy_local):
        cfy_local.execute('execute_operation', parameters={
            'operation': 'test.interface.create',
            'nodes': ['node1', 'node2'],
            'testing': 'test_node_type'})

        expected = {
            'node1': ['test.node1.type', ['test.node1.type']],
            'node2': [
                'test.node2.type',
                ['test.node1.type', 'test.node2.type']]}

        for node in ['node1', 'node2']:
            instance = cfy_local.storage.get_node_instances(node_id=node)[0]
            self.assertEqual(expected[node][0],
                             instance.runtime_properties['type'])
            self.assertEqual(expected[node][1],
                             instance.runtime_properties['type_hierarchy'])


class PluginContextTests(testtools.TestCase):
    # workdir is tested separately for local and remote workflows

    def setUp(self):
        super(PluginContextTests, self).setUp()
        self.plugin_name = 'test_plugin'
        self.plugin_package_name = 'test-plugin'
        self.plugin_package_version = '0.1.1'
        self.deployment_id = 'test_deployment'
        self.tenant_name = 'default_tenant'
        self.ctx = context.CloudifyContext({
            'deployment_id': self.deployment_id,
            'tenant': {'name': self.tenant_name},
            'plugin': {
                'name': self.plugin_name,
                'package_name': self.plugin_package_name,
                'package_version': self.plugin_package_version
            }
        })
        self.test_prefix = tempfile.mkdtemp(prefix='context-plugin-test-')
        self.addCleanup(lambda: shutil.rmtree(self.test_prefix,
                                              ignore_errors=True))

    def test_attributes(self):
        self.assertEqual(self.ctx.plugin.name, self.plugin_name)
        self.assertEqual(self.ctx.plugin.package_name,
                         self.plugin_package_name)
        self.assertEqual(self.ctx.plugin.package_version,
                         self.plugin_package_version)

    def test_prefix_from_wagon(self):
        expected_prefix = os.path.join(
            self.test_prefix,
            'plugins',
            self.tenant_name,
            self.plugin_package_name,
            self.plugin_package_version)
        os.makedirs(expected_prefix)
        with open(os.path.join(expected_prefix, 'plugin.id'), 'w') as f:
            f.write('plugin id')
        with patch('sys.prefix', self.test_prefix):
            self.assertEqual(self.ctx.plugin.prefix, expected_prefix)

    def test_prefix_from_source(self):
        expected_prefix = os.path.join(
            self.test_prefix,
            'source_plugins',
            self.tenant_name,
            self.deployment_id,
            self.plugin_package_name,
            self.plugin_package_version)
        os.makedirs(expected_prefix)
        with open(os.path.join(expected_prefix, 'plugin.id'), 'w') as f:
            f.write('plugin id')
        with patch('sys.prefix', self.test_prefix):
            self.assertEqual(self.ctx.plugin.prefix, expected_prefix)


class GetResourceTemplateTests(testtools.TestCase):

    def __init__(self, *args, **kwargs):
        super(GetResourceTemplateTests, self).__init__(*args, **kwargs)
        self.blueprint_resources_path = os.path.join(
            os.path.dirname(os.path.realpath(__file__)),
            "resources/blueprints/resources")
        self.blueprint_path = os.path.join(
            os.path.dirname(os.path.realpath(__file__)),
            "resources/blueprints/test-get-resource-template.yaml")

    def _assert_rendering(self, env, download,
                          rendered, should_fail_rendering):
        instance = env.storage.get_node_instances(node_id='node1')[0]
        resource = instance.runtime_properties['resource']
        if not should_fail_rendering:
            if download:
                with open(resource, 'rb') as f:
                    rendered_resource = f.read()
            else:
                rendered_resource = resource

            if rendered == 'normal':
                expected_resource_path = \
                    os.path.join(self.blueprint_resources_path,
                                 'rendered_template.conf')
            elif rendered == 'extended':
                expected_resource_path = \
                    os.path.join(self.blueprint_resources_path,
                                 'extended_rendered_template.conf')
            else:
                expected_resource_path = \
                    os.path.join(self.blueprint_resources_path,
                                 'for_template_rendering_tests.conf')

            with open(expected_resource_path, 'rb') as f:
                expected = f.read()
            self.assertEqual(expected, rendered_resource)
        else:
            self.assertEqual('failed', resource)

    def _generic_get_download_template_test(self,
                                            parameters,
                                            download=False,
                                            rendered='normal',
                                            should_fail_rendering=False):
        env = local.init_env(self.blueprint_path)
        updated_params = {'nodes': ['node1']}
        updated_params.update(parameters)
        env.execute('execute_operation',
                    parameters=updated_params)
        self._assert_rendering(env, download,
                               rendered, should_fail_rendering)

    def test_get_resource_template_with_ctx(self):
        self._generic_get_download_template_test({
            'operation': 'get_template',
            'testing': 'get_resource_with_ctx'
        }, rendered='false', should_fail_rendering=True)

    def test_get_resource_no_template(self):
        self._generic_get_download_template_test({
            'operation': 'get_template',
            'testing': 'get_resource_no_template'
        })

    def test_get_resource_empty_template_variables(self):
        self._generic_get_download_template_test({
            'operation': 'get_template',
            'testing': 'get_resource_empty_template'
        })

    def test_get_resource(self):
        self._generic_get_download_template_test({
            'operation': 'get_template',
            'testing': 'get_resource'
        }, rendered='extended')

    def test_download_resource_template_with_ctx(self):
        self._generic_get_download_template_test(
            dict(operation='download_template',
                 testing='download_resource_with_ctx'),
            download=True,
            rendered='false',
            should_fail_rendering=True)

    def test_download_resource_no_template(self):
        self._generic_get_download_template_test(
            dict(operation='download_template',
                 testing='download_resource_no_template'),
            download=True)

    def test_download_resource_empty_template_variables(self):
        self._generic_get_download_template_test(
            dict(operation='download_template',
                 testing='download_resource_empty_template'),
            download=True)

    def test_download_resource(self):
        self._generic_get_download_template_test(
            dict(operation='download_template',
                 testing='download_resource'),
            download=True,
            rendered='extended')


def _context_with_endpoint(endpoint, **kwargs):
    """Get a NodeInstanceContext with the passed stub data."""
    context_kwargs = {
        'context': {'node_id': 'node_id'},
        'endpoint': endpoint,
        'node': None,
        'modifiable': True
    }
    context_kwargs.update(kwargs)
    return context.NodeInstanceContext(**context_kwargs)


class TestPropertiesRefresh(testtools.TestCase):
    def test_refresh_fetches(self):
        """Refreshing a node instance fetches new properties."""
        # first .get_node_instances call returns an instance with value=1
        # next call returns one with value=2
        instances = [
            NodeInstance('id', 'node_id', {'value': 1}),
            NodeInstance('id', 'node_id', {'value': 2})
        ]
        ep = mock.Mock(**{
            'get_node_instance.side_effect': instances
        })
        ctx = _context_with_endpoint(ep)
        self.assertEqual(1, ctx.runtime_properties['value'])
        ctx.refresh()
        self.assertEqual(2, ctx.runtime_properties['value'])

    def test_cant_refresh_dirty(self):
        """Refreshing a dirty instance throws instead of overwriting data."""
        instance = NodeInstance('id', 'node_id', {'value': 1})
        ep = mock.Mock(**{
            'get_node_instance.return_value': instance
        })
        ctx = _context_with_endpoint(ep)
        ctx.runtime_properties['value'] += 5
        try:
            ctx.refresh()
        except exceptions.NonRecoverableError as e:
            self.assertIn('dirty', str(e))
        else:
            self.fail('NonRecoverableError was not thrown')
        self.assertEqual(
            6, ctx.runtime_properties['value'],
            "Instance properties were overwritten, losing local changes.")

    def test_force_overwrites_dirty(self):
        """Force-refreshing a dirty instance overwrites local changes."""

        def get_instance(endpoint):
            # we'll be mutating the instance properties, so make sure
            # we return a new object every time - otherwise the properties
            # would've been overwritten with the same object, not with fresh
            # values.
            return NodeInstance('id', 'node_id', {'value': 1})

        ep = mock.Mock(**{
            'get_node_instance.side_effect': get_instance
        })
        ctx = _context_with_endpoint(ep)
        ctx.runtime_properties['value'] += 5
        ctx.refresh(force=True)
        self.assertEqual(
            1, ctx.runtime_properties['value'],
            "Instance properties were not overwritten but force was used")


class TestPropertiesUpdate(testtools.TestCase):
    ERR_CONFLICT = CloudifyClientError('conflict', status_code=409)

    def test_update(self):
        """.update() without a handler sends the changed runtime properties."""

        def mock_update(instance):
            self.assertEqual({'foo': 42}, instance.runtime_properties)

        instance = NodeInstance('id', 'node_id')
        ep = mock.Mock(**{
            'get_node_instance.return_value': instance,
            'update_node_instance.side_effect': mock_update
        })
        ctx = _context_with_endpoint(ep)
        ctx.runtime_properties['foo'] = 42
        ctx.update()

        ep.update_node_instance.assert_called_once_with(instance)

    def test_update_conflict_no_handler(self):
        """Version conflict without a handler function aborts the operation."""
        instance = NodeInstance('id', 'node_id')

        ep = mock.Mock(**{
            'get_node_instance.return_value': instance,
            'update_node_instance.side_effect': self.ERR_CONFLICT
        })

        ctx = _context_with_endpoint(ep)
        ctx.runtime_properties['foo'] = 42

        try:
            ctx.update()
        except CloudifyClientError as e:
            self.assertEqual(409, e.status_code)
        else:
            self.fail('ctx.update() has hidden the 409 error')

    def test_update_conflict_simple_handler(self):
        """On a conflict, the handler will be called until it succeeds.

        The simple handler function in this test will just increase the
        runtime property value by 1 each call. When the value reaches 5,
        the mock update method will at last allow it to save.
        """
        # each next call of the mock .get_node_instance will return subsequent
        # instances: each time the runtime property is changed
        instances = [NodeInstance('id', 'node_id', {'value': i})
                     for i in range(5)]

        def mock_update(instance):
            if instance.runtime_properties.get('value', 0) < 5:
                raise self.ERR_CONFLICT

        ep = mock.Mock(**{
            'get_node_instance.side_effect': instances,
            'update_node_instance.side_effect': mock_update
        })

        ctx = _context_with_endpoint(ep)
        ctx.runtime_properties['value'] = 1

        def _handler(previous, next_props):
            # the "previous" argument is always the props as they were before
            # .update() was called
            self.assertEqual(previous, {'value': 1})
            return {'value': next_props['value'] + 1}

        handler = mock.Mock(side_effect=_handler)  # Mock() for recording calls
        ctx.update(handler)

        self.assertEqual(5, len(handler.mock_calls))
        self.assertEqual(5, len(ep.update_node_instance.mock_calls))


class TestPropertiesUpdateDefaultMergeHandler(unittest.TestCase):
    ERR_CONFLICT = CloudifyClientError('conflict', status_code=409)

    def test_merge_handler_noconflict(self):
        """The merge builtin handler adds properties that are not present.

        If a property was added locally, but isn't in the storage version,
        it can be added.
        """
        instance = NodeInstance('id', 'node_id', {'value': 1})

        def mock_update(instance):
            # we got both properties merged - the locally added one
            # and the server one
            self.assertEqual({'othervalue': 1, 'value': 1},
                             instance.runtime_properties)

        ep = mock.Mock(**{
            'get_node_instance.return_value': instance,
            'update_node_instance.side_effect': mock_update
        })

        ctx = _context_with_endpoint(ep)
        ctx.runtime_properties['othervalue'] = 1
        ctx.update(conflict_handlers.simple_merge_handler)

        ep.update_node_instance.assert_called_once_with(instance)

    def test_merge_handler_repeated_property(self):
        """Merge handler won't overwrite already existing properties.

        First fetch returns value=1; locally change that to 2 and try to
        update. However server says that's a conflict, and now says value=5.
        Merge handler decides it can't merge and errors out.
        """
        instance = NodeInstance('id', 'node_id', {'value': 1})

        ep = mock.Mock(**{
            'get_node_instance.return_value': instance,
            'update_node_instance.side_effect': self.ERR_CONFLICT
        })
        ctx = _context_with_endpoint(ep)
        ctx.runtime_properties['value'] = 2

        # in the meantime, server's version changed! value is now 5
        ep.get_node_instance.return_value = NodeInstance('id', 'node_id',
                                                         {'value': 5})

        try:
            ctx.update(conflict_handlers.simple_merge_handler)
        except ValueError:
            pass
        else:
            self.fail('merge handler should fail to merge repeated properties')

        self.assertEqual(1, len(ep.update_node_instance.mock_calls))

    def test_merge_handler_conflict_resolved(self):
        """Merge handler can resolve conflicts, adding new properties.

        First fetch returns instance without the 'value' property.
        Handler adds the locally-added 'othervalue' and tries updating.
        That's a conflict, because now the server version has the 'value'
        property. Handler refetches, and is able to merge.
        """

        instances = [NodeInstance('id', 'node_id'),
                     NodeInstance('id', 'node_id', {'value': 1})]

        def mock_update(instance):
            if 'value' not in instance.runtime_properties:
                raise self.ERR_CONFLICT
            self.assertEqual({'othervalue': 1, 'value': 1},
                             instance.runtime_properties)

        ep = mock.Mock(**{
            'get_node_instance.side_effect': instances,
            'update_node_instance.side_effect': mock_update
        })

        ctx = _context_with_endpoint(ep)
        ctx.runtime_properties['othervalue'] = 1
        # at this point we don't know about the 'value' property yet
        ctx.update(conflict_handlers.simple_merge_handler)

        self.assertEqual(2, len(ep.update_node_instance.mock_calls))


@operation
def get_template(ctx, testing, **_):

    resource = 'empty'
    rendering_tests_demo_conf = 'resources/for_template_rendering_tests.conf'

    if testing == 'get_resource_with_ctx':
        try:
            resource = ctx.get_resource_and_render(
                rendering_tests_demo_conf,
                template_variables={'ctx': ctx})
        except exceptions.NonRecoverableError:
            resource = 'failed'

    if testing == 'get_resource_no_template':
        resource = ctx.get_resource_and_render(rendering_tests_demo_conf)

    if testing == 'get_resource_empty_template':
        resource = ctx.get_resource_and_render(rendering_tests_demo_conf,
                                               template_variables={})

    if testing == 'get_resource':
        resource = ctx.get_resource_and_render(
            rendering_tests_demo_conf,
            template_variables={'key': 'value'})

    ctx.instance.runtime_properties['resource'] = resource


@operation
def download_template(ctx, testing, **_):

    resource = 'empty'
    rendering_tests_demo_conf = 'resources/for_template_rendering_tests.conf'

    if testing == 'download_resource_with_ctx':
        try:
            resource = ctx.download_resource_and_render(
                rendering_tests_demo_conf,
                template_variables={'ctx': ctx})
        except exceptions.NonRecoverableError:
            resource = 'failed'

    if testing == 'download_resource_no_template':
        resource = ctx.download_resource_and_render(
            rendering_tests_demo_conf)

    if testing == 'download_resource_empty_template':
        resource = ctx.download_resource_and_render(
            rendering_tests_demo_conf,
            template_variables={})

    if testing == 'download_resource':
        resource = ctx.download_resource_and_render(
            rendering_tests_demo_conf,
            template_variables={'key': 'value'})

    ctx.instance.runtime_properties['resource'] = resource


@operation
def get_node_type(ctx, **kwargs):
    ctx.instance.runtime_properties['type'] = ctx.node.type
    ctx.instance.runtime_properties['type_hierarchy'] = ctx.node.type_hierarchy
