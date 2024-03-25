########
# Copyright (c) 2013 GigaSpaces Technologies Ltd. All rights reserved
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
import re
import ssl
import sys
import time
import pytz
import shlex
import random
import string
import tarfile
import zipfile
import logging
import tempfile
import importlib
import traceback
import threading
import subprocess

from datetime import datetime, timedelta
from contextlib import contextmanager, closing
from functools import wraps
from io import StringIO
from requests.exceptions import ConnectionError, Timeout
import socket   # replace with ipaddress when this is py3-only


from dsl_parser.constants import PLUGIN_INSTALL_KEY, PLUGIN_NAME_KEY

from cloudify import constants
from cloudify.state import workflow_parameters, workflow_ctx, ctx, current_ctx
from cloudify.constants import (SUPPORTED_ARCHIVE_TYPES,
                                KEEP_TRYING_HTTP_TOTAL_TIMEOUT_SEC,
                                MAX_WAIT_BETWEEN_HTTP_RETRIES_SEC)
from cloudify.exceptions import CommandExecutionException, NonRecoverableError

try:
    from packaging.version import parse as parse_version
except ImportError:
    from distutils.version import LooseVersion as parse_version


ENV_CFY_EXEC_TEMPDIR = 'CFY_EXEC_TEMP'
ENV_AGENT_LOG_LEVEL = 'AGENT_LOG_LEVEL'
ENV_AGENT_LOG_DIR = 'AGENT_LOG_DIR'
ENV_AGENT_LOG_MAX_BYTES = 'AGENT_LOG_MAX_BYTES'
ENV_AGENT_LOG_MAX_HISTORY = 'AGENT_LOG_MAX_HISTORY'

ADMIN_API_TOKEN_PATH = '/opt/mgmtworker/work/admin_token'

try:
    from contextlib import nullcontext
except ImportError:
    # python 3.6 doesn't have nullcontext, so let's pretty much copy over
    # 3.7+'s implementation'
    class nullcontext:
        def __init__(self, enter_result=None):
            self.enter_result = enter_result

        def __enter__(self):
            return self.enter_result

        def __exit__(self, *excinfo):
            pass

        async def __aenter__(self):
            return self.enter_result

        async def __aexit__(self, *excinfo):
            pass


class ManagerVersion(object):
    """Cloudify manager version helper class."""

    def __init__(self, raw_version):
        """Raw version, for example: 3.4.0-m1, 3.3, 3.2.1, 3.3-rc1."""

        components = []
        for x in raw_version.split('-')[0].split('.'):
            try:
                components += [int(x)]
            except ValueError:
                pass
        if len(components) == 2:
            components.append(0)
        self.major = components[0]
        self.minor = components[1]
        self.service = components[2]

    def greater_than(self, other):
        """Returns true if this version is greater than the provided one."""

        if self.major > other.major:
            return True
        if self.major == other.major:
            if self.minor > other.minor:
                return True
            if self.minor == other.minor and self.service > other.service:
                return True
        return False

    def equals(self, other):
        """Returns true if this version equals the provided version."""
        return self.major == other.major and self.minor == other.minor and \
            self.service == other.service

    def __str__(self):
        return '{0}.{1}.{2}'.format(self.major, self.minor, self.service)

    def __eq__(self, other):
        return self.equals(other)

    def __gt__(self, other):
        return self.greater_than(other)

    def __lt__(self, other):
        return other > self

    def __ge__(self, other):
        return self > other or self == other

    def __le__(self, other):
        return self < other or self == other

    def __ne__(self, other):
        return self > other or self < other


def setup_logger(logger_name,
                 logger_level=logging.INFO,
                 handlers=None,
                 remove_existing_handlers=True,
                 logger_format=None,
                 propagate=True):
    """
    :param logger_name: Name of the logger.
    :param logger_level: Level for the logger (not for specific handler).
    :param handlers: An optional list of handlers (formatter will be
                     overridden); If None, only a StreamHandler for
                     sys.stdout will be used.
    :param remove_existing_handlers: Determines whether to remove existing
                                     handlers before adding new ones
    :param logger_format: the format this logger will have.
    :param propagate: propagate the message the parent logger.

    :return: A logger instance.
    :rtype: logging.Logger
    """
    if logger_format is None:
        logger_format = '%(asctime)s [%(levelname)s] [%(name)s] %(message)s'
    logger = logging.getLogger(logger_name)

    if remove_existing_handlers:
        for handler in logger.handlers:
            logger.removeHandler(handler)

    if not handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setLevel(logging.DEBUG)
        handlers = [handler]

    formatter = logging.Formatter(fmt=logger_format,
                                  datefmt='%H:%M:%S')
    for handler in handlers:
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    logger.setLevel(logger_level)
    if not propagate:
        logger.propagate = False
    return logger


def format_exception(e):
    """Human-readable representation of an exception, as a bytestring.

    The canonical way to print an exception, str(e), also made to handle
    unicode exception messages in python 2.
    Additionally, if the exception message is incompatible with utf-8,
    (which should only happen in extreme cases, such as NUL bytes),
    fallback to repr().
    """
    try:
        return str(e)
    except UnicodeEncodeError:
        try:
            return u'{0}'.format(e).encode('utf-8')
        except UnicodeEncodeError:
            return repr(e)


def get_daemon_name():
    """Name of the currently running agent."""
    return os.environ.get('AGENT_NAME')


def get_manager_name():
    """Hostname/id of the current manager.

    Available only on mgmtworkers, returns the hostname of the manager
    this is running on.
    """
    return os.environ.get(constants.MANAGER_NAME)


def get_manager_file_server_scheme():
    """Returns the protocol to use for connecting to the fileserver"""
    return os.environ.get(
        constants.MANAGER_FILE_SERVER_SCHEME,
        get_manager_rest_service_protocol(),
    )


def get_manager_file_server_url():
    """
    Returns the manager file server base url.
    """
    port = get_manager_rest_service_port()
    scheme = get_manager_file_server_scheme()
    return [
        '{0}://{1}:{2}/resources'.format(scheme, ipv6_url_compat(host), port)
        for host in get_manager_rest_service_host()
    ]


def get_manager_file_server_root():
    """
    Returns the host the manager REST service is running on.
    """
    return os.environ[constants.MANAGER_FILE_SERVER_ROOT_KEY]


def get_manager_rest_service_protocol():
    return os.environ.get(
        constants.REST_PROTOCOL_KEY,
        constants.SECURED_PROTOCOL,
    )


def get_manager_rest_service_host():
    """
    Returns the host the manager REST service is running on.
    """
    environ_rest_host = os.environ.get(constants.REST_HOST_KEY)
    # hostnames must be ascii, otherwise they're invalid
    if environ_rest_host and environ_rest_host.isprintable():
        hosts = []
        for host in environ_rest_host.split(','):
            host = host.strip()
            # hostnames must not have spaces in them
            if ' ' in host:
                continue
            hosts.append(host)
        # if we seem to have 64 hosts, surely that's an invalid/malformed
        # input, and we should not use that
        if 0 < len(hosts) < 64:
            return hosts
    try:
        # Context could be not available sometimes
        return _get_current_context().rest_host
    except RuntimeError:
        pass


def get_manager_rest_service_port():
    """
    Returns the port the manager REST service is running on.
    """
    environ_rest_port = os.environ.get(constants.REST_PORT_KEY)
    if environ_rest_port and environ_rest_port.isdigit():
        rest_port = int(environ_rest_port)
        if 0 < rest_port < 65536:
            return rest_port
    try:
        return _get_current_context().rest_port
    except RuntimeError:
        pass


def get_broker_ssl_cert_path():
    """
    Returns location of the broker certificate on the agent
    """
    return os.environ[constants.BROKER_SSL_CERT_PATH]


# maintained for backwards compatibility
get_manager_ip = get_manager_rest_service_host


def get_local_rest_certificate():
    """
    Returns the path to the local copy of the server's public certificate
    """
    ssl_cert_key_path = os.environ[constants.LOCAL_REST_CERT_FILE_KEY]
    if not os.path.isfile(ssl_cert_key_path):
        raise RuntimeError(f'Local REST certificate file not found, is '
                           f'{constants.LOCAL_REST_CERT_FILE_KEY} environment '
                           f'variable set?')

    ssl_cert_path = os.path.join(
        os.path.dirname(ssl_cert_key_path),
        'tmp_cloudify_internal_cert.pem'
    )
    ssl_cert_content = None
    try:
        ssl_cert_content = \
            _get_current_context().rest_ssl_cert if \
            hasattr(_get_current_context(), 'rest_ssl_cert') else None
    except RuntimeError:
        pass
    if ssl_cert_content:
        with open(ssl_cert_path, 'w') as f:
            f.write(ssl_cert_content)

    return ssl_cert_path if ssl_cert_content else ssl_cert_key_path


def _get_current_context():
    for context in [ctx, workflow_ctx]:
        try:
            return context._get_current_object()
        except RuntimeError:
            continue
    raise RuntimeError('Context required, but no operation or workflow '
                       'context available.')


def get_execution_creator_username():
    """Returns the execution creator username to use in the logs"""
    try:
        return _get_current_context().execution_creator_username
    except RuntimeError:
        return None


def get_rest_token():
    """
    Returns the auth token to use when calling the REST service
    """
    return _get_current_context().rest_token


def get_execution_token():
    """
    Returns the execution token to use when calling the REST service
    """
    try:
        return _get_current_context().execution_token
    except RuntimeError:  # There is no context
        return None


def get_execution_id():
    """The current execution ID"""
    try:
        return _get_current_context().execution_id
    except RuntimeError:  # There is no context
        return None


def get_instances_of_node(rest_client,
                          deployment_id=None,
                          node_id=None,
                          _ctx=None):
    """
    Get node instances of a node from a deployment.
    """
    _ctx = _ctx or _get_current_context()
    deployment_id = deployment_id or _ctx.deployment.id
    node_id = node_id or _ctx.node.id
    return rest_client.node_instances.list(deployment_id=deployment_id,
                                           node_id=node_id,
                                           sort='index')


def get_tenant():
    """Returns a dict with the details of the current tenant"""
    # this now gets the tenant from REST, however it needs to stay here to
    # not break backwards compat for users of this function (mainly in
    # agent), so we have to import from manager into here
    from cloudify.manager import get_rest_client
    rest_client = get_rest_client()
    return rest_client.tenants.get(get_tenant_name())


def get_tenant_name():
    """
    Returns the tenant name to use when calling the REST service
    """
    return _get_current_context().tenant_name


def get_is_bypass_maintenance():
    """
    Returns true if workflow should run in maintenance mode.
    """
    try:
        return _get_current_context().bypass_maintenance
    except RuntimeError:    # not in context
        return False


def id_generator(size=6, chars=string.ascii_uppercase + string.digits):
    """
    Generate and return a random string using upper case letters and digits.
    """
    return ''.join(random.choice(chars) for _ in range(size))


def get_exec_tempdir():
    """
    Returns the directory to use for temporary files, when the intention
    is to place an executable file there.
    This is needed because some production systems disallow executions from
    the default temporary directory.
    """
    exec_tempdir = os.environ.get(ENV_CFY_EXEC_TEMPDIR)
    if exec_tempdir and os.path.exists(exec_tempdir):
        return os.path.abspath(exec_tempdir)
    return tempfile.gettempdir()


def create_temp_folder():
    """
    Create a temporary folder.
    """
    path_join = os.path.join(get_exec_tempdir(), id_generator(5))
    os.makedirs(path_join)
    return path_join


def exception_to_error_cause(exception, tb):
    error = StringIO()
    etype = type(exception)
    traceback.print_exception(etype, exception, tb, file=error)
    return {
        'message': u'{0}'.format(exception),
        'traceback': error.getvalue(),
        'type': etype.__name__
    }


def get_func(task_name):
    split = task_name.split('.')
    module_name = '.'.join(split[:-1])
    function_name = split[-1]
    try:
        module = importlib.import_module(module_name)
    except ImportError as e:
        raise NonRecoverableError(
            'No module named {0} ({1})'.format(module_name, e))
    try:
        func = getattr(module, function_name)
    except AttributeError:
        raise NonRecoverableError(
            "{0} has no function named '{1}' ".format(module_name,
                                                      function_name))
    if not callable(func):
        raise NonRecoverableError(
            "{0}.{1} is not callable".format(module_name, function_name))
    return func


def get_kerberos_indication(kerberos_env):
    if kerberos_env is None:
        return None
    return str(kerberos_env).lower() == 'true'


def get_workflow_parameters():
    """Get workflow parameters (except `ctx` key) as a dict."""
    p = workflow_parameters.copy()
    if 'ctx' in p:
        del p['ctx']
    return p


def get_local_resources_root():
    return os.environ.get(constants.LOCAL_RESOURCES_ROOT_ENV_KEY)


class LocalCommandRunner(object):

    def __init__(self, logger=None, host='localhost'):
        """
        :param logger: This logger will be used for
                       printing the output and the command.
        """

        logger = logger or setup_logger('LocalCommandRunner')
        self.logger = logger
        self.host = host

    def run(self, command,
            exit_on_failure=True,
            stdout_pipe=True,
            stderr_pipe=True,
            cwd=None,
            execution_env=None,
            encoding='utf-8'):
        """
        Runs local commands.

        :param command: The command to execute.
        :param exit_on_failure: False to ignore failures.
        :param stdout_pipe: False to not pipe the standard output.
        :param stderr_pipe: False to not pipe the standard error.
        :param cwd: the working directory the command will run from.
        :param execution_env: dictionary of environment variables that will
                              be present in the command scope.

        :return: A wrapper object for all valuable info from the execution.
        :rtype: cloudify.utils.CommandExecutionResponse
        """

        if isinstance(command, list):
            popen_args = command
        else:
            popen_args = _shlex_split(command)
        self.logger.debug('[{0}] run: {1}'.format(self.host, popen_args))
        stdout = subprocess.PIPE if stdout_pipe else None
        stderr = subprocess.PIPE if stderr_pipe else None
        command_env = os.environ.copy()
        command_env.update(execution_env or {})
        p = subprocess.Popen(args=popen_args, stdout=stdout,
                             stderr=stderr, cwd=cwd, env=command_env)
        out, err = p.communicate()
        if out is not None:
            out = out.rstrip().decode(encoding, 'replace')
        if err is not None:
            err = err.rstrip().decode(encoding, 'replace')

        if p.returncode != 0:
            error = CommandExecutionException(
                command=command,
                error=err,
                output=out,
                code=p.returncode)
            if exit_on_failure:
                raise error
            else:
                self.logger.error(error)

        return CommandExecutionResponse(
            command=command,
            std_out=out,
            std_err=err,
            return_code=p.returncode)


class CommandExecutionResponse(object):

    """
    Wrapper object for info returned when running commands.

    :param command: The command that was executed.
    :param std_out: The output from the execution.
    :param std_err: The error message from the execution.
    :param return_code: The return code from the execution.
    """

    def __init__(self, command, std_out, std_err, return_code):
        self.command = command
        self.std_out = std_out
        self.std_err = std_err
        self.return_code = return_code


setup_default_logger = setup_logger  # deprecated; for backwards compatibility


def _shlex_split(command):
    lex = shlex.shlex(command, posix=True)
    lex.whitespace_split = True
    lex.escape = ''
    return list(lex)


class Internal(object):

    @staticmethod
    def get_install_method(properties):
        install_agent = properties.get('install_agent')
        if install_agent is False:
            return 'none'
        elif install_agent is True:
            return 'remote'
        else:
            return properties.get('agent_config', {}).get('install_method')

    @staticmethod
    def get_broker_ssl_options(ssl_enabled, cert_path):
        if ssl_enabled:
            ssl_options = {
                'ca_certs': cert_path,
                'cert_reqs': ssl.CERT_REQUIRED,
            }
        else:
            ssl_options = {}
        return ssl_options

    @staticmethod
    def get_broker_credentials(cloudify_agent):
        """Get broker credentials or their defaults if not set."""
        default_user = 'guest'
        default_pass = 'guest'
        default_vhost = '/'

        try:
            broker_user = cloudify_agent.broker_user or default_user
            broker_pass = cloudify_agent.broker_pass or default_pass
            broker_vhost = cloudify_agent.broker_vhost or default_vhost
        except AttributeError:
            # Handle non-agent from non-manager (e.g. for manual tests)
            broker_user = default_user
            broker_pass = default_pass
            broker_vhost = default_vhost

        return broker_user, broker_pass, broker_vhost

    @staticmethod
    @contextmanager
    def _change_tenant(ctx, tenant):
        """
            Temporarily change the tenant the context is pretending to be.
            This is not supported for anything other than snapshot restores.
            If you are thinking of using this for something, it would be
            better not to.
        """
        if 'original_name' in ctx._context['tenant']:
            raise RuntimeError(
                'Overriding tenant name cannot happen while tenant name is '
                'already being overridden.'
            )

        try:
            ctx._context['tenant']['original_name'] = ctx.tenant_name
            ctx._context['tenant']['name'] = tenant
            yield
        finally:
            ctx._context['tenant']['name'] = (
                ctx._context['tenant']['original_name']
            )
            ctx._context['tenant'].pop('original_name')


def is_management_environment():
    """
    Checks whether we're currently running within a management worker.
    """
    return os.environ.get('MGMTWORKER_HOME')


def extract_archive(source):
    if tarfile.is_tarfile(source):
        return untar(source)
    elif zipfile.is_zipfile(source):
        return unzip(source)
    raise NonRecoverableError(
        'Unsupported archive type provided or archive is not valid: {0}.'
        ' Supported archive types are: {1}'
        .format(source, SUPPORTED_ARCHIVE_TYPES)
    )


def unzip(archive, destination=None):
    if not destination:
        destination = tempfile.mkdtemp()
    with closing(zipfile.ZipFile(archive, 'r')) as zip_file:
        zip_file.extractall(destination)
    return destination


def untar(archive, destination=None):
    if not destination:
        destination = tempfile.mkdtemp()
    with closing(tarfile.open(name=archive)) as tar:
        tar.extractall(path=destination, members=tar.getmembers())
    return destination


def generate_user_password(password_length=32):
    """Generate random string to use as user password."""
    system_random = random.SystemRandom()
    allowed_characters = (
        string.ascii_letters +
        string.digits +
        '-_'
    )

    password = ''.join(
        system_random.choice(allowed_characters)
        for _ in range(password_length)
    )
    return password


def get_admin_api_token():
    token = os.environ.get(constants.ADMIN_API_TOKEN_KEY)

    if not token:
        with open(ADMIN_API_TOKEN_PATH, 'r') as token_file:
            token = token_file.read()
            token = token.strip()

    return token


internal = Internal()


def extract_plugins_to_install(plugin_list, filter_func):
    """
    :param plugin_list: list of plugins.
    :return: a list of plugins that are marked for installation (that
        potentially need installation) and pass the given filter function.
    """
    return [p for p in plugin_list if p[PLUGIN_INSTALL_KEY] and filter_func(p)]


def extract_and_merge_plugins(deployment_plugins,
                              workflow_plugins,
                              filter_func=lambda _: True,
                              with_repetition=False):
    """
    :param deployment_plugins: deployment plugins to install.
    :param workflow_plugins: workflow plugins to install.
    :param filter_func: predicate function to filter the plugins, should return
     True for plugins that don't need to be filtered out.
    :param with_repetition: can plugins appear twice in the list.
    :return: a list of merged plugins that were marked for installation.
    """
    dep_plugins = extract_plugins_to_install(deployment_plugins, filter_func)
    wf_plugins = extract_plugins_to_install(workflow_plugins, filter_func)
    if with_repetition:
        return dep_plugins + wf_plugins
    return merge_plugins(dep_plugins, wf_plugins)


def merge_plugins(deployment_plugins, workflow_plugins):
    """Merge plugins to install.

    :param deployment_plugins: deployment plugins to install.
    :param workflow_plugins: workflow plugins to install.
    :return: the merged plugins.
    """
    added_plugins = set()
    result = []

    def add_plugins(plugins):
        for plugin in plugins:
            if plugin[PLUGIN_NAME_KEY] in added_plugins:
                continue
            added_plugins.add(plugin[PLUGIN_NAME_KEY])
            result.append(plugin)

    for plugins in (deployment_plugins, workflow_plugins):
        add_plugins(plugins)
    return result


def add_plugins_to_install(ctx, plugins_to_install, sequence):
    """Adds a task to the sequence that installs the plugins.
    """
    if not sequence or not ctx or not plugins_to_install:
        return
    sequence.add(
        ctx.send_event('Installing deployment and workflow plugins'),
        ctx.execute_task(
            task_name='cloudify_agent.operations.install_plugins',
            kwargs={'plugins': plugins_to_install}))


def add_plugins_to_uninstall(ctx, plugins_to_uninstall, sequence):
    """Adds a task to the sequence that uninstalls the plugins.
    """
    if not sequence or not ctx or not plugins_to_uninstall:
        return
    sequence.add(
        ctx.send_event('Uninstalling deployment and workflow plugins'),
        ctx.execute_task(
            task_name='cloudify_agent.operations.uninstall_plugins',
            kwargs={
                'plugins': plugins_to_uninstall,
                'delete_managed_plugins': False}))


def wait_for(callable_obj,
             callable_obj_key,
             value_attr,
             test_condition,
             exception_class,
             msg='',
             timeout=900):
    deadline = time.time() + timeout
    while True:
        if time.time() > deadline:
            raise exception_class(msg)
        value = callable_obj(callable_obj_key)
        if test_condition(getattr(value, value_attr)):
            return value
        time.sleep(3)


class OutputConsumer(object):
    def __init__(self, out, logger, prefix, ctx=None):
        self.out = out
        self.output = []
        self.logger = logger
        self.prefix = prefix
        self.ctx = ctx
        self.consumer = threading.Thread(target=self.consume_output)
        self.consumer.daemon = True
        self.consumer.start()

    def consume_output(self):
        if self.ctx is not None:
            with current_ctx.push(self.ctx):
                self._do_consume()
        else:
            self._do_consume()

    def _do_consume(self):
        for line in self.out:
            line = line.decode('utf-8', 'replace')
            self.output.append(line)
            line = line.rstrip('\r\n')
            self.logger.info("%s%s", self.prefix, line)
        self.out.close()

    def join(self):
        self.consumer.join()


def get_executable_path(executable, venv):
    """Lookup the path to the executable, os agnostic

    :param executable: the name of the executable
    :param venv: the venv to look for the executable in
    """
    if os.name == 'nt':
        return '{0}\\Scripts\\{1}'.format(venv, executable)
    else:
        return '{0}/bin/{1}'.format(venv, executable)


def get_python_path(venv):
    """Path to the python executable in the given venv"""
    return get_executable_path(
        'python.exe' if os.name == 'nt' else 'python',
        venv=venv
    )


def _is_plugin_dir(path):
    """Is the given path a directory containing a plugin?"""
    return (os.path.isdir(path) and
            os.path.exists(os.path.join(path, 'plugin.id')))


def _find_versioned_plugin_dir(base_dir, version):
    """In base_dir, find a subdirectory containing the version, or the newest.

    If version is not None, the plugin directory is base_dir+version.
    If version is None, then the plugin directory is the highest version
    from base_dir.

    For example, say base_dir contains the subdirectories: 1.0.0, 2.0.0:
    >>> _find_versioned_plugin_dir('/plugins', None)
    /plugins/2.0.0
    >>> _find_versioned_plugin_dir('/plugins', '1.0.0')
    /plugins/1.0.0
    >>> _find_versioned_plugin_dir('/plugins', '3.0.0')
    None
    """
    if not os.path.isdir(base_dir):
        return
    if version:
        found = os.path.join(base_dir, version)
    else:
        available_versions = [
            (d, parse_version(d))
            for d in os.listdir(base_dir)
            if _is_plugin_dir(os.path.join(base_dir, d)) and
            parse_version(d) is not None
        ]
        if not available_versions:
            return
        newest = max(available_versions, key=lambda pair: pair[1])[0]
        found = os.path.join(base_dir, newest)
    if _is_plugin_dir(found):
        return found


def _plugins_base_dir():
    """The directory where plugins/ and source_plugins/ are stored.

    Default to sys.prefix, which is going to be in the mgmtworker/agent venv.
    """
    plugins_root = os.environ.get('CFY_PLUGINS_ROOT')
    if plugins_root and os.path.exists(plugins_root):
        return os.path.abspath(plugins_root)
    return sys.prefix


def plugin_prefix(name, tenant_name, version=None, deployment_id=None):
    """Virtualenv for the specified plugin.

    If version is not provided, the highest version is used.

    :param name: package name of the plugin
    :param tenant_name: tenant name, or None for local executions
    :param version: version of the plugin, or None for the newest version
    :param deployment_id: deployment id, for source plugins
    :return: directory containing the plugin virtualenv, or None
    """
    managed_plugin_dir = os.path.join(_plugins_base_dir(), 'plugins')
    if tenant_name:
        managed_plugin_dir = os.path.join(
            managed_plugin_dir, tenant_name, name)
    else:
        managed_plugin_dir = os.path.join(managed_plugin_dir, name)

    prefix = _find_versioned_plugin_dir(managed_plugin_dir, version)
    if prefix is None and deployment_id is not None:
        source_plugin_dir = os.path.join(_plugins_base_dir(), 'source_plugins')
        if tenant_name:
            source_plugin_dir = os.path.join(
                source_plugin_dir, tenant_name, deployment_id, name)
        else:
            source_plugin_dir = os.path.join(
                source_plugin_dir, deployment_id, name)
        prefix = _find_versioned_plugin_dir(
            source_plugin_dir, version)
    return prefix


# this function must be kept in sync with the above plugin_prefix,
# so that the target directory created by this, can be then found
# by plugin_prefix
def target_plugin_prefix(name, tenant_name, version=None, deployment_id=None):
    """Target directory into which the plugin should be installed"""
    parts = [
        _plugins_base_dir(),
        'source_plugins' if deployment_id else 'plugins'
    ]
    if tenant_name:
        parts.append(tenant_name)
    if deployment_id:
        parts.append(deployment_id)
    # if version is not given, default to 0.0.0 so that version that _are_
    # declared always take precedence
    parts += [name, version or '0.0.0']
    return os.path.join(*parts)


def parse_utc_datetime(time_expression, timezone=None):
    """
    :param time_expression: a string representing date a and time.
        The following formats are possible: YYYY-MM-DD HH:MM, HH:MM,
        or a time delta expression such as '+2 weeks' or '+1day+10min.
    :param timezone: a string representing a timezone. Any timezone recognized
        by UNIX can be used here, e.g. 'EST' or Asia/Jerusalem.
    :return: A naive datetime object, in UTC time.
    """
    if not time_expression:
        return None
    if time_expression.startswith('+'):
        return parse_utc_datetime_relative(time_expression)
    return parse_utc_datetime_absolute(time_expression, timezone)


def parse_utc_datetime_relative(time_expression, base_datetime=None):
    """
    :param time_expression: a string representing a relative time delta,
        such as '+2 weeks' or '+1day+10min.
    :param base_datetime: a datetime object representing the absolute date
        and time to which we apply the time delta. By default: UTC now
    :return: A naive datetime object, in UTC time.
    """
    if not time_expression:
        return None
    base_datetime = base_datetime or datetime.utcnow()
    deltas = re.findall(r"(\+\d+\ ?[a-z]+\ ?)", time_expression)
    if not deltas:
        raise NonRecoverableError(
            "{} is not a legal time delta".format(time_expression))
    date_time = base_datetime.replace(second=0, microsecond=0)
    for delta in deltas:
        date_time = parse_and_apply_timedelta(
            delta.rstrip().lstrip('+'), date_time)
    return date_time


def parse_utc_datetime_absolute(time_expression, timezone=None):
    """
    :param time_expression: a string representing an absolute date and time.
        The following formats are possible: YYYY-MM-DD HH:MM, HH:MM
    :param timezone: a string representing a timezone. Any timezone recognized
        by UNIX can be used here, e.g. 'EST' or Asia/Jerusalem.
    :return: A naive datetime object, in UTC time.
    """
    if not time_expression:
        return None
    date_time = parse_schedule_datetime_string(time_expression)
    if timezone:
        if timezone not in pytz.all_timezones:
            raise NonRecoverableError(
                "{} is not a recognized timezone".format(timezone))
        return pytz.timezone(timezone).localize(date_time).astimezone(
            pytz.utc).replace(tzinfo=None)
    else:
        ts = time.time()
        return date_time - (datetime.fromtimestamp(ts) -
                            datetime.utcfromtimestamp(ts))


def unpack_timedelta_string(expr):
    """
    :param expr: a string representing a time delta, such as '+2 weeks',
        '+1', or '+10min'.
    :return: a tuple of int + period string, e.g. (2, 'weeks')
    """
    match = r"^(\d+)\ ?(min(ute)?|h(our)?|d(ay)?|w(eek)?|mo(nth)?|y(ear)?)s?$"
    parsed = re.findall(match, expr)
    if not parsed or len(parsed[0]) < 2:
        raise NonRecoverableError("{} is not a legal time delta".format(expr))
    return int(parsed[0][0]), parsed[0][1]


def parse_and_apply_timedelta(expr, date_time):
    """
    :param expr: a string representing a time delta, such as '+2 weeks',
        '+1', or '+10min'.
    :param date_time: a datetime object
    :return: a datetime object with added time delta
    """
    number, period = unpack_timedelta_string(expr)
    if period in ['y', 'year']:
        return date_time.replace(year=date_time.year + number)
    if period in ['mo', 'month']:
        new_month = (date_time.month + number) % 12
        new_year = date_time.year + (date_time.month + number) // 12
        if new_month == 0:
            new_month = 12
            new_year -= 1
        new_day = date_time.day
        date_time = date_time.replace(day=1, month=new_month, year=new_year)
        return date_time + timedelta(days=new_day - 1)
    if period in ['w', 'week']:
        return date_time + timedelta(days=number * 7)
    if period in ['d', 'day']:
        return date_time + timedelta(days=number)
    if period in ['h', 'hour']:
        period = 'hours'
    elif period in ['min', 'minute']:
        period = 'minutes'
    return date_time + timedelta(**{period: number})


def parse_schedule_datetime_string(date_str):
    try:
        return datetime.strptime(date_str, '%Y-%m-%d %H:%M')
    except ValueError:
        pass
    try:
        return datetime.combine(
            datetime.today(), datetime.strptime(date_str, '%H:%M').time())
    except ValueError:
        raise NonRecoverableError(
            "{} is not a legal time format. accepted formats are "
            "YYYY-MM-DD HH:MM | HH:MM".format(date_str))


def _is_ipv6(addr):
    """Verifies if `addr` is a valid IPv6 address."""
    # TODO replace socket with ipaddress once we're py3-only
    try:
        socket.inet_pton(socket.AF_INET6, addr)
    except socket.error:
        return False
    return True


def ipv6_url_compat(addr):
    """Return URL-compatible version of IPv6 address (or just an address)."""
    if _is_ipv6(addr):
        return '[{0}]'.format(addr)
    return addr


def uuid4():
    """Generate a random UUID, and return a string representation of it.

    This is pretty much a copy of the stdlib uuid4. We inline it here,
    because we'd like to avoid importing the stdlib uuid module on the
    operation dispatch critical path, because importing the stdlib
    uuid module runs some subprocesses (for detecting the uuid1-uuid3 MAC
    address), and that causes more memory pressure than we'd like.
    """
    uuid_bytes = os.urandom(16)
    uuid_as_int = int.from_bytes(uuid_bytes, byteorder='big')
    uuid_as_int &= ~(0xc000 << 48)
    uuid_as_int |= 0x8000 << 48
    uuid_as_int &= ~(0xf000 << 64)
    uuid_as_int |= 4 << 76
    uuid_as_hex = '%032x' % uuid_as_int
    return '%s-%s-%s-%s-%s' % (
        uuid_as_hex[:8],
        uuid_as_hex[8:12],
        uuid_as_hex[12:16],
        uuid_as_hex[16:20],
        uuid_as_hex[20:]
    )


def keep_trying_http(total_timeout_sec=KEEP_TRYING_HTTP_TOTAL_TIMEOUT_SEC,
                     max_delay_sec=MAX_WAIT_BETWEEN_HTTP_RETRIES_SEC):
    """
    Keep (re)trying HTTP requests.

    This is a wrapper function that will keep (re)running whatever function it
    wraps until it raises a "non-repairable" exception (where "repairable"
    exceptions are `requests.exceptions.ConnectionError` and
    `requests.exceptions.Timeout`), or `total_timeout_sec` is exceeded.

    :param total_timeout_sec: The amount of seconds to keep trying, if `None`,
                              there is no timeout and the wrapped function
                              will be re-tried indefinitely.
    :param max_delay_sec: The maximum delay (in seconds) between consecutive
                          calls to the wrapped function.  The actual delay will
                          be a pseudo-random number between
                          `0` and `max_delay_sec`.
    """
    logger = setup_logger('http_retrying')

    def ex_to_str(exception):
        return str(exception) or str(type(exception))

    def inner(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            timeout_at = None if total_timeout_sec is None \
                else datetime.utcnow() + timedelta(seconds=total_timeout_sec)
            while True:
                try:
                    return func(*args, **kwargs)
                except (ConnectionError, Timeout) as ex:
                    if timeout_at and datetime.utcnow() > timeout_at:
                        logger.error(f'Finished retrying {func}: '
                                     f'total timeout of {total_timeout_sec} '
                                     f'seconds exceeded: {ex_to_str(ex)}')
                        raise
                    delay = random.randint(0, max_delay_sec)
                    logger.warning(f'Will retry {func} in {delay} seconds: ' +
                                   ex_to_str(ex))
                    time.sleep(delay)
                except Exception as ex:
                    logger.error(f'Will not retry {func}: the encountered '
                                 'error cannot be fixed by retrying: ' +
                                 ex_to_str(ex))
                    raise
        return wrapper
    return inner
