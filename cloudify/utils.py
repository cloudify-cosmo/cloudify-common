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
import ssl
import sys
import time
import pika
import shlex
import random
import string
import tarfile
import zipfile
import logging
import tempfile
import StringIO
import importlib
import traceback
import subprocess

from distutils.version import StrictVersion
from contextlib import contextmanager, closing

from dsl_parser.constants import PLUGIN_INSTALL_KEY, PLUGIN_NAME_KEY

from cloudify import constants
from cloudify.state import workflow_ctx, ctx
from cloudify.constants import SUPPORTED_ARCHIVE_TYPES
from cloudify.amqp_client import BlockingRequestResponseHandler
from cloudify.exceptions import CommandExecutionException, NonRecoverableError

ENV_CFY_EXEC_TEMPDIR = 'CFY_EXEC_TEMP'
ENV_AGENT_LOG_LEVEL = 'AGENT_LOG_LEVEL'
ENV_AGENT_LOG_DIR = 'AGENT_LOG_DIR'
ENV_AGENT_LOG_MAX_BYTES = 'AGENT_LOG_MAX_BYTES'
ENV_AGENT_LOG_MAX_HISTORY = 'AGENT_LOG_MAX_HISTORY'

INSPECT_TIMEOUT = 30
ADMIN_API_TOKEN_PATH = '/opt/mgmtworker/work/admin_token'


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
            return unicode(e).encode('utf-8')
        except UnicodeEncodeError:
            return repr(e)


def get_manager_name():
    """Hostname/id of the current manager.

    Available only on mgmtworkers, returns the hostname of the manager
    this is running on.
    """
    return os.environ[constants.MANAGER_NAME]


def get_manager_file_server_scheme():
    """
    Returns the manager file server base url.
    """
    return os.environ.get(constants.MANAGER_FILE_SERVER_SCHEME, 'https')


def get_manager_file_server_url():
    """
    Returns the manager file server base url.
    """
    port = get_manager_rest_service_port()
    scheme = get_manager_file_server_scheme()
    return [
        '{0}://{1}:{2}/resources'.format(scheme, host, port)
        for host in get_manager_rest_service_host()
    ]


def get_manager_file_server_root():
    """
    Returns the host the manager REST service is running on.
    """
    return os.environ[constants.MANAGER_FILE_SERVER_ROOT_KEY]


def get_manager_rest_service_host():
    """
    Returns the host the manager REST service is running on.
    """
    return os.environ[constants.REST_HOST_KEY].split(',')


def get_broker_ssl_cert_path():
    """
    Returns location of the broker certificate on the agent
    """
    return os.environ[constants.BROKER_SSL_CERT_PATH]


# maintained for backwards compatibility
get_manager_ip = get_manager_rest_service_host


def get_manager_rest_service_port():
    """
    Returns the port the manager REST service is running on.
    """
    return int(os.environ[constants.REST_PORT_KEY])


def get_local_rest_certificate():
    """
    Returns the path to the local copy of the server's public certificate
    """
    return os.environ[constants.LOCAL_REST_CERT_FILE_KEY]


def _get_current_context():
    for context in [ctx, workflow_ctx]:
        try:
            return context._get_current_object()
        except RuntimeError:
            continue
    raise RuntimeError('Context required, but no operation or workflow '
                       'context available.')


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
    return os.environ.get(constants.BYPASS_MAINTENANCE, '').lower() == 'true'


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
    return os.environ.get(ENV_CFY_EXEC_TEMPDIR) or tempfile.gettempdir()


def create_temp_folder():
    """
    Create a temporary folder.
    """
    path_join = os.path.join(get_exec_tempdir(), id_generator(5))
    os.makedirs(path_join)
    return path_join


def exception_to_error_cause(exception, tb):
    error = StringIO.StringIO()
    etype = type(exception)
    traceback.print_exception(etype, exception, tb, file=error)
    return {
        'message': str(exception),
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
            execution_env=None):

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
        if out:
            out = out.rstrip()
        if err:
            err = err.rstrip()

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
    def _get_formatted_version(version):
        try:
            return StrictVersion(version)
        except ValueError:
            return None

    @staticmethod
    def _get_package_version(plugins_dir, package_name):
        # get all plugin dirs
        subdirs = next(os.walk(plugins_dir))[1]
        # filter by package name
        package_dirs = [dir for dir in subdirs if dir.startswith(package_name)]
        # cut package name prefix
        versions = [dir[len(package_name) + 1:] for dir in package_dirs]
        # return the latest version
        newest = max(versions, key=Internal._get_formatted_version)
        if Internal._get_formatted_version(newest) is None:
            return
        return newest

    @staticmethod
    def plugin_prefix(package_name=None, package_version=None,
                      deployment_id=None, plugin_name=None, tenant_name=None,
                      sys_prefix_fallback=True):
        tenant_name = tenant_name or ''
        plugins_dir = os.path.join(sys.prefix, 'plugins', tenant_name)
        prefix = None
        if package_name:
            package_version = package_version or Internal._get_package_version(
                plugins_dir, package_name)
            wagon_dir = os.path.join(
                plugins_dir, '{0}-{1}'.format(package_name, package_version))
            if os.path.isdir(wagon_dir):
                prefix = wagon_dir
        if prefix is None and deployment_id and plugin_name:
            source_dir = os.path.join(
                plugins_dir, '{0}-{1}'.format(deployment_id, plugin_name))
            if os.path.isdir(source_dir):
                prefix = source_dir
        if prefix is None and sys_prefix_fallback:
            prefix = sys.prefix
        return prefix

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


def is_agent_alive(name,
                   client,
                   timeout=INSPECT_TIMEOUT,
                   connect=True):
    """
    Send a `ping` service task to an agent, and validate that a correct
    response is received

    :param name: the agent's amqp exchange name
    :param client: an AMQPClient for the agent's vhost
    :param timeout: how long to wait for the response
    :param connect: whether to connect the client (should be False if it is
                    already connected)
    """
    handler = BlockingRequestResponseHandler(name)
    client.add_handler(handler)
    if connect:
        with client:
            response = _send_ping_task(name, handler, timeout)
    else:
        response = _send_ping_task(name, handler, timeout)
    return 'time' in response


def _send_ping_task(name, handler, timeout=INSPECT_TIMEOUT):
    logger = setup_logger('cloudify.utils.is_agent_alive')
    task = {
        'service_task': {
            'task_name': 'ping',
            'kwargs': {}
        }
    }
    # messages expire shortly before we hit the timeout - if they haven't
    # been handled by then, they won't make the timeout
    expiration = (timeout * 1000) - 200  # milliseconds
    try:
        return handler.publish(task, routing_key='service',
                               timeout=timeout, expiration=expiration)
    except pika.exceptions.AMQPError as e:
        logger.warning('Could not send a ping task to {0}: {1}'
                       .format(name, e))
        return {}
    except RuntimeError as e:
        logger.info('No ping response from {0}: {1}'.format(name, e))
        return {}


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
        string.letters +
        string.digits +
        '-_'
    )

    password = ''.join(
        system_random.choice(allowed_characters)
        for _ in xrange(password_length)
    )
    return password


def get_admin_api_token():
    with open(ADMIN_API_TOKEN_PATH, 'r') as token_file:
        token = token_file.read()
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
