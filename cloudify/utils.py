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

from contextlib import contextmanager
import logging
import os
import random
import shlex
import ssl
import string
import subprocess
import sys
import tempfile
import traceback
import StringIO

from distutils.version import LooseVersion

from cloudify import cluster, constants
from cloudify.state import workflow_ctx, ctx
from cloudify.exceptions import CommandExecutionException


CFY_EXEC_TEMPDIR_ENVVAR = 'CFY_EXEC_TEMP'


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


def get_manager_file_server_url():
    """
    Returns the manager file server base url.
    """
    if cluster.is_cluster_configured():
        active_node_ip = cluster.get_cluster_active()
        port = get_manager_rest_service_port()
        if active_node_ip:
            return 'https://{0}:{1}/resources'.format(active_node_ip, port)
    return os.environ[constants.MANAGER_FILE_SERVER_URL_KEY]


def get_manager_file_server_root():
    """
    Returns the host the manager REST service is running on.
    """
    return os.environ[constants.MANAGER_FILE_SERVER_ROOT_KEY]


def get_manager_rest_service_host():
    """
    Returns the host the manager REST service is running on.
    """
    return os.environ[constants.REST_HOST_KEY]


def get_broker_ssl_cert_path():
    """
    Returns location of the broker certificate on the agent
    """
    if cluster.is_cluster_configured():
        active_node = cluster.get_cluster_active() or {}
        broker_ssl_cert_path = active_node.get('internal_cert_path')
        if broker_ssl_cert_path:
            return broker_ssl_cert_path
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


def get_tenant():
    """
    Returns a dict with the details of the current tenant
    """
    return _get_current_context().tenant


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
    return os.environ.get(CFY_EXEC_TEMPDIR_ENVVAR) or tempfile.gettempdir()


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


if sys.version_info >= (2, 7):
    # requires 2.7+
    def wait_for_event(evt, poll_interval=0.5):
        """Wait for a threading.Event by polling, which allows handling signals.
        (ie. doesnt block ^C)
        """
        while True:
            if evt.wait(poll_interval):
                return
else:
    def wait_for_event(evt, poll_interval=None):
        """Wait for a threading.Event. Stub for compatibility."""
        # in python 2.6, Event.wait always returns None, so we can either:
        #  - .wait() without a timeout and block ^C which is inconvenient
        #  - .wait() with timeout and then check .is_set(),
        #     which is not threadsafe
        # We choose the inconvenient but safe method.
        evt.wait()


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
    def _get_package_version(plugins_dir, package_name):
        # get all plugin dirs
        subdirs = next(os.walk(plugins_dir))[1]
        # filter by package name
        package_dirs = [dir for dir in subdirs if dir.startswith(package_name)]
        # cut package name prefix
        versions = [dir[len(package_name) + 1:] for dir in package_dirs]
        # sort versions from new to old
        versions.sort(key=lambda version: LooseVersion(version), reverse=True)
        # return the latest
        return versions[0]

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


internal = Internal()
