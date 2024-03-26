#########
# Copyright (c) 2014 GigaSpaces Technologies Ltd. All rights reserved
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
#  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  * See the License for the specific language governing permissions and
#  * limitations under the License.

import os
import re
import sys
import glob
import json
import errno
import shutil
import tempfile
import platform
import threading
import subprocess

from os import walk
from functools import wraps
from contextlib import contextmanager
from urllib.parse import urljoin
from urllib.request import pathname2url

import wagon
import fasteners

from cloudify import ctx
from cloudify.manager import get_rest_client
from cloudify.utils import (
    LocalCommandRunner, target_plugin_prefix, extract_archive,
    get_python_path, get_manager_name, get_daemon_name,
)
from cloudify.exceptions import (
    NonRecoverableError,
    CommandExecutionException,
    PluginInstallationError
)
from cloudify.models_states import PluginInstallationState
from cloudify_rest_client.exceptions import CloudifyClientError

try:
    from packaging.version import parse as parse_version
except ImportError:
    from distutils.version import LooseVersion as parse_version

PLUGIN_INSTALL_LOCK = threading.Lock()
runner = LocalCommandRunner()


def _manage_plugin_state(pre_state, post_state, allow_missing=False):
    """Update plugin state when doing a plugin operation.

    Decorate a function that takes a plugin as the first argument with this,
    and the plugin's state will be updated to pre_state before running
    the function, and to post_state after. If the function throws, then
    to the ERROR state.
    """
    def _set_state(plugin, **kwargs):
        client = get_rest_client()
        if not plugin.get('id'):
            # we don't have the full plugin details, try to retrieve them
            # from the restservice
            managed_plugin = get_managed_plugin(plugin)
            if managed_plugin and managed_plugin.get('id'):
                plugin = managed_plugin
            else:
                return

        manager = None
        agent = get_daemon_name()
        if agent is None:
            manager = get_manager_name()

        try:
            client.plugins.set_state(
                plugin['id'], agent_name=agent, manager_name=manager, **kwargs
            )
        except CloudifyClientError as e:
            if e.status_code != 404 or not allow_missing:
                raise

    def _decorator(f):
        @wraps(f)
        def _inner(plugin, *args, **kwargs):
            if pre_state:
                _set_state(plugin, state=pre_state)
            try:
                rv = f(plugin, *args, **kwargs)
            except Exception as e:
                _set_state(plugin, state=PluginInstallationState.ERROR,
                           error=str(e))
                raise
            else:
                if post_state:
                    _set_state(plugin, state=post_state)
                return rv
        return _inner
    return _decorator


def install(plugin, deployment_id=None, blueprint_id=None):
    """Install the plugin to the current virtualenv.

    :param plugin: A plugin structure as defined in the blueprint.
    :param deployment_id: The deployment id associated with this
                          installation.
    :param blueprint_id: The blueprint id associated with this
                         installation. if specified, will be used
                         when downloading plugins that were included
                         as part of the blueprint itself.
    """
    managed_plugin = get_managed_plugin(plugin)
    args = get_plugin_args(plugin)
    if managed_plugin:
        _install_managed_plugin(managed_plugin, args=args)
        return

    source = get_plugin_source(plugin, blueprint_id)
    if source:
        _install_source_plugin(
            deployment_id=deployment_id,
            plugin=plugin,
            source=source,
            args=args)
    else:
        name = plugin.get('package_name') or plugin.get('name')
        raise NonRecoverableError(
           f'No source or managed plugin found for {name} '
           f'[current platform={platform.system()}, '
           f'arch={platform.machine()}]')


def _make_virtualenv(python_executable, path):
    """Make a venv and link the current venv to it.

    The new venv will have the current venv linked, ie. it will be
    able to import libraries from the current venv, but libraries
    installed directly will have precedence.
    """
    runner.run([python_executable, '-m', 'venv', '--without-pip', path])
    _link_virtualenv(python_executable, path)


def is_already_installed(dst_dir, plugin_id):
    ctx.logger.debug('Checking if managed plugin installation exists '
                     'in %s', dst_dir)
    if os.path.exists(dst_dir):
        ctx.logger.debug('Plugin path exists: %s', dst_dir)
        plugin_id_path = os.path.join(dst_dir, 'plugin.id')
        if os.path.exists(plugin_id_path):
            ctx.logger.debug('Plugin id path exists: %s', plugin_id_path)
            with open(plugin_id_path) as f:
                existing_plugin_id = f.read().strip()
            if existing_plugin_id == plugin_id:
                return True
            else:
                raise PluginInstallationError(
                    'Managed plugin installation found but its ID '
                    'does not match the ID of the plugin currently '
                    'on the manager. [existing: {0}, new: {1}]'
                    .format(existing_plugin_id, plugin_id))
        else:
            raise PluginInstallationError(
                'Managed plugin installation found but it is '
                'in a corrupted state. [{0}]'.format(plugin_id))


def _get_plugin_description(managed_plugin):
    fields = ['package_name',
              'package_version',
              'supported_platform',
              'distribution',
              'distribution_release']
    return ', '.join('{0}: {1}'.format(
        field, managed_plugin.get(field))
        for field in fields if managed_plugin.get(field))


@_manage_plugin_state(pre_state=PluginInstallationState.INSTALLING,
                      post_state=PluginInstallationState.INSTALLED)
def _install_managed_plugin(plugin, args):
    dst_dir = target_plugin_prefix(
        name=plugin.package_name,
        tenant_name=ctx.tenant_name,
        version=plugin.package_version
    )
    with _lock(dst_dir):
        if is_already_installed(dst_dir, plugin.id):
            ctx.logger.info(
                'Using existing installation of managed plugin: %s [%s]',
                plugin.id, _get_plugin_description(plugin))
            return

        ctx.logger.info(
            'Installing managed plugin: %s [%s]',
            plugin.id, _get_plugin_description(plugin))
        _make_virtualenv(_python_executable(plugin), dst_dir)
        try:
            _wagon_install(plugin, venv=dst_dir, args=args)
            with open(os.path.join(dst_dir, 'plugin.id'), 'w') as f:
                f.write(plugin.id)
        except Exception as e:
            shutil.rmtree(dst_dir, ignore_errors=True)
            tpe, value, tb = sys.exc_info()
            exc = NonRecoverableError(
                'Failed installing managed plugin: {0} [{1}][{2}]'
                .format(plugin.id, plugin.package_name, e))
            raise exc.with_traceback(tb)


def _wagon_install(plugin, venv, args):
    client = get_rest_client()
    wagon_dir = tempfile.mkdtemp(prefix='{0}-'.format(plugin.id))
    wagon_path = os.path.join(wagon_dir, 'wagon.tar.gz')
    try:
        ctx.logger.debug('Downloading plugin %s from manager into %s',
                         plugin.id, wagon_path)
        client.plugins.download(plugin_id=plugin.id,
                                output_file=wagon_path)
        ctx.logger.debug('Installing plugin %s using wagon', plugin.id)
        wagon.install(
            wagon_path,
            ignore_platform=True,
            install_args=args,
            venv=venv
        )
    finally:
        ctx.logger.debug('Removing directory: %s', wagon_dir)
        shutil.rmtree(wagon_dir, ignore_errors=True)


def _install_source_plugin(deployment_id, plugin, source, args):
    name = plugin.get('package_name') or plugin['name']
    dst_dir = target_plugin_prefix(
        name=name,
        tenant_name=ctx.tenant_name,
        version=plugin.get('package_version'),
        deployment_id=deployment_id
    )
    with _lock(dst_dir):
        if is_already_installed(dst_dir, 'source-{0}'.format(deployment_id)):
            ctx.logger.info(
                'Using existing installation of source plugin: %s', name)
            return

        ctx.logger.info('Installing plugin from source: %s', name)
        _make_virtualenv(_python_executable(plugin), dst_dir)
        try:
            _pip_install(source=source, venv=dst_dir, args=args)
        except Exception:
            shutil.rmtree(dst_dir, ignore_errors=True)
            raise
        with open(os.path.join(dst_dir, 'plugin.id'), 'w') as f:
            f.write('source-{0}'.format(deployment_id))


def _pip_install(source, venv, args):
    plugin_dir = None
    try:
        if os.path.isabs(source):
            plugin_dir = source
        else:
            ctx.logger.debug('Extracting archive: %s', source)
            plugin_dir = extract_package_to_dir(source)
        ctx.logger.debug('Installing from directory: %s [args=%ss]',
                         plugin_dir, args)
        command = [
            get_python_path(venv), '-m', 'pip',
            '--disable-pip-version-check', 'install'
        ] + args + [plugin_dir]
        runner.run(command=command, cwd=plugin_dir)
    except CommandExecutionException as e:
        ctx.logger.debug('Failed running pip install. Output:\n%s', e.output)
        raise PluginInstallationError(
            'Failed running pip install. ({0})'.format(e.error))
    finally:
        if plugin_dir and not os.path.isabs(source):
            ctx.logger.debug('Removing directory: %s', plugin_dir)
            shutil.rmtree(plugin_dir, ignore_errors=True)


@_manage_plugin_state(pre_state=None,
                      post_state=PluginInstallationState.UNINSTALLED,
                      allow_missing=True)
def uninstall(plugin, deployment_id=None):
    name = plugin.get('package_name') or plugin['name']
    dst_dir = target_plugin_prefix(
        name=name,
        tenant_name=ctx.tenant_name,
        version=plugin['package_version'],
        deployment_id=deployment_id
    )
    ctx.logger.info('uninstalling %s', dst_dir)
    if os.path.isdir(dst_dir):
        shutil.rmtree(dst_dir, ignore_errors=True)
    lock_file = '{0}.lock'.format(dst_dir)
    if os.path.exists(lock_file):
        try:
            os.remove(lock_file)
        except OSError as e:
            if e.errno != errno.ENOENT:
                raise

    parent_dir = os.path.dirname(dst_dir)
    try:
        if not os.listdir(parent_dir):
            shutil.rmtree(parent_dir, ignore_errors=True)
    except OSError as e:
        if e.errno != errno.ENOENT:
            raise


@contextmanager
def _lock(path):
    # lock with both a regular threading lock - for multithreaded access,
    # and fasteners lock for multiprocess access
    with PLUGIN_INSTALL_LOCK:
        with fasteners.InterProcessLock('{0}.lock'.format(path)):
            yield


def extract_package_to_dir(package_url):
    """
    Using a subprocess to extract a pip package to a temporary directory.
    :param: package_url: the URL of the package source.
    :return: the directory the package was extracted to.

    """
    plugin_dir = None
    archive_dir = tempfile.mkdtemp()
    runner = LocalCommandRunner()

    try:
        # We run `pip download` command in a subprocess to support
        # multi-threaded scenario (i.e snapshot restore).
        # We don't use `curl` because pip can handle different kinds of files,
        # including .git.
        command = [sys.executable, '-m', 'pip',
                   '--disable-pip-version-check', 'download', '-d',
                   archive_dir, '--no-deps', package_url]
        runner.run(command=command)
        archive = _get_archive(archive_dir, package_url)
        plugin_dir_parent = extract_archive(archive)
        plugin_dir = _get_plugin_path(plugin_dir_parent, package_url)

    except NonRecoverableError as e:
        if plugin_dir and os.path.exists(plugin_dir):
            shutil.rmtree(plugin_dir)
        raise e

    finally:
        if os.path.exists(archive_dir):
            shutil.rmtree(archive_dir)

    return plugin_dir


def _get_plugin_path(plugin_dir_parent, package_url):
    """
    plugin_dir_parent is a directory containing the plugin dir. Since we
    need the plugin`s dir, we find it's name and concatenate it to the
    plugin_dir_parent.
    """
    contents = list(walk(plugin_dir_parent))
    if len(contents) < 1:
        _remove_tempdir_and_raise_proper_exception(package_url,
                                                   plugin_dir_parent)
    parent_dir_content = contents[0]
    plugin_dir_name = parent_dir_content[1][0]
    return os.path.join(plugin_dir_parent, plugin_dir_name)


def _assert_list_len(lst, expected_len, package_url, archive_dir):
    if len(lst) != expected_len:
        _remove_tempdir_and_raise_proper_exception(package_url, archive_dir)


def _remove_tempdir_and_raise_proper_exception(package_url, tempdir):
    if tempdir and os.path.exists(tempdir):
        shutil.rmtree(tempdir)
    raise PluginInstallationError(
        'Failed to download package from {0}.'
        'You may consider uploading the plugin\'s Wagon archive '
        'to the manager, For more information please refer to '
        'the documentation.'.format(package_url))


def _get_archive(archive_dir, package_url):
    """
    archive_dir contains a zip file with the plugin directory. This function
    finds the name of that zip file and returns the full path to it (the full
    path is required in order to extract it)
    """
    contents = list(walk(archive_dir))
    _assert_list_len(contents, 1, package_url, archive_dir)
    files = contents[0][2]
    _assert_list_len(files, 1, package_url, archive_dir)
    return os.path.join(archive_dir, files[0])


def get_managed_plugin(plugin):
    package_name = plugin.get('package_name')
    package_version = plugin.get('package_version')
    if not package_name:
        return None
    query_parameters = {'package_name': package_name}
    if package_version:
        query_parameters['package_version'] = package_version
    client = get_rest_client()
    plugins = client.plugins.list(**query_parameters)
    if not plugins:
        return None
    return max(plugins, key=_managed_plugin_sort_key)


def _managed_plugin_sort_key(plugin):
    """Sort key for choosing a managed plugin to install

    The plugin that is the "first" according to this key, will be selected.
    First, put only "supported" (by OS) plugins in the front.
    Then, choose highest available plugin package versions.
    Then, select the highest possible python version among those.
    """
    return (
        _is_plugin_supported(plugin),
        parse_version(plugin.package_version),
        available_python_executables.version_for_plugin(plugin),
    )


def _is_plugin_supported(plugin):
    """Can plugin be installed on the current OS?

    This compares a plugin's .supported_platform field with the
    platform we're running on.
    """
    if not plugin.supported_platform:
        return False
    if plugin.supported_platform == 'any':
        return True

    if os.name == 'posix':
        # for linux,
        # 1) allow manylinux always,
        # 2) disallow if the arch is specified and different than current
        if plugin.supported_platform.startswith('manylinux'):
            return True
        if plugin.supported_platform.startswith('linux_'):
            if plugin.supported_platform.split('_', 1)[1] != \
                    platform.machine():
                return False
    # non-linux, or linux but the platform fits
    return plugin.supported_platform == wagon.get_platform()


def get_plugin_source(plugin, blueprint_id=None):

    source = plugin.get('source') or ''
    if not source:
        return None
    source = source.strip()

    # validate source url
    if '://' in source:
        split = source.split('://')
        schema = split[0]
        if schema not in ['http', 'https']:
            # invalid schema
            raise NonRecoverableError('Invalid schema: {0}'.format(schema))
    else:
        # Else, assume its a relative path from <blueprint_home>/plugins
        # to a directory containing the plugin archive.
        # in this case, the archived plugin is expected to reside on the
        # manager file server as a zip file.
        if blueprint_id is None:
            raise ValueError('blueprint_id must be specified when plugin '
                             'source does not contain a schema')

        plugin_zip = ctx.download_resource('plugins/{0}.zip'.format(source))
        source = path_to_file_url(plugin_zip)

    return source


def get_plugin_args(plugin):
    args = plugin.get('install_arguments') or ''
    return args.strip().split()


def path_to_file_url(path):
    """
    Convert a path to a file: URL.  The path will be made absolute and have
    quoted path parts.
    As taken from: https://github.com/pypa/pip/blob/9.0.1/pip/download.py#L459
    """
    path = os.path.normpath(os.path.abspath(path))
    url = urljoin('file:', pathname2url(path))
    return url


def _link_virtualenv(executable, venv):
    """Add current venv's libs to the target venv.

    Add a .pth file with a link to the current venv, to the target
    venv's site-packages.
    Also copy .pth files' contents from the current venv, so that the
    target venv also uses editable packages from the source venv.
    """
    own_site_packages = get_pth_dir(executable)
    target = get_pth_dir(get_python_path(venv))
    with open(os.path.join(target, 'agent.pth'), 'w') as agent_link:
        agent_link.write('# link to the agent virtualenv, created by '
                         'the plugin installer\n')
        agent_link.write('{0}\n'.format(own_site_packages))

        for filename in glob.glob(os.path.join(own_site_packages, '*.pth')):
            pth_path = os.path.join(own_site_packages, filename)
            with open(pth_path) as pth:
                agent_link.write('\n# copied from {0}:\n'.format(pth_path))
                agent_link.write(pth.read())
                agent_link.write('\n')


def get_pth_dir(executable):
    """Get the directory suitable for .pth files in this venv.

    This will return the site-packages directory, which is one of the
    targets that is scanned for .pth files.
    This is mostly a reimplementation of sysconfig.get_path('purelib'),
    but sysconfig is not available in 2.6.
    """
    output = runner.run([
        executable,
        '-c',
        'import json, sys; print(json.dumps([sys.prefix, sys.version_info]))'
    ]).std_out
    prefix, version_parts = json.loads(output)
    version = '{0}.{1}'.format(version_parts[0], version_parts[1])
    if os.name == 'nt':
        return '{0}/Lib/site-packages'.format(prefix)
    elif os.name == 'posix':
        return '{0}/lib/python{1}/site-packages'.format(prefix, version)
    else:
        raise NonRecoverableError('Unsupported OS: {0}'.format(os.name))


def _python_executable(plugin):
    version = available_python_executables.version_for_plugin(plugin)
    return available_python_executables.get_executable(version)


class _PythonExecutables(object):
    """Find python executables available on the local system.

    This examines PATH and decides what Python versions are available.
    """
    PYTHON_VERSION_RE = re.compile(
        'Python ([0-9]+)\\.([0-9]+)\\.([0-9]+)',
        re.IGNORECASE
    )

    def __init__(self):
        self._executables = None
        self._main_version = (sys.version_info.major, sys.version_info.minor)
        self._find_executables_lock = threading.Lock()

    def version_for_plugin(self, plugin):
        """Based on the plugin, select a Python version we can run it with.

        If the version requested by the plugin isn't available, default to
        the version of the executable we're currently running.
        Returns a tuple of 2 ints: (major, minor), e.g. (3, 11).
        """
        if not plugin.get('supported_py_versions'):
            # This is the case for source plugins
            return self._main_version

        available_versions = self._get_available_executables()
        required_versions = self._parse_supported_versions(
            plugin['supported_py_versions'])
        for plugin_version in sorted(required_versions, reverse=True):
            if plugin_version in available_versions:
                return plugin_version
        return self._main_version

    def get_executable(self, version):
        """Find a Python executable for the requested version.

        Given a Python version (as a tuple of 2 ints), return the path
        to a Python executable of that version.
        """
        return self._get_available_executables()[version]

    def _get_available_executables(self):
        if self._executables is not None:
            return self._executables
        with self._find_executables_lock:
            # check again, in case we were waiting on the lock, and another
            # thread ran this already
            if self._executables is not None:
                return self._executables
            self._executables = self._find_executables()
        return self._executables

    def _find_executables(self):
        executables = {
            self._main_version: sys.executable,
        }
        executable_filename = os.path.basename(sys.executable)
        # examine all directories in PATH for executables named the same
        # as the executable we're running, so either `python` or `Python.exe`
        for path in os.environ['PATH'].split(os.pathsep):
            if path and os.path.exists(path):
                path = os.path.abspath(path)
            else:
                continue
            target_executable = os.path.join(path, executable_filename)
            version = self._get_executable_version(target_executable)
            if not version or version in executables:
                continue
            executables[version] = target_executable
        return executables

    def _get_executable_version(self, target_executable):
        """Examine the executable, and return what Python version is it.

        To find the Python version of the given executable, run it with
        --version, and parse the resulting output.
        If the version cannot be determined, return None.
        """
        if not os.path.isfile(target_executable):
            return
        if not os.access(target_executable, os.X_OK):
            return

        try:
            version_string = subprocess.check_output(
                [target_executable, '--version']
            ).decode('utf-8', 'replace')
        except subprocess.CalledProcessError:
            return

        match = self.PYTHON_VERSION_RE.match(version_string)
        if match:
            major, minor, patch = match.groups()
            key = int(major), int(minor)
            return key

    def _parse_supported_versions(self, versions):
        """Parse the plugin supported_py_version field.

        The plugin field is a list of strings, like ["py310"].
        This parses it to a list of tuples of 2 ints, like [(3, 10)].
        Items that cannot be parsed will be omitted.
        """
        parsed_versions = []
        for version in versions:
            try:
                # handle inputs of the form:
                #  "py3.10" -> (3, 10)
                #  "3.10" -> (3, 10)
                #  "py310" -> (3, 10)
                # The last one (which is wagon's default) will however
                # break when we reach python 10 :) By then, we need to switch
                # to the dotted format.
                version = version.replace('py', '')
                if '.' in version:
                    parsed = tuple(int(x) for x in version.split('.'))
                else:
                    parsed = (int(version[0]), int(version[1:]))
                parsed_versions.append(parsed)
            except ValueError:
                pass
        return parsed_versions


available_python_executables = _PythonExecutables()
