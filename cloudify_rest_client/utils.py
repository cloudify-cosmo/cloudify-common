import os
import sys
import stat
import tarfile
from os.path import expanduser

SUPPORTED_ARCHIVE_TYPES = ['zip', 'tar', 'tar.gz', 'tar.bz2']


def tar_blueprint(blueprint_path, dest_dir):
    """
    creates a tar archive out of a blueprint dir.

    :param blueprint_path: the path to the blueprint.
    :param dest_dir: destination dir for the path
    :return: the path for the dir.
    """
    blueprint_path = expanduser(blueprint_path)
    app_name = os.path.basename(os.path.splitext(blueprint_path)[0])
    blueprint_directory = os.path.dirname(blueprint_path) or os.getcwd()
    return tar_file(blueprint_directory, dest_dir, app_name)


def tar_file(file_to_tar, destination_dir, tar_name=''):
    """
    tar a file into a destination dir.
    :param file_to_tar:
    :param destination_dir:
    :param tar_name: optional tar name.
    :return:
    """
    def _reset_tarinfo(tarinfo):
        """Set all tar'd files to be world-readable, so that other services
        (nginx) can access the files uploaded by restservice (cfyuser).
        """
        tarinfo.mode = tarinfo.mode | stat.S_IROTH
        if stat.S_ISDIR(tarinfo.mode):
            # directories must also have u+w so that files can be stored in
            # them, and have the execute bit set so that permissions can be
            # exercised for them
            tarinfo.mode = (tarinfo.mode | stat.S_IWUSR |
                            stat.S_IXUSR | stat.S_IXOTH)

        return tarinfo

    tar_name = tar_name or os.path.basename(file_to_tar)
    tar_path = os.path.join(destination_dir, '{0}.tar.gz'.format(tar_name))
    with tarfile.open(tar_path, "w:gz", dereference=True) as tar:
        tar.add(file_to_tar, arcname=tar_name, filter=_reset_tarinfo)
    return tar_path


def is_supported_archive_type(blueprint_path):

    extensions = ['.{0}'.format(ext) for ext in SUPPORTED_ARCHIVE_TYPES]
    return blueprint_path.endswith(tuple(extensions))


def is_kerberos_env():
    if os.path.exists('/etc/krb5.conf') and find_executable('klist'):
        return True
    return False


def get_folder_size_and_files(path):
    size = os.path.getsize(path)
    files = len(os.listdir(path))
    for entry in os.listdir(path):
        entry_path = os.path.join(path, entry)
        if os.path.isfile(entry_path) and not os.path.islink(entry_path):
            size += os.path.getsize(entry_path)
        elif os.path.isdir(entry_path):
            sub_size, sub_files = get_folder_size_and_files(entry_path)
            size += sub_size
            files += sub_files
    return size, files


def get_file_content(file_path):
    expanded_file_path = os.path.expanduser(file_path)
    if os.path.exists(expanded_file_path):
        with open(expanded_file_path) as fd:
            return fd.read()
    return None


# Copied verbatim from distutils.spawn, since we don't always ship distutils.
def find_executable(executable, path=None):
    """Tries to find 'executable' in the directories listed in 'path'.

    A string listing directories separated by 'os.pathsep'; defaults to
    os.environ['PATH'].  Returns the complete filename or None if not found.
    """
    if path:
        paths = path.split(os.pathsep)
    else:
        paths = os.get_exec_path()
    base, ext = os.path.splitext(executable)
    if (sys.platform == 'win32') and (ext != '.exe'):
        executable = executable + '.exe'
    if not os.path.isfile(executable):
        for p in paths:
            f = os.path.join(p, executable)
            if os.path.isfile(f):
                # the file exists, we have a shot at spawn working
                return f
        return None
    else:
        return executable


def get_all(method, *args, **kwargs):
    """Generator of entities retrieved by a method called with args/kwargs."""
    include = kwargs.get('_include')
    params = kwargs.get('params', {})
    if params.get('_include_hash'):
        include.append('password_hash')
    more_data = True
    entities_yielded, pagination_offset = 0, 0
    while more_data:
        params['_offset'] = pagination_offset
        kwargs['params'] = params
        result = method(*args, **kwargs)
        for item in result['items']:
            yield {k: v for k, v in item.items()
                   if include is None or k in include}
            entities_yielded += 1
        more_data = \
            (entities_yielded < result['metadata']['pagination']['total'])
        pagination_offset = entities_yielded


class StreamedResponse(object):

    def __init__(self, response):
        self._response = response

    @property
    def headers(self):
        return self._response.headers

    def bytes_stream(self, chunk_size=8192):
        return self._response.iter_content(chunk_size)

    def lines_stream(self):
        return self._response.iter_lines()

    def close(self):
        self._response.close()
