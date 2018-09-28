import os
import stat
import tarfile
from os.path import expanduser
from distutils.spawn import find_executable

from opentracing_instrumentation.request_context import get_current_span

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
    tar a file into a desintation dir.
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
    with tarfile.open(tar_path, "w:gz") as tar:
        tar.add(file_to_tar, arcname=tar_name, filter=_reset_tarinfo)
    return tar_path


def is_supported_archive_type(blueprint_path):

    extensions = ['.{0}'.format(ext) for ext in SUPPORTED_ARCHIVE_TYPES]
    return blueprint_path.endswith(tuple(extensions))


def is_kerberos_env():
    if os.path.exists('/etc/krb5.conf') and find_executable('klist'):
        return True
    return False


def with_tracing(func_name=None):
    """Wrapper function for Flask operation call tracing.
    This decorator must be activate iff the following condition apply. One of
    the parent calls must be done inside a 'with span_in_context(span)' scope
    (otherwise you'd get a parentless span).

    :param name: name to display in tracing
    """

    def decorator(f):
        name = func_name
        if func_name is None:
            name = f.__name__

        @wraps(f)
        def with_tracing_wrapper(*args, **kwargs):
            root_span = get_current_span()
            with current_app.tracer.start_span(
                    name,
                    child_of=root_span) as span:
                with span_in_context(span):
                    return f(*args, **kwargs)

        return with_tracing_wrapper

    return decorator
