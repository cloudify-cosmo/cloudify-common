import os
import stat
import tarfile
from os.path import expanduser
from distutils.spawn import find_executable

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
