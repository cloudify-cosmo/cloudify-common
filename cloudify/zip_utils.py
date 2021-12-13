import os
import zipfile


def make_zip64_archive(zip_filename, directory):
    """Create zip64 archive that contains all files in a directory.
    zip64 is a set of extensions on top of the zip file format that allows to
    have files larger than 2GB. This is important in snapshots where the amount
    of data to backup might be huge.
    Note that `shutil` provides a method to register new formats based on the
    extension of the target file, since a `.zip` extension is still desired,
    using such an extension mechanism is not an option to avoid patching the
    already registered zip format.
    In any case, this function is heavily inspired in stdlib's
    `shutil._make_zipfile`.
    :param zip_filename: Path to the zip file to be created
    :type zip_filename: str
    :path directory: Path to directory where all files to compress are located
    :type directory: str
    """
    zip_context_manager = zipfile.ZipFile(
        zip_filename,
        'w',
        compression=zipfile.ZIP_DEFLATED,
        allowZip64=True,
    )

    with zip_context_manager as zip_file:
        path = os.path.normpath(directory)
        base_dir = path
        for dirpath, dirnames, filenames in os.walk(directory):
            for dirname in sorted(dirnames):
                path = os.path.normpath(os.path.join(dirpath, dirname))
                zip_file.write(path, os.path.relpath(path, base_dir))
            for filename in filenames:
                path = os.path.normpath(os.path.join(dirpath, filename))
                # Avoid trying to back up special files
                # (as the stdlib implementation does)
                if os.path.isfile(path):
                    zip_file.write(path, os.path.relpath(path, base_dir))
