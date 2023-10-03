import cgi
import errno
import os

CONTENT_DISPOSITION_HEADER = 'content-disposition'
DEFAULT_BUFFER_SIZE = 8192


def request_data_file_stream(file_path,
                             buffer_size=DEFAULT_BUFFER_SIZE,
                             progress_callback=None,
                             client=None):
    """
    Read file data in kerberos mode or split into chunks,
    :param file_path: Local path of the file to be transferred
    :param buffer_size: Size of the buffer
    :param progress_callback: Callback function - can be used to print progress
    :return: File data or generator object
    """
    if client and client.has_kerberos() and not client.has_auth_header():
        # kerberos currently does not support chunks
        with open(file_path, 'rb') as f:
            data = f.read()
        if progress_callback:
            progress_callback(1, 1)
    else:
        # upload it in chunks
        data = request_data_file_stream_gen(
            file_path,
            buffer_size=buffer_size,
            progress_callback=progress_callback)
    return data


def request_data_file_stream_gen(file_path,
                                 buffer_size=DEFAULT_BUFFER_SIZE,
                                 progress_callback=None):
    """
    Split a file into buffer-sized chunks,
    :param file_path: Local path of the file to be transferred
    :param buffer_size: Size of the buffer
    :param progress_callback: Callback function - can be used to print progress
    :return: Generator object
    """
    total_bytes_read = 0
    total_file_size = os.path.getsize(file_path)

    with open(file_path, 'rb') as f:
        while True:
            read_bytes = f.read(buffer_size)
            read_bytes_len = len(read_bytes)

            if progress_callback:
                total_bytes_read += read_bytes_len
                progress_callback(total_bytes_read, total_file_size)

            yield read_bytes
            if read_bytes_len < buffer_size:
                return


def write_response_stream_to_file(streamed_response,
                                  output_file=None,
                                  buffer_size=DEFAULT_BUFFER_SIZE,
                                  progress_callback=None,):
    """
    Read buffer-sized chunks from a stream, and write them to file
    :param streamed_response: The binary stream
    :param output_file: Name of the output file
    :param buffer_size: Size of the buffer
    :param progress_callback: Callback function - can be used to print progress
    :return:
    """
    header_value = streamed_response.headers.get(CONTENT_DISPOSITION_HEADER)
    output_filename = None
    if header_value:
        _, content_params = cgi.parse_header(header_value)
        output_filename = content_params.get('filename')

    if not output_file:
        if not output_filename:
            raise RuntimeError(
                'Cannot determine attachment filename: {0} header not'
                ' found in response headers'.format(
                    CONTENT_DISPOSITION_HEADER))
        output_file = output_filename

    if os.path.isdir(output_file):
        output_file = os.path.join(output_file, output_filename)

    if os.path.exists(output_file):
        raise OSError(
            errno.EEXIST,
            "Output file '{0}' already exists".format(output_file),
            output_filename,
        )

    total_file_size = int(streamed_response.headers['content-length'])
    total_bytes_written = 0

    with open(output_file, 'wb') as f:
        for chunk in streamed_response.bytes_stream(buffer_size):
            if chunk:
                f.write(chunk)
                f.flush()

            if progress_callback:
                total_bytes_written += len(chunk)
                progress_callback(total_bytes_written, total_file_size)
    return output_file
