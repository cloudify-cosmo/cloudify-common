#########
# Copyright (c) 2015 GigaSpaces Technologies Ltd. All rights reserved
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

import abc
import time

import requests

from dsl_parser import exceptions
from cloudify.manager import get_resource_from_manager

DEFAULT_RETRY_DELAY = 1
MAX_NUMBER_RETRIES = 5
DEFAULT_REQUEST_TIMEOUT = 10


def is_remote_resource(resource_url):
    url_parts = resource_url.split(':')
    if url_parts[0] in ['http', 'https', 'file', 'ftp', 'plugin', 'blueprint']:
        return True


class AbstractImportResolver(abc.ABC):
    """
    This class is abstract and should be inherited by concrete
    implementations of import resolver.
    The only mandatory implementation is of resolve, which is expected
    to open the import url and return its data.
    """

    @abc.abstractmethod
    def resolve(self, import_url):
        raise NotImplementedError

    def fetch_import(self, import_url, **kwargs):
        if is_remote_resource(import_url):
            return self.resolve(import_url)
        return read_import(import_url)

    @abc.abstractmethod
    def retrieve_plugin(self, import_url, **kwargs):
        raise NotImplementedError


def read_import(import_url):
    error_str = 'Import failed: Unable to open import url'
    if import_url.startswith('file:'):
        try:
            filename = import_url[len('file:'):]
            with open(filename) as f:
                return f.read()
        except Exception as ex:
            raise exceptions.DSLParsingLogicException(
                13, f"{error_str} {import_url}; {ex}")
    elif import_url.startswith('resource:'):
        try:
            filename = import_url[len('resource:'):]
            return get_resource_from_manager(filename)
        except Exception as ex:
            raise exceptions.DSLParsingLogicException(
                13, f"{error_str} {import_url}; {ex}")
    else:
        number_of_attempts = MAX_NUMBER_RETRIES + 1

        # Defines on which errors we should retry the import.
        def _is_recoverable_error(e):
            return isinstance(e, (requests.ConnectionError, requests.Timeout))

        # Defines on which return values we should retry the import.
        def _is_internal_error(result):
            return hasattr(result, 'status_code') and result.status_code >= 500

        def get_import():
            response = requests.get(import_url,
                                    timeout=DEFAULT_REQUEST_TIMEOUT)
            # The response is a valid one, and the content should be returned
            if 200 <= response.status_code < 300:
                return response.text
            # If the response status code is above 500, an internal server
            # error has occurred. The return value would be caught by
            # _is_internal_error (as specified in the decorator), and retried.
            elif response.status_code >= 500:
                return response
            # Any other response should raise an exception.
            else:
                invalid_url_err = exceptions.DSLParsingLogicException(
                    13, '{0} {1}; status code: {2}'.format(
                        error_str, import_url, response.status_code))
                raise invalid_url_err

        def get_import_wrapper():
            exception_caught = None
            for attempt in range(number_of_attempts):
                try:
                    result = get_import()
                    if not _is_internal_error(result):
                        return result
                except Exception as ex:
                    if not _is_recoverable_error(ex):
                        raise ex
                    else:
                        exception_caught = ex
                time.sleep(DEFAULT_RETRY_DELAY)
            if exception_caught:
                raise exception_caught

        try:
            import_result = get_import_wrapper()
            # If the error is an internal error only. A custom exception should
            # be raised.
            if _is_internal_error(import_result):
                raise exceptions.DSLParsingLogicException(
                    13,
                    f"Import failed {number_of_attempts} times, "
                    f"due to internal server error; {import_result.text}"
                )
            return import_result
        # If any ConnectionError, Timeout or URLRequired should rise
        # after the retrying mechanism, a custom exception will be raised.
        except (requests.ConnectionError, requests.Timeout,
                requests.URLRequired) as ex:
            raise exceptions.DSLParsingLogicException(
                13, f"{error_str} {import_url}; {ex}")
