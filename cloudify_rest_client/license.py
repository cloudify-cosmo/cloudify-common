########
# Copyright (c) 2019 Cloudify Platform Ltd. All rights reserved
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

from cloudify_rest_client import bytes_stream_utils
from cloudify_rest_client.responses import ListResponse


class License(dict):

    def __init__(self, license):
        super(License, self).__init__()
        self.update(license)

    @property
    def customer_id(self):
        """
        :returns: The customer ID this license belongs to.
        """
        return self.get('customer_id')

    @property
    def expiration_date(self):
        """
        :returns: The expiration date of this license.
        """
        return self.get('expiration_date')

    @property
    def license_edition(self):
        """
        :returns: The edition of the license (Spire/ Premium).
        """
        return self.get('license_edition')

    @property
    def trial(self):
        """
        :returns: Whether or not this is a trial vesion.
        """
        return self.get('license_edition')

    @property
    def cloudify_version(self):
        """
        :returns: The Cloudify Manager version this license provides access to.
        """
        return self.get('cloudify_version')

    @property
    def capabilities(self):
        """
        :returns: A list of capabilities this license enables.
        """
        return self.get('capabilities')

    @property
    def signature(self):
        """
        :returns: The signature that is used to verify the license was
        not tampered.
        """
        return self.get('signature')

    @property
    def expired(self):
        """
        :returns: Whether or not this Cloudify license has expired
        """
        return self.get('expired')


class LicenseClient(object):

    def __init__(self, api):
        self.api = api
        self._wrapper_cls = License

    def list(self):
        """Get the Cloudify license from the Manager.

        :rtype: License
        """
        response = self.api.get('/license')

        return ListResponse(
            [self._wrapper_cls(item) for item in response['items']],
            response['metadata']
        )

    def upload(self, license_path):
        """Uploads a Cloudify license the Manager
        :param license_path: Path to the Cloudfiy license file.
        :return: License
        """
        assert license_path

        data = bytes_stream_utils.request_data_file_stream(
            license_path,
            client=self.api)

        response = self.api.put(
            '/license',
            data=data
        )

        return response
