########
# Copyright (c) 2013-2019 Cloudify Technologies Ltd. All rights reserved
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

from cloudify_rest_client.responses import ListResponse
from cloudify_rest_client.constants import VisibilityState


class Site(dict):

    def __init__(self, site):
        super(Site, self).__init__()
        self.update(site)

    @property
    def name(self):
        """
        :return: The name of the site.
        """
        return self.get('name')

    @property
    def latitude(self):
        """
        :return: The latitude of the sites location.
        """
        return self.get('latitude')

    @property
    def longitude(self):
        """
        :return: The longitude of the sites location.
        """
        return self.get('longitude')

    @property
    def created_at(self):
        """
        :return: Site creation date.
        """
        return self.get('created_at')

    @property
    def visibility(self):
        """
        :return: Site visibility.
        """
        return self.get('visibility')


class SitesClient(object):

    def __init__(self, api):
        self.api = api
        self._uri_prefix = 'sites'
        self._wrapper_cls = Site

    def create(self, name, latitude=None, longitude=None,
               visibility=VisibilityState.TENANT):
        """
        Create a new site.

        :param name: The name of the site
        :param latitude: The latitude of the sites location
        :param longitude: The longitude of the sites location
        :param visibility: The visibility of the site, can be 'private',
                           'tenant' or 'global'
        :return: The created site.
        """
        data = {
            'latitude': latitude,
            'longitude': longitude,
            'visibility': visibility
        }
        # Remove the keys with value None
        data = dict((k, v) for k, v in data.iteritems() if v is not None)
        response = self.api.put(
            '/{self._uri_prefix}/{name}'.format(self=self, name=name),
            data=data
        )
        return self._wrapper_cls(response)

    def update(self, name, latitude=None, longitude=None,
               visibility=VisibilityState.TENANT, new_name=None):
        """
        Update an existing site.

        :param name: The name of the site
        :param latitude: The new latitude of the sites location
        :param longitude: The new longitude of the sites location
        :param visibility: The new visibility of the site, can be 'private',
                           'tenant' or 'global'
        :param new_name: The new name of the site
        :return: The created site.
        """
        data = {
            'latitude': latitude,
            'longitude': longitude,
            'visibility': visibility,
            'new_name': new_name
        }
        # Remove the keys with value None
        data = dict((k, v) for k, v in data.iteritems() if v is not None)
        response = self.api.post(
            '/{self._uri_prefix}/{name}'.format(self=self, name=name),
            data=data
        )
        return self._wrapper_cls(response)

    def get(self, name):
        """
        Get a site from the manager.

        :param name: The name of the site
        :return: The details of the site
        """
        response = self.api.get(
            '/{self._uri_prefix}/{name}'.format(self=self, name=name)
        )
        return self._wrapper_cls(response)

    def list(self, _include=None, sort=None, is_descending=False, **kwargs):
        """
        Returns a list of currently stored sites.

        :param _include: List of fields to include in response.
        :param sort: Key for sorting the list.
        :param is_descending: True for descending order, False for ascending.
        :param kwargs: Optional filter fields. For a list of available fields
               see the REST service's models.Site.fields
        :return: Sites list.
        """

        if sort:
            kwargs['_sort'] = '-' + sort if is_descending else sort

        response = self.api.get('/{self._uri_prefix}'.format(self=self),
                                _include=_include,
                                params=kwargs)
        return ListResponse(
            [self._wrapper_cls(item) for item in response['items']],
            response['metadata']
        )

    def delete(self, name):
        """
        Deletes a site.

        :param name: The name of the site to be deleted.
        :return: Deleted site.
        """
        response = self.api.delete(
            '/{self._uri_prefix}/{name}'.format(self=self, name=name)
        )
        return self._wrapper_cls(response)
