from cloudify_rest_client import utils
from cloudify_rest_client.exceptions import CloudifyClientError
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
    def location(self):
        """
        :return: The location of the site : latitude,longitude.
        """
        return self.get('location')

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

    def create(self, name, location=None, visibility=VisibilityState.TENANT,
               created_by=None, created_at=None):
        """
        Create a new site.

        :param name: The name of the site.
        :param location: The location of the site : "latitude,longitude".
        :param visibility: The visibility of the site, can be 'private',
                           'tenant' or 'global'
        :param created_by: Override the creator. Internal use only.
        :param created_at: Override the creation timestamp. Internal use only.
        :return: The created site.
        """
        data = {'visibility': visibility}
        if location:
            data['location'] = location
        if created_by:
            data['created_by'] = created_by
        if created_at:
            data['created_at'] = created_at
        response = self.api.put(
            '/{self._uri_prefix}/{name}'.format(self=self, name=name),
            data=data
        )
        return self._wrapper_cls(response)

    def update(self, name, location=None, visibility=VisibilityState.TENANT,
               new_name=None):
        """
        Update an existing site.

        :param name: The name of the site
        :param location: The location of the site : "latitude,longitude".
        :param visibility: The new visibility of the site, can be 'private',
                           'tenant' or 'global'
        :param new_name: The new name of the site
        :return: The created site.
        """
        data = {
            'location': location,
            'visibility': visibility,
            'new_name': new_name
        }
        # Remove the keys with value None
        data = dict((k, v) for k, v in data.items() if v is not None)
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
        self.api.delete(
            '/{self._uri_prefix}/{name}'.format(self=self, name=name)
        )

    def dump(self, site_ids=None):
        """Generate sites' attributes for a snapshot.

        :param site_ids: A list of site identifiers, if not empty,
         used to select specific sites to be dumped.
        :returns: A generator of dictionaries, which describe sites'
         attributes.
        """
        entities = utils.get_all(
                self.api.get,
                f'/{self._uri_prefix}',
                params={'_get_data': True},
                _include=['name', 'location', 'visibility', 'created_by',
                          'created_at']
        )
        if not site_ids:
            return entities
        return (e for e in entities if e['name'] in site_ids)

    def restore(self, entities, logger):
        """Restore sites from a snapshot.

        :param entities: An iterable (e.g. a list) of dictionaries describing
         sites to be restored.
        :param logger: A logger instance.
        """
        for entity in entities:
            try:
                self.create(**entity)
            except CloudifyClientError as exc:
                logger.error("Error restoring site "
                             f"{entity['name']}: {exc}")
