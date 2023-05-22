import warnings

from cloudify_rest_client import utils
from cloudify_rest_client.exceptions import CloudifyClientError
from cloudify_rest_client.responses import ListResponse


class PluginsUpdate(dict):

    def __init__(self, update):
        self.update(update)

    @property
    def id(self):
        return self['id']

    @property
    def state(self):
        return self['state']

    @property
    def blueprint_id(self):
        return self['blueprint_id']

    @property
    def temp_blueprint_id(self):
        return self['temp_blueprint_id']

    @property
    def execution_id(self):
        return self['execution_id']

    @property
    def deployments_to_update(self):
        return self['deployments_to_update']

    @property
    def deployments_per_tenant(self):
        return self['deployments_per_tenant']

    @property
    def created_at(self):
        return self['created_at']

    @property
    def forced(self):
        return self['forced']

    @property
    def tenant_name(self):
        return self['tenant_name']


class PluginsUpdateClient(object):
    """
    Cloudify's plugins update management client.
    """

    def __init__(self, api):
        self.api = api
        self._uri_prefix = 'plugins-updates'
        self._wrapper_cls = PluginsUpdate

    def get(self, plugins_update_id, _include=None, **kwargs):
        """
        Gets a plugins update by its id.

        :param plugins_update_id: PluginsUpdate's id to get.
        :param _include: List of fields to include in response.
        :return: The plugins update details.
        """
        assert plugins_update_id
        uri = '/{self._uri_prefix}/{id}'.format(
            self=self, id=plugins_update_id)
        response = self.api.get(uri, _include=_include, params=kwargs)
        return self._wrapper_cls(response)

    def _wrap_list(self, response):
        return ListResponse(
            [self._wrapper_cls(item) for item in response['items']],
            response['metadata']
        )

    def list(self, _include=None, sort=None, is_descending=False, **kwargs):
        """
        Returns a list of available plugins updates.

        :param _include: List of fields to include in response.
        :param sort: Key for sorting the list.
        :param is_descending: True for descending order, False for ascending.
        :param kwargs: Optional filter fields. For a list of available fields
               see the REST service's models.PluginsUpdate.fields
        :return: Plugins list.
        """
        params = kwargs
        if sort:
            params['_sort'] = '-' + sort if is_descending else sort

        response = self.api.get('/{self._uri_prefix}'.format(self=self),
                                _include=_include,
                                params=params)
        return self._wrap_list(response)

    def inject(self, blueprint_id, force=False,
               created_by=None, created_at=None,
               execution_id=None, state=None,
               update_id=None, affected_deployments=None,
               deployments_per_tenant=None, all_tenants=None,
               temp_blueprint_id=None):
        return PluginsUpdate(self.api.post(
            '/{self._uri_prefix}/{}/update/initiate'.format(blueprint_id,
                                                            self=self),
            data=_data_from_kwargs(
                force=force,
                created_by=created_by,
                created_at=created_at,
                execution_id=execution_id,
                state=state,
                update_id=update_id,
                all_tenants=all_tenants,
                affected_deployments=affected_deployments,
                deployments_per_tenant=deployments_per_tenant,
                temp_blueprint_id=temp_blueprint_id,
            ),
        ))

    def update_plugins(self, blueprint_id, force=False, plugin_names=None,
                       to_latest=None, all_to_latest=True,
                       to_minor=None, all_to_minor=False,
                       mapping=None, auto_correct_types=False,
                       reevaluate_active_statuses=False,
                       all_tenants=False):
        """
        Updates the plugins in all the deployments that use the given
        blueprint.

        :param blueprint_id: blueprint ID to perform the update with.
        :param force: if to forcefully update when other non-active plugins
         updates exists associated with this blueprint.
        :param plugin_names: list of plugin names to update (only those).
        :param to_latest: list of plugin names to be upgraded to the latest
         installed version.
        :param all_to_latest: update all (selected) plugins to the latest
         installed version of a plugin.
        :param to_minor: list of plugin names to be upgraded to the latest
         installed minor version (i.e. major versions of the plugin in use and
         the upgraded one will match)
        :param all_to_minor: update all (selected) plugins to the latest
         installed minor version
        :param mapping: detailed information on required plugin update
         (overrides all other arguments/settings concerning version
         constraints)
        :param auto_correct_types: auto_correct_types flag to run deployments
         update with
        :param reevaluate_active_statuses: reevaluate active plugin-updates'
         and deployment-updates' states based on relevant executions statuses.
        :param all_tenants: defines if plugin update process should update any
         deployments based on blueprint_id (owned by any tenant)
        :return: a PluginUpdate object.
        """
        if mapping and mapping.get('updates'):
            warnings.warn("The 'mapping file' was used during the update; "
                          "remember to update your blueprint files",
                          RuntimeWarning)
        else:
            mapping = {}
        response = self.api.post(
            '/{self._uri_prefix}/{}/update/initiate'.format(blueprint_id,
                                                            self=self),
            data=_data_from_kwargs(
                plugin_names=plugin_names,
                to_latest=to_latest,
                all_to_latest=all_to_latest,
                to_minor=to_minor,
                all_to_minor=all_to_minor,
                mapping=mapping,
                force=force,
                auto_correct_types=auto_correct_types,
                reevaluate_active_statuses=reevaluate_active_statuses,
                all_tenants=all_tenants
            )
        )
        return PluginsUpdate(response)

    def finalize_plugins_update(self, plugins_update_id):
        """
        Finalize the plugins update (for internal use).

        :return: a PluginUpdate object.
        """
        response = self.api.post(
            '/{self._uri_prefix}/{}/update/finalize'.format(plugins_update_id,
                                                            self=self)
        )
        return PluginsUpdate(response)

    def dump(self, plugins_update_ids=None):
        """Generate plugins updates' attributes for a snapshot.

        :param plugins_update_ids: A list of plugins updates' identifiers,
         if not empty, used to select specific plugins updates to be dumped.
        :returns: A generator of dictionaries, which describe plugins
         updates' attributes.
        """
        entities = utils.get_all(
                self.api.get,
                f'/{self._uri_prefix}',
                params={'_get_data': True},
                _include=['id', 'state', 'forced', 'all_tenants',
                          'blueprint_id', 'execution_id', 'created_by',
                          'created_at', 'deployments_to_update',
                          'deployments_per_tenant', 'temp_blueprint_id'],
        )
        if not plugins_update_ids:
            return entities
        return (e for e in entities if e['id'] in plugins_update_ids)

    def restore(self, entities, logger):
        """Restore plugins updates from a snapshot.

        :param entities: An iterable (e.g. a list) of dictionaries describing
         plugins updates to be restored.
        :param logger: A logger instance.
        """
        for entity in entities:
            entity['update_id'] = entity.pop('id')
            entity['affected_deployments'] = entity.pop(
                    'deployments_to_update', None)
            entity['force'] = entity.pop('forced', None)
            try:
                self.inject(**entity)
            except CloudifyClientError as exc:
                logger.error("Error restoring plugins update "
                             f"{entity['update_id']}: {exc}")


def _data_from_kwargs(**kwargs):
    data = {}
    for k, v in kwargs.items():
        if v:
            data[k] = v
    return data or None
