from cloudify.deployment_dependencies import (build_deployment_dependency,
                                              DEPENDENCY_CREATOR)

from cloudify_rest_client import utils
from cloudify_rest_client.exceptions import CloudifyClientError
from cloudify_rest_client.responses import ListResponse


class InterDeploymentDependency(dict):

    def __init__(self, dependency):
        self.update(dependency)

    @property
    def id(self):
        return self['id']

    @property
    def target_deployment_func(self):
        return self['target_deployment_func']

    @property
    def dependency_creator(self):
        return self[DEPENDENCY_CREATOR]

    @property
    def source_deployment_id(self):
        return self['source_deployment_id']

    @property
    def target_deployment_id(self):
        return self['target_deployment_id']

    @property
    def created_at(self):
        return self['created_at']


class InterDeploymentDependencyClient(object):
    """
    Cloudify's plugins update management client.
    """

    def __init__(self, api):
        self.api = api
        self._uri_prefix = 'deployments/inter-deployment-dependencies'
        self._wrapper_cls = InterDeploymentDependency

    def _wrap_list(self, response):
        return ListResponse(
            [self._wrapper_cls(item) for item in response['items']],
            response['metadata']
        )

    def create(self,
               dependency_creator,
               source_deployment,
               target_deployment=None,
               external_source=None,
               external_target=None,
               target_deployment_func=None,
               _id=None,
               _visibility=None,
               _created_at=None,
               _created_by=None,
               ):
        """Creates an inter-deployment dependency.

        :param dependency_creator: a string representing the entity that
         is responsible for this dependency (e.g. an intrinsic function
         blueprint path, 'node_instances.some_node_instance', etc.).
        :param source_deployment: source deployment that depends on the target
         deployment.
        :param target_deployment: the deployment that the source deployment
         depends on.
        :param external_source: if the source deployment uses an external
        resource as target, pass here a JSON containing the source deployment
        metadata, i.e. deployment name, tenant name, and the manager host(s).
        :param external_target: if the source deployment uses an external
        resource as target, pass here a JSON containing the target deployment
        metadata, i.e. deployment name, tenant name, and the manager host(s).
        :param target_deployment_func: a function used to determine the target
        deployment.
        :param _id: Override the identifier. Internal use only.
        :param _visibility: Override the visibility. Internal use only.
        :param _created_at: Override the creation timestamp. Internal use only.
        :param _created_by: Override the creator. Internal use only.
        :return: an InterDeploymentDependency object.
        """
        data = build_deployment_dependency(
            dependency_creator,
            source_deployment=source_deployment,
            target_deployment=target_deployment,
            target_deployment_func=target_deployment_func,
            external_source=external_source,
            external_target=external_target,
            id=_id,
            visibility=_visibility,
            created_at=_created_at,
            created_by=_created_by,
        )
        response = self.api.put(
            '/{self._uri_prefix}'.format(self=self), data=data)
        return self._wrapper_cls(response)

    def create_many(self, source_deployment_id, inter_deployment_dependencies):
        """Creates a number of inter-deployment dependencies.

        :param source_deployment_id: ID of the source deployment (the one which
         depends on the target deployment).
        :param inter_deployment_dependencies: a list of inter-deployment
         dependencies descriptions, but without a source_deployment(_id).
        :return: a list of created InterDeploymentDependencies IDs.
        """
        response = self.api.post(
            '/{self._uri_prefix}'.format(self=self), data={
                'source_deployment_id': source_deployment_id,
                'inter_deployment_dependencies': inter_deployment_dependencies}
        )
        return self._wrap_list(response)

    def update_all(self, source_deployment_id, inter_deployment_dependencies):
        """Update (i.e. rewrite all) inter-deployment dependencies for
        a given deployment.

        :param source_deployment_id: ID of the source deployment (the one which
         depends on the target deployment).
        :param inter_deployment_dependencies: a list of inter-deployment
         dependencies descriptions, but without a source_deployment(_id).
        :return: a list of created InterDeploymentDependencies IDs.
        """
        response = self.api.put(
            '/deployments/{0}/inter-deployment-dependencies'.format(
                source_deployment_id),
            data={
                'inter_deployment_dependencies': inter_deployment_dependencies,
            },
        )
        return self._wrap_list(response)

    def delete(self, dependency_creator, source_deployment,
               target_deployment=None,
               is_component_deletion=False, external_source=None,
               external_target=None):
        """Deletes an inter-deployment dependency.

        :param dependency_creator: a string representing the entity that
         is responsible for this dependency (e.g. an intrinsic function
         blueprint path, 'node_instances.some_node_instance', etc.).
        :param source_deployment: source deployment that depends on the target
         deployment.
        :param target_deployment: the deployment that the source deployment
         depends on.
        :param is_component_deletion: a special flag for allowing the
         deletion of a Component inter-deployment dependency when the target
         deployment is already deleted.
        :param external_source: if the source deployment uses an external
        resource as target, pass here a JSON containing the source deployment
        metadata, i.e. deployment name, tenant name, and the manager host(s).
        :param external_target: if the source deployment uses an external
        resource as target, pass here a JSON containing the target deployment
        metadata, i.e. deployment name, tenant name, and the manager host(s).
        :return: an InterDeploymentDependency object.
        """
        data = build_deployment_dependency(
            dependency_creator,
            source_deployment=source_deployment,
            target_deployment=target_deployment,
            external_source=external_source,
            external_target=external_target,
        )
        data['is_component_deletion'] = is_component_deletion
        self.api.delete('/{self._uri_prefix}'.format(self=self), data=data)

    def list(self, _include=None, sort=None, is_descending=False, **kwargs):
        """
        Returns a list of available  inter-deployment dependencies.

        :param _include: List of fields to include in response.
        :param sort: Key for sorting the list.
        :param is_descending: True for descending order, False for ascending.
        :param kwargs: Optional filter fields. For a list of available fields
               see the REST service's models.InterDeploymentDependencies.fields
        :return: InterDeploymentDependencies list.
        """
        params = kwargs
        if sort:
            params['_sort'] = '-' + sort if is_descending else sort

        response = self.api.get('/{self._uri_prefix}'.format(self=self),
                                _include=_include,
                                params=params)
        return self._wrap_list(response)

    def legacy_restore(self, deployment_id, update_service_composition):
        """
        Updating the inter deployment dependencies table from the specified
        deployment during an upgrade

       """
        data = {
            'deployment_id': deployment_id,
            'update_service_composition': update_service_composition,
        }
        self.api.post('/{self._uri_prefix}/restore'.format(self=self),
                      data=data)

    def dump(self, inter_deployment_dependency_ids=None):
        """Generate inter-deployment dependencies' attributes for a snapshot.

        :param inter_deployment_dependency_ids: A list of inter-deployment
         dependencies' identifiers, if not empty, used to select specific
         inter-deployment dependencies to be dumped.
        :returns: A generator of dictionaries, which describe inter-deployment
         dependencies' attributes.
        """
        entities = utils.get_all(
                self.api.get,
                f'/{self._uri_prefix}',
                _include=['id', 'visibility', 'created_at', 'created_by',
                          'dependency_creator', 'target_deployment_func',
                          'source_deployment_id', 'target_deployment_id',
                          'external_source', 'external_target'],
        )
        if not inter_deployment_dependency_ids:
            return entities
        return (e for e in entities
                if e['id'] in inter_deployment_dependency_ids)

    def restore(self, entities, logger):
        """Restore inter-deployment dependencies from a snapshot.

        :param entities: An iterable (e.g. a list) of dictionaries describing
         inter-deployment dependencies to be restored.
        :param logger: A logger instance.
        """
        for entity in entities:
            entity['_id'] = entity.pop('id')
            entity['_visibility'] = entity.pop('visibility')
            entity['_created_at'] = entity.pop('created_at')
            entity['_created_by'] = entity.pop('created_by')
            entity['source_deployment'] = \
                entity.pop('source_deployment_id')
            entity['target_deployment'] = \
                entity.pop('target_deployment_id')
            try:
                self.create(**entity)
            except CloudifyClientError as exc:
                logger.error("Error restoring inter-deployment dependency "
                             f"{entity['_id']}: {exc}")
