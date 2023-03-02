########
# Copyright (c) 2020 Cloudify Platform Ltd. All rights reserved
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

DEPENDENCY_CREATOR = 'dependency_creator'
SOURCE_DEPLOYMENT = 'source_deployment'
TARGET_DEPLOYMENT = 'target_deployment'
TARGET_DEPLOYMENT_FUNC = 'target_deployment_func'
EXTERNAL_SOURCE = 'external_source'
EXTERNAL_TARGET = 'external_target'

VALID_ATTRIBUTES = {DEPENDENCY_CREATOR, SOURCE_DEPLOYMENT, TARGET_DEPLOYMENT,
                    TARGET_DEPLOYMENT_FUNC, EXTERNAL_SOURCE, EXTERNAL_TARGET,
                    'id', 'visibility', 'created_at', 'created_by'}


def build_deployment_dependency(dependency_creator, **kwargs) -> dict:
    """Build dict representation of the inter-deployment dependency.

    :param dependency_creator: A string representing the entity that is
                               responsible for this dependency (e.g. an
                               intrinsic function, blueprint path,
                               'node_instances.some_node_instance', etc.).
    :param source_deployment: Source deployment that depends on the target
                              deployment.
    :param target_deployment: The deployment that the source deployment
                              depends on.
    :param target_deployment_func: A function used to determine the target
                                   deployment.
    :param external_source: If the source deployment uses an external resource
                            as target, pass here a JSON containing the source
                            deployment metadata, i.e. deployment name, tenant
                            name, and the manager host(s).
    :param external_target: If the source deployment uses an external resource
                            as target, pass here a JSON containing the target
                            deployment metadata, i.e. deployment name, tenant
                            name, and the manager host(s).
    :param id: Override the identifier. Internal use only.
    :param visibility: Override the visibility. Internal use only.
    :param created_at: Override the creation timestamp. Internal use only.
    :param created_by: Override the creator. Internal use only.
    """
    invalid = kwargs.keys() - VALID_ATTRIBUTES
    if invalid:
        raise RuntimeError("Not valid inter-deployment dependency "
                           f"attribute(s): {', '.join(invalid)}")
    dependency = {
        DEPENDENCY_CREATOR: dependency_creator,
    }
    for key, value in kwargs.items():
        if value is not None:
            dependency[key] = value
    return dependency


def format_dependency_creator(connection_type, to_deployment) -> str:
    """Format a dependency_creator string, which represents the entity
     responsible for the inter-deployment dependency."""
    return f'{connection_type}.{to_deployment}'
