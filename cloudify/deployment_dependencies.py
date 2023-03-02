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


def build_deployment_dependency(dependency_creator, **kwargs) -> dict:
    """Build dict representation of the inter-deployment dependency."""
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
