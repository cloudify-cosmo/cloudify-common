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


def create_deployment_dependency(dependency_creator, source_deployment, target_deployment=None):
    dependency = {
        DEPENDENCY_CREATOR: dependency_creator,
        SOURCE_DEPLOYMENT: source_deployment,
    }

    if target_deployment:
        dependency[TARGET_DEPLOYMENT] = target_deployment
    return dependency


def dependency_creator_generator(connection_type, to_deployment):
    return '{0}.{1}'.format(connection_type, to_deployment)
