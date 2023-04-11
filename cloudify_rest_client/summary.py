########
# Copyright (c) 2018 Cloudify Ltd. All rights reserved
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


class SummaryClient(object):
    def __init__(self, api, summary_type):
        self.api = api
        self.summary_type = summary_type

    def get(self, _target_field, _sub_field=None, **kwargs):
        params = {
            '_target_field': _target_field,
        }
        if _sub_field:
            params['_sub_field'] = _sub_field
        params.update(kwargs)
        response = self.api.get(
            '/summary/{summary_type}'.format(summary_type=self.summary_type),
            params=params,
        )
        return ListResponse(response['items'], response['metadata'])


class SummariesClient(object):
    def __init__(self, api):
        self.blueprints = SummaryClient(api, 'blueprints')
        self.deployments = SummaryClient(api, 'deployments')
        self.executions = SummaryClient(api, 'executions')
        self.nodes = SummaryClient(api, 'nodes')
        self.node_instances = SummaryClient(api, 'node_instances')
        self.execution_schedules = SummaryClient(api, 'execution_schedules')
