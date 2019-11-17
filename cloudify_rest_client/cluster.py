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

import json

class ClusterClient(object):
    """
    Cloudify's cluster management client.
    """

    def __init__(self, api):
        self.api = api
        self._uri_prefix = '/cluster_status/{0}/{1}'

    def get_status(self):
        """
        :return: Cloudify cluster's status.
        """
        response = self.api.get('/cluster_status')
        return response

    def _report(self, node_type, node_id, status_report):
        if not isinstance(status_report, dict):
            raise Exception('Tried to send malformed cluster node\'s '
                            'status report of type {0}'.format(
                             type(status_report)))
        data = json.dumps(status_report)
        uri = self._uri_prefix.format(node_type, node_id)
        return self.api.put(uri, data, expected_status_code=201)

    def report_manager_status(self, node_id, status_report):
        return self._report('manager', node_id, status_report)

    def report_broker_node_status(self, node_id, status_report):
        return self._report('broker', node_id, status_report)

    def report_db_node_status(self, node_id, status_report):
        return self._report('db', node_id, status_report)
