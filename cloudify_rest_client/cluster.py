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


class ClusterStatusClient(object):
    """
    Cloudify's cluster status client.
    """

    def __init__(self, api):
        self.api = api

    def get_status(self, summary_format=False):
        """
        :return: Cloudify cluster's status.
        """
        return self.api.get('/cluster-status',
                            params={'summary': summary_format})
