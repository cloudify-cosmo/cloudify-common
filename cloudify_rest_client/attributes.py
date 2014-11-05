########
# Copyright (c) 2014 GigaSpaces Technologies Ltd. All rights reserved
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


class ProcessedAttributes(dict):
    """
    Processed attributes.
    """

    def __init__(self, processed_attributes):
        self.update(processed_attributes)

    @property
    def deployment_id(self):
        """
        :return: The deployment id this request belongs to.
        """
        return self['deployment_id']

    @property
    def payload(self):
        """
        :return: The processed payload.
        """
        return self['payload']


class AttributesClient(object):

    def __init__(self, api):
        self.api = api

    def process(self, deployment_id, context, payload):
        """Processes `get_attribute` references in payload in respect to the
        provided context.

        :param deployment_id: The deployment's id of the node.
        :param context: The processing context
                        (dict with optional self, source, target).
        :param payload: The payload to process.
        :return: The payload with its `get_attribute` references processed.
        :rtype: ProcessedAttributes
        """
        assert deployment_id
        assert context
        assert payload
        result = self.api.post('/attributes', data={
            'deployment_id': deployment_id,
            'context': context,
            'payload': payload
        })
        return ProcessedAttributes(result)
