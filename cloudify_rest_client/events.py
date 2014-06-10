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

__author__ = 'idanmo'


class EventsClient(object):

    def __init__(self, api):
        self.api = api

    @staticmethod
    def _create_events_query(execution_id, include_logs):
        query = {
            "bool": {
                "must": [
                    {"match": {"context.execution_id": execution_id}},
                ]
            }
        }
        match_cloudify_event = {"match": {"type": "cloudify_event"}}
        if include_logs:
            match_cloudify_log = {"match": {"type": "cloudify_log"}}
            query['bool']['should'] = [
                match_cloudify_event, match_cloudify_log
            ]
        else:
            query['bool']['must'].append(match_cloudify_event)
        return query

    def get(self,
            execution_id,
            from_event=0,
            batch_size=100,
            include_logs=False):
        """
        Returns event for the provided execution id.

        :param execution_id: Id of execution to get events for.
        :param from_event: Index of first event to retrieve on pagination.
        :param batch_size: Maximum number of events to retrieve per call.
        :param include_logs: Whether to also get logs.
        :return: Events list and total number of currently available
         events (tuple).
        """
        body = {
            "from": from_event,
            "size": batch_size,
            "sort": [{"@timestamp": {"order": "asc"}}],
            "query": self._create_events_query(execution_id, include_logs)
        }
        response = self.api.get('/events', data=body)
        events = map(lambda x: x['_source'], response['hits']['hits'])
        total_events = response['hits']['total']
        return events, total_events
