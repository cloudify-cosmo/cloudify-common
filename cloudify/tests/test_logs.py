# This Python file uses the following encoding: utf-8
########
# Copyright (c) 2016 GigaSpaces Technologies Ltd. All rights reserved
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

import testtools

from cloudify import logs


class TestLogs(testtools.TestCase):

    def test_create_event_message_prefix(self):
        test_event = {'type': 'cloudify_log',
                      'level': 'INFO',
                      'context': {'deployment_id': ''},
                      'timestamp': '',
                      'message': {'text': 'message'}}
        self.assertIn('message', logs.create_event_message_prefix(test_event))
        test_event['level'] = 'DEBUG'
        self.assertIsNone(logs.create_event_message_prefix(test_event))

    def test_create_event_message_prefix_unicode(self):
        test_event = {'type': 'cloudify_log',
                      'level': 'INFO',
                      'context': {'deployment_id': ''},
                      'timestamp': '',
                      'message': {'text': u'Zażółć gęślą jaźń'}}
        self.assertIn(u'Zażółć gęślą jaźń',
                      logs.create_event_message_prefix(test_event))
        test_event['level'] = 'DEBUG'
        self.assertIsNone(logs.create_event_message_prefix(test_event))
