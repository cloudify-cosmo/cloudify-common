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

import sys

import testtools

from cloudify import utils
from cloudify import event
from cloudify._compat import text_type


class TestEvent(testtools.TestCase):

    def test_event_has_output(self):
        test_event = _event('cloudify_event')
        self.assertTrue(test_event.has_output)
        test_event = _event('cloudify_log', level='INFO')
        self.assertTrue(test_event.has_output)
        test_event = _event('cloudify_log', level='DEBUG')
        self.assertFalse(test_event.has_output)
        test_event = _event('cloudify_log', level='DEBUG',
                            verbosity_level=event.MEDIUM_VERBOSE)
        self.assertTrue(test_event.has_output)

    def test_task_failure_causes(self):
        message = 'test_message'
        test_event = _event('cloudify_event',
                            event_type='task_failed',
                            message=message)
        self.assertEqual(test_event.text, message)
        causes = []
        test_event = _event('cloudify_event',
                            event_type='task_failed',
                            message=message,
                            causes=causes)
        self.assertEqual(test_event.text, message)
        try:
            raise RuntimeError()
        except RuntimeError:
            _, ex, tb = sys.exc_info()
            causes = [utils.exception_to_error_cause(ex, tb)]
        test_event = _event('cloudify_event',
                            event_type='task_failed',
                            message=message,
                            causes=causes,
                            verbosity_level=event.LOW_VERBOSE)
        text = test_event.text
        self.assertIn(message, text)
        self.assertNotIn('Causes (most recent cause last):', text)
        self.assertEqual(1, text.count(causes[0]['traceback']))
        causes = causes + causes
        test_event = _event('cloudify_event',
                            event_type='task_failed',
                            message=message,
                            causes=causes,
                            verbosity_level=event.LOW_VERBOSE)
        text = test_event.text
        self.assertIn(message, text)
        self.assertIn('Causes (most recent cause last):', text)
        self.assertEqual(2, text.count(causes[0]['traceback']))

        test_event = _event('cloudify_event',
                            event_type='task_failed',
                            message=message,
                            causes=causes)
        text = test_event.text
        self.assertIn(message, text)
        self.assertIn('Causes (most recent cause last):', text)
        self.assertEqual(2, text.count(causes[0]['traceback']))

        # one test with task_rescheduled
        test_event = _event('cloudify_event',
                            event_type='task_rescheduled',
                            message=message,
                            causes=causes,
                            verbosity_level=event.LOW_VERBOSE)
        text = test_event.text
        self.assertIn(message, text)
        self.assertIn('Causes (most recent cause last):', text)
        self.assertEqual(2, text.count(causes[0]['traceback']))

    def test_event_unicode_event(self):
        test_event = _event('cloudify_event', level='INFO',
                            message=u'אני אבחן אותך ביסודיות',
                            deployment_id='b44a008f-a8fa-4790-xyz',
                            timestamp='NOW')
        self.assertIn(u'אני אבחן אותך ביסודיות', text_type(test_event))

    def test_event_unicode_log(self):
        test_event = _event('cloudify_log', level='DEBUG',
                            message=u'אני אבחן אותך ביסודיות',
                            deployment_id='64be2ce0-93c5-453e-qwe',
                            timestamp='THEN')
        self.assertIn(u'אני אבחן אותך ביסודיות', text_type(test_event))


def _event(type, event_type=None, level=None, message=None,
           causes=None, verbosity_level=event.NO_VERBOSE, deployment_id=None,
           timestamp=None):
    result = {'type': type, 'context': {}}
    if deployment_id:
        result['context']['deployment_id'] = deployment_id
    if event_type:
        result['event_type'] = event_type
    if level:
        result['level'] = level
    if message:
        result['message'] = {'text': message}
    if causes:
        result['context']['task_error_causes'] = causes
    if timestamp:
        result['timestamp'] = timestamp
    return event.Event(result, verbosity_level=verbosity_level)
