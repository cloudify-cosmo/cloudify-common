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


EXECUTION_CANCELLED_RESULT = 'execution_cancelled'

cancel_request = False
kill_request = False

cancel_callbacks = set()
kill_callbacks = set()


def set_cancel_request():
    global cancel_request
    cancel_request = True
    for f in cancel_callbacks:
        f()


def set_kill_request():
    global cancel_request, kill_request
    cancel_request = True
    kill_request = True
    for f in kill_callbacks:
        f()
    for f in cancel_callbacks:
        f()


def has_cancel_request():
    """
    Checks for requests to cancel the workflow execution.
    This should be used to allow graceful termination of workflow executions.

    If this method is not used and acted upon, a simple 'cancel'
    request for the execution will have no effect - 'force-cancel' will have
    to be used to abruptly terminate the execution instead.

    Note: When this method returns True, the workflow should make the
    appropriate cleanups and then it must raise an ExecutionCancelled error
    if the execution indeed gets cancelled (i.e. if it's too late to cancel
    there is no need to raise this exception and the workflow should end
    normally).

    :return: whether there was a request to cancel the workflow execution
    """
    return cancel_request or kill_request


def has_kill_request():
    """Checks for requests to kill-cancel the workflow execution.

    Kill-cancelling will stop the workflow process using SIGTERM, and
    SIGKILL after 5 seconds, so if the workflow function must attempt
    cleanup before it is kill-cancelled, it must install a signal to
    catch SIGTERM, then it can confirm using this flag whether it is
    being cancelled. Then it must do the necessary cleanup within the
    5 seconds before the process is killed.

    Note that when this is set, cancel_request will always also be set,
    so if no special cleanup is necessary, there is no need to check
    this flag.
    """
    return kill_request


class ExecutionCancelled(Exception):
    """
    This exception should be raised when a workflow has been cancelled,
    once appropriate cleanups have taken place.
    """
    pass
