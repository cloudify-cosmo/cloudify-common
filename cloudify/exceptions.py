########
# Copyright (c) 2013 GigaSpaces Technologies Ltd. All rights reserved
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


class NonRecoverableError(Exception):
    """
    An error raised by plugins to denote that no retry should be attempted by
    by the executing workflow engine.
    """
    def __init__(self, *args, **kwargs):
        self.causes = kwargs.pop('causes', []) or []
        super(NonRecoverableError, self).__init__(*args, **kwargs)


class RecoverableError(Exception):
    """
    An error raised by plugins to explicitly denote that this is a recoverable
    error (note that this is the default behavior). It is possible specifying
    how many seconds should pass before a retry is attempted thus overriding
    the bootstrap context configuration parameter:
    ``cloudify.workflows.retry_interval``

    :param retry_after: How many seconds should the workflow engine wait
                        before re-executing the task the raised this
                        exception. (only applies when the workflow engine
                        decides that this task should be retried)
    """

    def __init__(self, message='', retry_after=None, causes=None, **kwargs):
        if retry_after is not None:
            suffix = '[retry_after={0}]'.format(retry_after)
            if suffix not in message:
                message = '{0} {1}'.format(message, suffix)
        self.retry_after = retry_after
        self.causes = causes or []
        super(RecoverableError, self).__init__(message, **kwargs)


class OperationRetry(RecoverableError):
    """
    An error raised internally when an operation uses the ctx.operation.retry
    API for specifying that an operation should be retried.
    """
    pass


class CloudifySerializationRetry(RecoverableError):
    """
    An error raised internally when an operation is blocking other node
    instances from installing.
    """
    pass


class HttpException(NonRecoverableError):
    """
    Wraps HTTP based exceptions that may be raised.

    :param url: The url the request was made to.
    :param code: The response status code.
    :param message: The underlying reason for the error.

    """

    def __init__(self, url, code, message, causes=None, **kwargs):
        self.url = url
        self.code = code
        self.message = message
        super(HttpException, self).__init__(str(self), causes=causes, **kwargs)

    def __str__(self):
        return "{0} ({1}) : {2}".format(self.code, self.url, self.message)


class CommandExecutionError(RuntimeError):

    """
    Indicates a command failed to execute. note that this is different than
    the CommandExecutionException in that in this case, the command
    execution did not even start, and therefore there is not return code or
    stdout output.

    :param command: The command executed
    :param error: the error preventing the command from executing
    """

    def __init__(self, command, error=None):
        self.command = command
        self.error = error
        super(RuntimeError, self).__init__(self.__str__())

    def __str__(self):
        return "Failed executing command: {0}." \
               "\nerror: {1}".format(self.command, self.error)


class CommandExecutionException(Exception):

    """
    Indicates a command was executed, however some sort of error happened,
    resulting in a non-zero return value of the process.

    :param command: The command executed
    :param code: process exit code
    :param error: process stderr output
    :param output: process stdout output
    """

    def __init__(self, command, error, output, code):
        self.command = command
        self.error = error
        self.code = code
        self.output = output
        message = (
            u"Command '{0}' executed with an error."
            u"\ncode: {1}"
            u"\nerror: {2}"
            u"\noutput: {3}"
            .format(self.command, self.code,
                    self.error or None,
                    self.output or None)
        )
        super(CommandExecutionException, self).__init__(message)


class TimeoutException(Exception):
    """Indicates some kind of timeout happened."""
    pass


class ProcessKillCancelled(NonRecoverableError):
    """The execution subprocess was kill-cancelled.

    This exception should never be shown to the user, but instead it's
    used to notify the taskhandler to not send a response.
    """


class ClosedAMQPClientException(Exception):
    """Raised when attempting to use a closed AMQP client"""
    pass


class PluginInstallationError(Exception):
    """An error occurred during plugin installation"""


class StopAgent(Exception):
    """Raise this in an operation to force the agent to exit."""
    def __init__(self, *args, **kwargs):
        self.causes = kwargs.pop('causes', []) or []
        super(StopAgent, self).__init__(*args, **kwargs)


class WorkflowFailed(RuntimeError):
    """A workflow has failed."""
    def __init__(self, *args, **kwargs):
        self.causes = kwargs.pop('causes', []) or []
        self.hide_traceback = kwargs.pop('hide_traceback', False)
        super(WorkflowFailed, self).__init__(*args, **kwargs)


class InvalidBlueprintImport(RuntimeError):
    def __init__(self, *args, **kwargs):
        self.causes = kwargs.pop('causes', []) or []
        super(InvalidBlueprintImport, self).__init__(*args, **kwargs)
