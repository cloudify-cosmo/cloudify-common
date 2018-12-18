########
# Copyright (c) 2015 GigaSpaces Technologies Ltd. All rights reserved
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


class Token(dict):

    def __init__(self, token):
        super(Token, self).__init__()
        self.update(token)

    @property
    def value(self):
        """
        :returns: The value of the token.
        """
        return self.get('value')

    @property
    def role(self):
        """
        :returns: The role of the user associated with the token
        """
        return self.get('role')


class TokensClient(object):

    def __init__(self, api):
        self.api = api

    def get(self):
        """Get an authentication token.

        :rtype: Token
        """
        response = self.api.get('/tokens')
        return Token(response)


class UserTokensClient(object):

    def __init__(self, api):
        self.api = api

    def get(self, user_id):
        """
        Get an authentication REST token of a specified user
        :param user_id: The id of the user
        :return: Token
        """
        uri = '/user-tokens/{0}'.format(user_id)
        response = self.api.get(uri)
        return Token(response)
