########
# Copyright (c) 2017 GigaSpaces Technologies Ltd. All rights reserved
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


class LdapResponse(dict):

    def __init__(self, ldap):
        self.update(ldap)

    @property
    def ldap_server(self):
        """
        :return: The LDAP server endpoint.
        """
        return self.get('ldap_server')

    @property
    def ldap_username(self):
        """
        :return: The admin LDAP user set on the Cloudify Manager.
        """
        return self.get('ldap_username')

    @property
    def ldap_domain(self):
        """
        :return: The LDAP domain used by the manager.
        """
        return self.get('ldap_domain')

    @property
    def ldap_is_active_directory(self):
        """
        :return: is LDAP an active-directory instance, false otherwise.
        """
        return self.get('ldap_is_active_directory')

    @property
    def ldap_dn_extra(self):
        """
        :return: Ldap DN extras.
        """
        return self.get('ldap_dn_extra')

    @property
    def ldap_ca_path(self):
        """:return: Ldap CA path."""
        return self.get('ldap_ca_path')


class LdapClient(object):
    def __init__(self, api):
        self.api = api

    def set(self,
            ldap_server,
            ldap_username=None,
            ldap_password=None,
            ldap_is_active_directory=False,
            ldap_domain='',
            ldap_dn_extra='',
            ldap_ca_path=''):
        """
        Sets the Cloudify manager to work with the LDAP authentication against
        the specified LDAP server.
        """
        params = {
            'ldap_server': ldap_server,
            'ldap_is_active_directory': ldap_is_active_directory,
            'ldap_domain': ldap_domain,
            'ldap_dn_extra': ldap_dn_extra,
        }
        if ldap_ca_path:
            with open(ldap_ca_path) as cert_handle:
                params['ldap_ca_cert'] = cert_handle.read()
        if ldap_username:
            params['ldap_username'] = ldap_username
        if ldap_password:
            params['ldap_password'] = ldap_password
        uri = '/ldap'
        response = self.api.post(uri, params)
        return LdapResponse(response)

    def get_status(self):
        uri = '/ldap'
        return self.api.get(uri)
