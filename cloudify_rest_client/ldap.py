class LdapResponse(dict):

    def __init__(self, ldap):
        self.update(ldap)

    @property
    def ldap_server(self):
        return self.get('ldap_server')

    @property
    def ldap_username(self):
        return self.get('ldap_username')

    @property
    def ldap_domain(self):
        return self.get('ldap_domain')

    @property
    def ldap_is_active_directory(self):
        return self.get('ldap_is_active_directory')

    @property
    def ldap_dn_extra(self):
        return self.get('ldap_dn_extra')

    @property
    def ldap_ca_path(self):
        return self.get('ldap_ca_path')

    @property
    def ldap_base_dn(self):
        return self.get('ldap_base_dn')

    @property
    def ldap_group_dn(self):
        return self.get('ldap_group_dn')

    @property
    def ldap_bind_format(self):
        return self.get('ldap_bind_format')

    @property
    def ldap_user_filter(self):
        return self.get('ldap_user_filter')

    @property
    def ldap_group_member_filter(self):
        return self.get('ldap_group_member_filter')

    @property
    def ldap_attribute_email(self):
        return self.get('ldap_attribute_email')

    @property
    def ldap_attribute_first_name(self):
        return self.get('ldap_attribute_first_name')

    @property
    def ldap_attribute_last_name(self):
        return self.get('ldap_attribute_last_name')

    @property
    def ldap_attribute_uid(self):
        return self.get('ldap_attribute_uid')

    @property
    def ldap_attribute_group_membership(self):
        return self.get('ldap_attribute_group_membership')


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
            ldap_base_dn=None,
            ldap_group_dn=None,
            ldap_bind_format=None,
            ldap_user_filter=None,
            ldap_group_member_filter=None,
            ldap_attribute_email=None,
            ldap_attribute_first_name=None,
            ldap_attribute_last_name=None,
            ldap_attribute_uid=None,
            ldap_attribute_group_membership=None,
            ldap_nested_levels=None,
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
            'ldap_username': ldap_username,
            'ldap_password': ldap_password,
            'ldap_base_dn': ldap_base_dn,
            'ldap_group_dn': ldap_group_dn,
            'ldap_bind_format': ldap_bind_format,
            'ldap_user_filter': ldap_user_filter,
            'ldap_group_member_filter': ldap_group_member_filter,
            'ldap_attribute_email': ldap_attribute_email,
            'ldap_attribute_first_name': ldap_attribute_first_name,
            'ldap_attribute_last_name': ldap_attribute_last_name,
            'ldap_attribute_uid': ldap_attribute_uid,
            'ldap_attribute_group_membership': (
                ldap_attribute_group_membership),
            'ldap_nested_levels': ldap_nested_levels,
        }
        if ldap_ca_path:
            with open(ldap_ca_path) as cert_handle:
                params['ldap_ca_cert'] = cert_handle.read()
        uri = '/ldap'
        response = self.api.post(uri, params)
        return LdapResponse(response)

    def get_status(self):
        uri = '/ldap'
        return self.api.get(uri)
