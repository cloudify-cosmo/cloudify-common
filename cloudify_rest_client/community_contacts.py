class CommunityContactsClient(object):

    def __init__(self, api):
        self.api = api

    def create(self,
               firstname,
               lastname,
               email,
               phone,
               is_eula):
        """
        Creates a new Cloudify Community contact.

        :param firstname: The contact's first name.
        :param lastname: The contact's last name.
        :param email: The contact's Email address.
        :param phone: The contact's phone number.
        :param is_eula: Whether the user agrees to the Cloudify Community
            End User License Agreement [https://cloudify.co/license-community].
        """
        params = {
            'first_name': firstname,
            'last_name': lastname,
            'email': email,
            'phone': phone,
            'is_eula': is_eula
        }
        response = self.api.post(
            '/contacts',
            data=params,
            expected_status_code=200,
        )
        customer_id = response.get('customer_id')
        self.api.post(
            '/license',
            data={'customer_id': customer_id},
            expected_status_code=200,
        )
        return customer_id
