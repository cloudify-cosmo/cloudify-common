import json
import logging
import numbers

import requests
from base64 import b64encode
from requests.packages import urllib3

from cloudify import constants
from cloudify.utils import ipv6_url_compat

from cloudify_rest_client.utils import is_kerberos_env, StreamedResponse
from cloudify_rest_client import exceptions
from cloudify_rest_client.idp import IdentityProviderClient
from cloudify_rest_client.ldap import LdapClient
from cloudify_rest_client.nodes import NodesClient
from cloudify_rest_client.users import UsersClient
from cloudify_rest_client.sites import SitesClient
from cloudify_rest_client.agents import AgentsClient
from cloudify_rest_client.events import EventsClient
from cloudify_rest_client.license import LicenseClient
from cloudify_rest_client.manager import ManagerClient
from cloudify_rest_client.plugins import PluginsClient
from cloudify_rest_client.secrets import SecretsClient
from cloudify_rest_client.secrets_providers import SecretsProvidersClient
from cloudify_rest_client.tenants import TenantsClient
from cloudify_rest_client.evaluate import EvaluateClient
from cloudify_rest_client.summary import SummariesClient
from cloudify_rest_client.snapshots import SnapshotsClient
from cloudify_rest_client.log_bundles import LogBundlesClient
from cloudify_rest_client.cluster import ClusterStatusClient
from cloudify_rest_client.blueprints import BlueprintsClient
from cloudify_rest_client.executions import (
    ExecutionsClient,
    ExecutionGroupsClient
)
from cloudify_rest_client.execution_schedules import ExecutionSchedulesClient
from cloudify_rest_client.user_groups import UserGroupsClient
from cloudify_rest_client.deployments import (
    DeploymentsClient,
    DeploymentGroupsClient
)
from cloudify_rest_client.permissions import PermissionsClient
from cloudify_rest_client.maintenance import MaintenanceModeClient
from cloudify_rest_client.plugins_update import PluginsUpdateClient
from cloudify_rest_client.node_instances import NodeInstancesClient
from cloudify_rest_client.tokens import TokensClient
from cloudify_rest_client.deployment_updates import DeploymentUpdatesClient
from cloudify_rest_client.operations import OperationsClient, TasksGraphClient
from cloudify_rest_client.deployment_modifications import (
    DeploymentModificationsClient)
from cloudify_rest_client.inter_deployment_dependencies import (
    InterDeploymentDependencyClient)
from cloudify_rest_client.labels import (DeploymentsLabelsClient,
                                         BlueprintsLabelsClient)
from cloudify_rest_client.filters import (DeploymentsFiltersClient,
                                          BlueprintsFiltersClient)
from cloudify_rest_client.workflows import WorkflowsClient
from cloudify_rest_client.resources import ResourcesClient
from cloudify_rest_client.audit_log import AuditLogClient
from cloudify_rest_client.community_contacts import CommunityContactsClient
from cloudify_async_client.audit_log import AuditLogAsyncClient

try:
    from requests_kerberos import HTTPKerberosAuth
except Exception:
    # requests_kerberos library require pykerberos.
    # pykerberos require krb5-devel, which isn't python lib.
    # Kerberos users will need to manually install it.
    HTTPKerberosAuth = None


DEFAULT_PORT = 80
SECURED_PORT = 443
SECURED_PROTOCOL = 'https'
DEFAULT_PROTOCOL = 'http'
DEFAULT_API_VERSION = 'v3.1'
BASIC_AUTH_PREFIX = 'Basic'
CLOUDIFY_TENANT_HEADER = 'Tenant'

urllib3.disable_warnings(urllib3.exceptions.InsecurePlatformWarning)


class HTTPClient(object):

    def __init__(self, host, port=DEFAULT_PORT,
                 protocol=DEFAULT_PROTOCOL, api_version=DEFAULT_API_VERSION,
                 headers=None, query_params=None, cert=None, trust_all=False,
                 username=None, password=None, token=None, tenant=None,
                 kerberos_env=None, timeout=None, session=None):
        self.port = port
        self.host = ipv6_url_compat(host)
        self.protocol = protocol
        self.api_version = api_version
        self.kerberos_env = kerberos_env
        self.default_timeout_sec = timeout or (5, 300)

        self.headers = headers.copy() if headers else {}
        if not self.headers.get('Content-type'):
            self.headers['Content-type'] = 'application/json'
        self.query_params = query_params.copy() if query_params else {}
        self.logger = logging.getLogger('cloudify.rest_client.http')
        self.cert = cert
        self.trust_all = trust_all
        self._set_header(constants.CLOUDIFY_AUTHENTICATION_HEADER,
                         self._get_auth_header(username, password),
                         log_value=False)
        self._set_header(constants.CLOUDIFY_TOKEN_AUTHENTICATION_HEADER, token)
        tenant_from_header = headers.get(CLOUDIFY_TENANT_HEADER) if headers\
            else None
        self.tenant_name = tenant or tenant_from_header
        if session is None:
            session = requests.Session()
        self._session = session

    @property
    def tenant_name(self):
        return self._tenant_name

    @tenant_name.setter
    def tenant_name(self, name):
        self._tenant_name = name
        self._set_header(CLOUDIFY_TENANT_HEADER, name)

    @property
    def base_url(self):
        return f'{self.protocol}://{self.host}:{self.port}'

    @property
    def url(self):
        return f'{self.base_url}/api/{self.api_version}'

    def has_kerberos(self):
        if self.kerberos_env is not None:
            return self.kerberos_env
        return bool(HTTPKerberosAuth) and is_kerberos_env()

    def has_auth_header(self):
        auth_headers = [constants.CLOUDIFY_AUTHENTICATION_HEADER,
                        constants.CLOUDIFY_EXECUTION_TOKEN_HEADER,
                        constants.CLOUDIFY_TOKEN_AUTHENTICATION_HEADER]
        return any(header in self.headers for header in auth_headers)

    def _raise_client_error(self, response, url=None):
        try:
            result = response.json()
        except Exception:
            if response.status_code == 304:
                error_msg = 'Nothing to modify'
                self._prepare_and_raise_exception(
                    message=error_msg,
                    error_code='not_modified',
                    status_code=response.status_code,
                    server_traceback='')
            else:
                message = response.content
                if url:
                    message = '{0} [{1}]'.format(message, url)
                error_msg = '{0}: {1}'.format(response.status_code, message)
            raise exceptions.CloudifyClientError(
                error_msg,
                status_code=response.status_code,
                response=response)
        # this can be changed after RD-3539
        message = result.get('message') or result.get('detail')
        code = result.get('error_code')
        server_traceback = result.get('server_traceback')
        self._prepare_and_raise_exception(
            message=message,
            error_code=code,
            status_code=response.status_code,
            server_traceback=server_traceback,
            response=response)

    @staticmethod
    def _prepare_and_raise_exception(message,
                                     error_code,
                                     status_code,
                                     server_traceback=None,
                                     response=None):

        error = exceptions.ERROR_MAPPING.get(error_code,
                                             exceptions.CloudifyClientError)
        raise error(message, server_traceback,
                    status_code, error_code=error_code, response=response)

    def verify_response_status(self, response, expected_code=200):
        if response.status_code != expected_code:
            self._raise_client_error(response)

    def _do_request(self, requests_method, request_url, body, params, headers,
                    expected_status_code, stream, verify, timeout):
        """Run a requests method.

        :param request_method: string choosing the method, eg "get" or "post"
        :param request_url: the URL to run the request against
        :param body: request body, as a string
        :param params: querystring parameters, as a dict
        :param headers: request headers, as a dict
        :param expected_status_code: check that the response is this
            status code, can also be an iterable of allowed status codes.
        :param stream: whether or not to stream the response
        :param verify: the CA cert path
        :param timeout: request timeout or a (connect, read) timeouts pair
        """
        auth = None
        if self.has_kerberos() and not self.has_auth_header():
            if HTTPKerberosAuth is None:
                raise exceptions.CloudifyClientError(
                    'Trying to create a client with kerberos, '
                    'but kerberos_env does not exist')
            auth = HTTPKerberosAuth()
        response = requests_method(request_url,
                                   data=body,
                                   params=params,
                                   headers=headers,
                                   stream=stream,
                                   verify=verify,
                                   timeout=timeout or self.default_timeout_sec,
                                   auth=auth)
        if self.logger.isEnabledFor(logging.DEBUG):
            for hdr, hdr_content in response.request.headers.items():
                self.logger.debug('request header:  %s: %s', hdr, hdr_content)
            self.logger.debug('reply:  "%s %s" %s', response.status_code,
                              response.reason, response.content)
            for hdr, hdr_content in response.headers.items():
                self.logger.debug('response header:  %s: %s', hdr, hdr_content)

        if isinstance(expected_status_code, numbers.Number):
            expected_status_code = [expected_status_code]
        if response.status_code not in expected_status_code:
            self._raise_client_error(response, request_url)

        if response.status_code == 204:
            return None

        if stream:
            return StreamedResponse(response)

        response_json = response.json()

        if response.history:
            response_json['history'] = response.history

        return response_json

    def get_request_verify(self):
        # disable certificate verification if user asked us to.
        if self.trust_all:
            return False
        # verify will hold the path to the self-signed certificate
        if self.cert:
            return self.cert
        # verify the certificate
        return True

    def do_request(self,
                   requests_method,
                   uri,
                   data=None,
                   params=None,
                   headers=None,
                   expected_status_code=200,
                   stream=False,
                   url_prefix=True,
                   versioned_url=True,
                   timeout=None):
        if not url_prefix:
            request_url = f'{self.base_url}{uri}'
        elif versioned_url:
            request_url = '{0}{1}'.format(self.url, uri)
        else:
            # remove version from url ending
            url = self.url.rsplit('/', 1)[0]
            request_url = '{0}{1}'.format(url, uri)

        # build headers
        headers = headers or {}
        total_headers = self.headers.copy()
        total_headers.update(headers)

        # build query params
        params = params or {}
        total_params = self.query_params.copy()
        total_params.update(params)

        # data is either dict, bytes data or None
        is_dict_data = isinstance(data, dict)
        body = json.dumps(data) if is_dict_data else data
        if self.logger.isEnabledFor(logging.DEBUG):
            log_message = 'Sending request: {0} {1}'.format(
                requests_method.__name__.upper(),
                request_url)
            if is_dict_data:
                log_message += '; body: {0}'.format(body)
            elif data is not None:
                log_message += '; body: bytes data'
            self.logger.debug(log_message)
        try:
            return self._do_request(
                requests_method=requests_method, request_url=request_url,
                body=body, params=total_params, headers=total_headers,
                expected_status_code=expected_status_code, stream=stream,
                verify=self.get_request_verify(), timeout=timeout)
        except requests.exceptions.SSLError as e:
            # Special handling: SSL Verification Error.
            # We'd have liked to use `__context__` but this isn't supported in
            # Py26, so as long as we support Py26, we need to go about this
            # awkwardly.
            if len(e.args) > 0 and 'CERTIFICATE_VERIFY_FAILED' in str(
                    e.args[0]):
                raise requests.exceptions.SSLError(
                    'Certificate verification failed; please ensure that the '
                    'certificate presented by Cloudify Manager is trusted '
                    '(underlying reason: {0})'.format(e))
            raise requests.exceptions.SSLError(
                'An SSL-related error has occurred. This can happen if the '
                'specified REST certificate does not match the certificate on '
                'the manager. Underlying reason: {0}'.format(e))
        except requests.exceptions.ConnectionError as e:
            raise requests.exceptions.ConnectionError(
                '{0}'
                '\nAn error occurred when trying to connect to the manager,'
                'please make sure it is online and all required ports are '
                'open.'
                '\nThis can also happen when the manager is not working with '
                'SSL, but the client does'.format(e)
            )

    def get(self, uri, data=None, params=None, headers=None, _include=None,
            expected_status_code=200, stream=False, url_prefix=True,
            versioned_url=True, timeout=None):
        if _include:
            fields = ','.join(_include)
            if not params:
                params = {}
            params['_include'] = fields
        return self.do_request(self._session.get,
                               uri,
                               data=data,
                               params=params,
                               headers=headers,
                               expected_status_code=expected_status_code,
                               stream=stream,
                               url_prefix=url_prefix,
                               versioned_url=versioned_url,
                               timeout=timeout)

    def put(self, uri, data=None, params=None, headers=None,
            expected_status_code=200, stream=False, url_prefix=True,
            timeout=None):
        return self.do_request(self._session.put,
                               uri,
                               data=data,
                               params=params,
                               headers=headers,
                               expected_status_code=expected_status_code,
                               stream=stream,
                               url_prefix=url_prefix,
                               timeout=timeout)

    def patch(self, uri, data=None, params=None, headers=None,
              expected_status_code=200, stream=False, url_prefix=True,
              timeout=None):
        return self.do_request(self._session.patch,
                               uri,
                               data=data,
                               params=params,
                               headers=headers,
                               expected_status_code=expected_status_code,
                               stream=stream,
                               url_prefix=url_prefix,
                               timeout=timeout)

    def post(self, uri, data=None, params=None, headers=None,
             expected_status_code=200, stream=False, url_prefix=True,
             timeout=None):
        return self.do_request(self._session.post,
                               uri,
                               data=data,
                               params=params,
                               headers=headers,
                               expected_status_code=expected_status_code,
                               stream=stream,
                               url_prefix=url_prefix,
                               timeout=timeout)

    def delete(self, uri, data=None, params=None, headers=None,
               expected_status_code=(200, 204), stream=False, url_prefix=True,
               timeout=None):
        return self.do_request(self._session.delete,
                               uri,
                               data=data,
                               params=params,
                               headers=headers,
                               expected_status_code=expected_status_code,
                               stream=stream,
                               url_prefix=url_prefix,
                               timeout=timeout)

    def _get_auth_header(self, username, password):
        if not username or not password:
            return None
        credentials = '{0}:{1}'.format(username, password).encode('utf-8')
        encoded_credentials = b64encode(credentials).decode('utf-8')
        return BASIC_AUTH_PREFIX + ' ' + encoded_credentials

    def _set_header(self, key, value, log_value=True):
        if not value:
            return
        self.headers[key] = value
        value = value if log_value else '*'
        self.logger.debug('Setting `%s` header: %s', key, value)


class CloudifyClient(object):
    """Cloudify's management client."""
    client_class = HTTPClient

    def __init__(self, host='localhost', port=None, protocol=DEFAULT_PROTOCOL,
                 api_version=DEFAULT_API_VERSION, headers=None,
                 query_params=None, cert=None, trust_all=False,
                 username=None, password=None, token=None, tenant=None,
                 kerberos_env=None, timeout=None, session=None):
        """
        Creates a Cloudify client with the provided host and optional port.

        :param host: Host of Cloudify's management machine.
        :param port: Port of REST API service on management machine.
        :param protocol: Protocol of REST API service on management machine,
                        defaults to http.
        :param api_version: version of REST API service on management machine.
        :param headers: Headers to be added to request.
        :param query_params: Query parameters to be added to the request.
        :param cert: Path to a copy of the server's self-signed certificate.
        :param trust_all: if `False`, the server's certificate
                          (self-signed or not) will be verified.
        :param username: Cloudify User username.
        :param password: Cloudify User password.
        :param token: Cloudify User token.
        :param tenant: Cloudify Tenant name.
        :param timeout: Requests timeout value. If not set, will default to
                        (5, 300)- 5 seconds connect timeout, 300 read timeout.
        :param session: a requests.Session to use for all HTTP calls
        :return: Cloudify client instance.
        """

        if not port:
            if protocol == SECURED_PROTOCOL:
                # SSL
                port = SECURED_PORT
            else:
                port = DEFAULT_PORT

        self.host = host
        self._client = self.client_class(host, port, protocol, api_version,
                                         headers, query_params, cert,
                                         trust_all, username, password,
                                         token, tenant, kerberos_env, timeout,
                                         session)
        self.blueprints = BlueprintsClient(self._client)
        self.idp = IdentityProviderClient(self._client)
        self.permissions = PermissionsClient(self._client)
        self.snapshots = SnapshotsClient(self._client)
        self.log_bundles = LogBundlesClient(self._client)
        self.deployments = DeploymentsClient(self._client)
        self.deployment_groups = DeploymentGroupsClient(self._client)
        self.executions = ExecutionsClient(self._client)
        self.execution_groups = ExecutionGroupsClient(self._client)
        self.execution_schedules = ExecutionSchedulesClient(self._client)
        self.nodes = NodesClient(self._client)
        self.node_instances = NodeInstancesClient(self._client)
        self.manager = ManagerClient(self._client)
        self.events = EventsClient(self._client)
        self.evaluate = EvaluateClient(self._client)
        self.deployment_modifications = DeploymentModificationsClient(
            self._client)
        self.tokens = TokensClient(self._client)
        self.plugins = PluginsClient(self._client)
        self.plugins_update = PluginsUpdateClient(self._client)
        self.maintenance_mode = MaintenanceModeClient(self._client)
        self.deployment_updates = DeploymentUpdatesClient(self._client)
        self.tenants = TenantsClient(self._client)
        self.user_groups = UserGroupsClient(self._client)
        self.users = UsersClient(self._client)
        self.ldap = LdapClient(self._client)
        self.secrets = SecretsClient(self._client)
        self.secrets_providers = SecretsProvidersClient(self._client)
        self.agents = AgentsClient(self._client)
        self.summary = SummariesClient(self._client)
        self.operations = OperationsClient(self._client)
        self.tasks_graphs = TasksGraphClient(self._client)
        self.license = LicenseClient(self._client)
        self.sites = SitesClient(self._client)
        self.cluster_status = ClusterStatusClient(self._client)
        self.inter_deployment_dependencies = InterDeploymentDependencyClient(
            self._client)
        self.deployments_filters = DeploymentsFiltersClient(self._client)
        self.blueprints_filters = BlueprintsFiltersClient(self._client)
        self.deployments_labels = DeploymentsLabelsClient(self._client)
        self.blueprints_labels = BlueprintsLabelsClient(self._client)
        self.workflows = WorkflowsClient(self._client)
        self.resources = ResourcesClient(self._client)
        self.community_contacts = CommunityContactsClient(self._client)
        if AuditLogAsyncClient is None:
            self.auditlog = AuditLogClient(self._client)
        else:
            self.auditlog = AuditLogAsyncClient(self._client)
