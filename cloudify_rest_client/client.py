import itertools
import json
import logging
import numbers
import random
import re
import types

import requests
from base64 import b64encode
from requests.packages import urllib3

from cloudify import constants
from cloudify.utils import ipv6_url_compat

from .utils import is_kerberos_env
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
from cloudify_rest_client.audit_log import AuditLogClient
from cloudify_rest_client.community_contacts import CommunityContactsClient

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


class HTTPClientBase:
    def __init__(
        self,
        host,
        port=DEFAULT_PORT,
        protocol=DEFAULT_PROTOCOL,
        api_version=DEFAULT_API_VERSION,
        headers=None,
        query_params=None,
        cert=None,
        trust_all=False,
        username=None,
        password=None,
        token=None,
        tenant=None,
        kerberos_env=None,
        timeout=None,
        retries=None,
    ):
        hosts = list(host) if isinstance(host, list) else [host]
        hosts = [ipv6_url_compat(h) for h in hosts]
        random.shuffle(hosts)
        self.hosts = itertools.cycle(hosts)
        self.retries = retries or len(hosts)

        self.port = port
        self.protocol = protocol
        self.api_version = api_version
        self.kerberos_env = kerberos_env
        self.default_timeout_sec = timeout or (5, None)

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
        self._set_header(CLOUDIFY_TENANT_HEADER, tenant)
        self._has_kerberos = None

        if self.kerberos_env is not None:
            self.has_kerberos = True
        else:
            self.has_kerberos = bool(HTTPKerberosAuth) and is_kerberos_env()

        if self.has_kerberos:
            self.auth = self._make_kerberos_auth()
        else:
            self.auth = None

    def _make_kerberos_auth(self):
        if self.has_kerberos and not self.has_auth_header():
            if HTTPKerberosAuth is None:
                raise exceptions.CloudifyClientError(
                    'Trying to create a client with kerberos, '
                    'but kerberos_env does not exist')
            return HTTPKerberosAuth()

    def _get_total_headers(self, headers):
        total_headers = self.headers.copy()
        if headers:
            total_headers.update(headers)
        return total_headers

    def _get_total_params(self, params):
        total_params = self.query_params.copy()
        if params:
            total_params.update(params)
        return {
            k: self._format_querystring_param(v)
            for k, v in total_params.items()
            if k is not None and v is not None
        }

    def _format_querystring_param(self, param):
        if isinstance(param, bool):
            return str(param)
        return param

    def get_host(self):
        return next(self.hosts)

    def get_request_url(self, host, uri, versioned=True):
        base_url = f'{self.protocol}://{host}:{self.port}/api'
        if not versioned:
            return f'{base_url}{uri}'
        return f'{base_url}/{self.api_version}{uri}'

    def _log_request(self, method, uri, data):
        if not self.logger.isEnabledFor(logging.DEBUG):
            return
        self.logger.debug(
            'Sending request: %s %s; body: %r',
            method,
            uri,
            data if not data or isinstance(data, dict) else '(bytes data)',
        )

    def get(self, uri, data=None, params=None, headers=None, _include=None,
            expected_status_code=200, stream=False, timeout=None,
            versioned_url=True, wrapper=None):
        if _include:
            fields = ','.join(_include)
            if not params:
                params = {}
            params['_include'] = fields

        self._log_request('GET', uri, data=data)
        return self.do_request(
            'GET',
            uri,
            data=data,
            params=self._get_total_params(params),
            headers=self._get_total_headers(headers),
            expected_status_code=expected_status_code,
            stream=stream,
            timeout=timeout,
            wrapper=wrapper,
            versioned_url=versioned_url,
        )

    def put(self, uri, data=None, params=None, headers=None,
            expected_status_code=200, stream=False, timeout=None,
            wrapper=None, versioned_url=True):
        self._log_request('PUT', uri, data)
        return self.do_request(
            'PUT',
            uri,
            data=data,
            params=self._get_total_params(params),
            headers=self._get_total_headers(headers),
            expected_status_code=expected_status_code,
            stream=stream,
            timeout=timeout,
            wrapper=wrapper,
            versioned_url=versioned_url,
        )

    def patch(self, uri, data=None, params=None, headers=None,
              expected_status_code=200, stream=False, timeout=None,
              wrapper=None, versioned_url=True):
        self._log_request('PATCH', uri, data)
        return self.do_request(
            'PATCH',
            uri,
            data=data,
            params=self._get_total_params(params),
            headers=self._get_total_headers(headers),
            expected_status_code=expected_status_code,
            stream=stream,
            timeout=timeout,
            wrapper=wrapper,
            versioned_url=versioned_url,
        )

    def post(self, uri, data=None, params=None, headers=None,
             expected_status_code=200, stream=False, timeout=None,
             wrapper=None, versioned_url=True):
        self._log_request('POST', uri, data)
        return self.do_request(
            'POST',
            uri,
            data=data,
            params=self._get_total_params(params),
            headers=self._get_total_headers(headers),
            expected_status_code=expected_status_code,
            stream=stream,
            timeout=timeout,
            wrapper=wrapper,
            versioned_url=versioned_url,
        )

    def delete(self, uri, data=None, params=None, headers=None,
               expected_status_code=(200, 204), stream=False, timeout=None,
               wrapper=None, versioned_url=True):
        self._log_request('DELETE', uri, data)
        return self.do_request(
            'DELETE',
            uri,
            data=data,
            params=self._get_total_params(params),
            headers=self._get_total_headers(headers),
            expected_status_code=expected_status_code,
            stream=stream,
            timeout=timeout,
            wrapper=wrapper,
            versioned_url=versioned_url,
        )

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

    def has_auth_header(self):
        auth_headers = [constants.CLOUDIFY_AUTHENTICATION_HEADER,
                        constants.CLOUDIFY_EXECUTION_TOKEN_HEADER,
                        constants.CLOUDIFY_TOKEN_AUTHENTICATION_HEADER]
        return any(header in self.headers for header in auth_headers)

    def _is_fileserver_download(self, response):
        """Is this response a file-download response?

        404 responses to requests that download files, need to be retried
        with all managers in the cluster: if some file was not yet
        replicated, another manager might have this file.

        This is because the file replication is asynchronous.
        """
        # str() the url because sometimes (aiohttp) it is a URL object
        if re.search('/(blueprints|snapshots)/', str(response.url)):
            return True
        disposition = response.headers.get('Content-Disposition')
        if not disposition:
            return False
        return disposition.strip().startswith('attachment')


class HTTPClient(HTTPClientBase):
    def __init__(self, *args, **kwargs):
        session = kwargs.pop('session', None)
        super().__init__(*args, **kwargs)

        if session is None:
            session = requests.Session()
        self._session = session

    def do_request(
        self,
        method,
        uri,
        data,
        params,
        headers,
        expected_status_code,
        stream,
        timeout,
        wrapper,
        versioned_url=True,
    ):
        """Run a requests method.

        :param request_method: string choosing the method, eg "get" or "post"
        :param request_url: the URL to run the request against
        :param data: request data, dict or string
        :param params: querystring parameters, as a dict
        :param headers: request headers, as a dict
        :param expected_status_code: check that the response is this
            status code, can also be an iterable of allowed status codes.
        :param stream: whether or not to stream the response
        :param verify: the CA cert path
        :param timeout: request timeout or a (connect, read) timeouts pair
        """
        requests_method = getattr(self._session, method.lower(), None)
        if requests_method is None:
            raise RuntimeError(f'Unknown method: {method}')

        copied_data = None
        if isinstance(data, types.GeneratorType):
            copied_data = itertools.tee(data, self.retries)
        elif isinstance(data, dict):
            data = json.dumps(data)

        errors = {}
        for retry in range(self.retries):
            manager_to_try = self.get_host()
            request_url = self.get_request_url(
                manager_to_try,
                uri,
                versioned=versioned_url,
            )
            if copied_data is not None:
                data = copied_data[retry]
            try:
                response = requests_method(
                    request_url,
                    data=data,
                    params=params,
                    headers=headers,
                    stream=stream,
                    verify=self.get_request_verify(),
                    timeout=timeout or self.default_timeout_sec,
                    auth=self.auth,
                )
            except requests.exceptions.SSLError as e:
                errors[manager_to_try] = exceptions.format_ssl_error(e)
                self.logger.debug(
                    'HTTP Client error: %s %s: %s', method, uri, e)
                continue
            except requests.exceptions.ConnectionError as e:
                errors[manager_to_try] = exceptions.format_connection_error(e)
                self.logger.debug(
                    'HTTP Client error: %s %s: %s', method, uri, e)
                continue
            except exceptions.CloudifyClientError as e:
                self.logger.debug(
                    'HTTP Client error: %s %s: %s', method, uri, e)
                errors[manager_to_try] = e.status_code
                if e.response.status_code == 502:
                    continue
                if e.response.status_code == 404 and \
                        self._is_fileserver_download(e.response):
                    continue
                else:
                    raise

            return self.process_response(
                response,
                expected_status_code,
                stream,
                wrapper,
            )
        mgr_errors = ', '.join(f'{host}: {e}' for host, e in errors.items())
        raise exceptions.CloudifyClientError(
            f'HTTP Client error: {method} {uri} ({mgr_errors})')

    def process_response(
        self,
        response,
        expected_status_code,
        stream,
        wrapper
    ):
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
            raise exceptions.CloudifyClientError.from_response(
                response, response.status_code, response.content)

        if response.status_code == 204:
            return None

        if stream:
            return StreamedResponse(response)

        response_json = response.json()

        if response.history:
            response_json['history'] = response.history

        if wrapper:
            return wrapper(response_json)
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


class StreamedResponse(object):

    def __init__(self, response):
        self._response = response

    @property
    def headers(self):
        return self._response.headers

    def bytes_stream(self, chunk_size=8192):
        return self._response.iter_content(chunk_size)

    def lines_stream(self):
        return self._response.iter_lines()

    def close(self):
        self._response.close()


class CloudifyClient(object):
    """Cloudify's management client."""
    client_class = HTTPClient

    def __init__(
        self,
        host='localhost',
        port=None,
        protocol=DEFAULT_PROTOCOL,
        api_version=DEFAULT_API_VERSION,
        headers=None,
        query_params=None,
        cert=None,
        trust_all=False,
        username=None,
        password=None,
        token=None,
        tenant=None,
        kerberos_env=None,
        timeout=None,
        session=None,
        retries=None,
    ):
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
                        (5, None)- 5 seconds connect timeout, no read timeout.
        :param session: a requests.Session to use for all HTTP calls
        :param retries: requests that fail with a connection error will be
                        retried this many times
        :param retry_interval: wait this many seconds between retries
        :return: Cloudify client instance.
        """

        if not port:
            if protocol == SECURED_PROTOCOL:
                port = SECURED_PORT
            else:
                port = DEFAULT_PORT

        self.host = host
        self._client = self.client_class(
            host=host,
            port=port,
            protocol=protocol,
            api_version=api_version,
            headers=headers,
            query_params=query_params,
            cert=cert,
            trust_all=trust_all,
            username=username,
            password=password,
            token=token,
            tenant=tenant,
            kerberos_env=kerberos_env,
            timeout=timeout,
            session=session,
            retries=retries,
        )

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
        self.community_contacts = CommunityContactsClient(self._client)
        self.auditlog = AuditLogClient(self._client)
