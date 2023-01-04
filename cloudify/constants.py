########
# Copyright (c) 2019 Cloudify Platform Ltd. All rights reserved
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

REST_PROTOCOL_KEY = 'REST_PROTOCOL'
REST_HOST_KEY = 'REST_HOST'
REST_PORT_KEY = 'REST_PORT'
ADMIN_API_TOKEN_KEY = 'ADMIN_API_TOKEN'
AGENT_WORK_DIR_KEY = 'AGENT_WORK_DIR'
MANAGER_FILE_SERVER_URL_KEY = 'MANAGER_FILE_SERVER_URL'
MANAGER_FILE_SERVER_ROOT_KEY = 'MANAGER_FILE_SERVER_ROOT'
MANAGER_FILE_SERVER_SCHEME = 'MANAGER_FILE_SERVER_SCHEME'
MANAGER_NAME = 'MANAGER_NAME'
FILE_SERVER_RESOURCES_FOLDER = 'resources'
FILE_SERVER_BLUEPRINTS_FOLDER = 'blueprints'
FILE_SERVER_DEPLOYMENTS_FOLDER = 'deployments'
FILE_SERVER_UPLOADED_BLUEPRINTS_FOLDER = 'uploaded-blueprints'
FILE_SERVER_SNAPSHOTS_FOLDER = 'snapshots'
FILE_SERVER_PLUGINS_FOLDER = 'plugins'
FILE_SERVER_GLOBAL_RESOURCES_FOLDER = 'global-resources'
FILE_SERVER_TENANT_RESOURCES_FOLDER = 'tenant-resources'
FILE_SERVER_AUTHENTICATORS_FOLDER = 'authenticators'

MANAGER_ROOT_PATH = '/opt/manager'
MANAGER_RESOURCES_PATH = '{0}/{1}'.format(MANAGER_ROOT_PATH,
                                          FILE_SERVER_RESOURCES_FOLDER)
MANAGER_PLUGINS_PATH = '{0}/{1}'.format(MANAGER_RESOURCES_PATH,
                                        FILE_SERVER_PLUGINS_FOLDER)

AGENT_INSTALL_METHOD_NONE = 'none'
AGENT_INSTALL_METHOD_REMOTE = 'remote'
AGENT_INSTALL_METHOD_INIT_SCRIPT = 'init_script'
AGENT_INSTALL_METHOD_PROVIDED = 'provided'
AGENT_INSTALL_METHOD_PLUGIN = 'plugin'
AGENT_INSTALL_METHODS = [
    AGENT_INSTALL_METHOD_NONE,
    AGENT_INSTALL_METHOD_REMOTE,
    AGENT_INSTALL_METHOD_INIT_SCRIPT,
    AGENT_INSTALL_METHOD_PROVIDED,
    AGENT_INSTALL_METHOD_PLUGIN
]
AGENT_INSTALL_METHODS_SCRIPTS = [
    AGENT_INSTALL_METHOD_INIT_SCRIPT,
    AGENT_INSTALL_METHOD_PROVIDED,
    AGENT_INSTALL_METHOD_PLUGIN
]
# install methods that mean the agent is actually installed (and so can be
# upgraded - not none or provided)
AGENT_INSTALL_METHODS_INSTALLED = [
    AGENT_INSTALL_METHOD_INIT_SCRIPT,
    AGENT_INSTALL_METHOD_PLUGIN,
    AGENT_INSTALL_METHOD_REMOTE,
]

LOCAL_RESOURCES_ROOT_ENV_KEY = 'CFY_RESOURCES_ROOT'

COMPUTE_NODE_TYPE = 'cloudify.nodes.Compute'

BROKER_PORT_NO_SSL = 5672
BROKER_PORT_SSL = 5671
CELERY_TASK_RESULT_EXPIRES = 600
LOCAL_REST_CERT_FILE_KEY = 'LOCAL_REST_CERT_FILE'
SECURED_PROTOCOL = 'https'
KERBEROS_ENV_KEY = 'KERBEROS_ENV'

BROKER_SSL_CERT_PATH = 'BROKER_SSL_CERT_PATH'
BYPASS_MAINTENANCE = 'BYPASS_MAINTENANCE'
LOGGING_CONFIG_FILE = '/etc/cloudify/logging.conf'
CLUSTER_SETTINGS_PATH_KEY = 'CLOUDIFY_CLUSTER_SETTINGS_PATH'

LOGS_EXCHANGE_NAME = 'cloudify-logs'
EVENTS_EXCHANGE_NAME = 'cloudify-events-topic'
CLUSTER_SERVICE_EXCHANGE_NAME = 'cloudify-cluster-service'

MGMTWORKER_QUEUE = 'cloudify.management'
DEPLOYMENT = 'deployment'
NODE_INSTANCE = 'node-instance'
RELATIONSHIP_INSTANCE = 'relationship-instance'

DEFAULT_NETWORK_NAME = 'default'

# This *seriously* shouldn't be here, but we use it in snapshot restore,
# so it's here until the snapshots become more manageable
SECURITY_FILE_LOCATION = '/opt/manager/rest-security.conf'

SUPPORTED_ARCHIVE_TYPES = ('zip', 'tar', 'tar.gz', 'tar.bz2')

NEW_TOKEN_FILE_NAME = 'new_token'

CLOUDIFY_EXECUTION_TOKEN_HEADER = 'Execution-Token'
CLOUDIFY_AUTHENTICATION_HEADER = 'Authorization'
CLOUDIFY_TOKEN_AUTHENTICATION_HEADER = 'Authentication-Token'

NAMESPACE_BLUEPRINT_IMPORT_DELIMITER = '--'

COMPONENT = 'component'
SHARED_RESOURCE = 'sharedresource'

TASK_PENDING = 'pending'
TASK_SENDING = 'sending'
TASK_SENT = 'sent'
TASK_STARTED = 'started'
TASK_RESCHEDULED = 'rescheduled'
TASK_RESPONSE_SENT = 'response-sent'
TASK_SUCCEEDED = 'succeeded'
TASK_FAILED = 'failed'

TERMINATED_STATES = [
    TASK_RESCHEDULED, TASK_SUCCEEDED, TASK_FAILED
]

CONVENTION_APPLICATION_BLUEPRINT_FILE = 'blueprint.yaml'
INSPECT_TIMEOUT = 30

KEEP_TRYING_HTTP_TOTAL_TIMEOUT_SEC = 3600
MAX_WAIT_BETWEEN_HTTP_RETRIES_SEC = 60
