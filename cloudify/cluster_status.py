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


MANAGER_STATUS_REPORTER = 'manager_status_reporter'
DB_STATUS_REPORTER = 'db_status_reporter'
BROKER_STATUS_REPORTER = 'broker_status_reporter'
STATUS_REPORTER_USERS = [DB_STATUS_REPORTER,
                         BROKER_STATUS_REPORTER,
                         MANAGER_STATUS_REPORTER]
MANAGER_STATUS_REPORTER_ID = 90000
DB_STATUS_REPORTER_ID = 90001
BROKER_STATUS_REPORTER_ID = 90002


class CloudifyNodeType(object):
    DB = 'db'
    BROKER = 'broker'
    MANAGER = 'manager'
    TYPE_LIST = [DB, BROKER, MANAGER]


class ServiceStatus(object):
    HEALTHY = 'OK'
    DEGRADED = 'Degraded'
    FAIL = 'Fail'


class NodeServiceStatus(object):
    ACTIVE = 'Active'
    INACTIVE = 'Inactive'
