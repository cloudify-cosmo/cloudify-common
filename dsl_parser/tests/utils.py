########
# Copyright (c) 2018 Cloudify Platform Ltd. All rights reserved
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

from dsl_parser import parser, constants
from ..import_resolver.default_import_resolver import DefaultImportResolver


class ResolverWithBlueprintSupport(DefaultImportResolver):
    def __init__(self, blueprint_mapping):
        super(ResolverWithBlueprintSupport, self).__init__()
        self.blueprint_mapping = blueprint_mapping

    def _is_blueprint_url(self, import_url):
        return import_url.startswith(constants.BLUEPRINT_IMPORT)

    def fetch_import(self, import_url):
        if self._is_blueprint_url(import_url):
            return self._fetch_blueprint_import(import_url)
        return super(ResolverWithBlueprintSupport,
                     self).fetch_import(import_url)

    def _fetch_blueprint_import(self, import_url):
        return parser.parse_from_import_blueprint(
            dsl_location=None,
            dsl_string=self.blueprint_mapping[import_url],
            resolver=self,
            resources_base_path=None)
