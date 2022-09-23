#########
# Copyright (c) 2015 GigaSpaces Technologies Ltd. All rights reserved
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
#  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  * See the License for the specific language governing permissions and
#  * limitations under the License.

import glob
import os
import re

from dsl_parser.exceptions import DSLParsingLogicException
from dsl_parser.import_resolver.abstract_import_resolver \
    import AbstractImportResolver, read_import

DEFAULT_RULES = []
DEFAULT_RESLOVER_RULES_KEY = 'rules'


class DefaultResolverValidationException(Exception):
    pass


class DefaultImportResolver(AbstractImportResolver):
    """
    This class is a default implementation of an import resolver.
    This resolver uses the rules to replace URL's prefix with another prefix
    and tries to resolve the new URL (after the prefix has been replaced).
    If there aren't any rules, none of the rules matches or
    none of the prefix replacements works,
    the resolver will try to use the original URL.

    Each rule in the ``rules`` list is expected to be
    a dictionary with one (key, value) pair which represents
    a prefix and its replacement which can be used to resolve the import url.

    The resolver will go over the rules and for each matching rule
    (its key is a prefix of the url) it will replace the prefix
    with the value and will try to resolve the new url.

    For example:
        The rules list: [
            {'http://prefix1': 'http://prefix1_replacement'},
            {'http://prefix2': 'http://prefix2_replacement1'},
            {'http://prefix2': 'http://prefix2_replacement2'}
        ]
        contains three rules that can be used for resolve URLs that
        starts with 'http://prefix1' and 'http://prefix2'.
        If the url is 'http://prefix2.suffix2.org' than the resolve method
        will find a match in both the second and the third rules.

        It will first try to apply the second rule by replacing the url's
        prefix with the second rule value ('http://prefix2_replacement1')
        and will try to resolve the new url:
        'http://prefix2_replacement1.suffix2.org'.

        In case this url cannot be resolved, it will try to apply
        the third rule by replacing the url's prefix with
        the third rule value ('http://prefix2_replacement2')
        and will try to resolve the url:
        'http://prefix2_replacement2.suffix2.org'.

        If this url, also, cannot be resolved,
        it will try to resolve the original url,
        i.e. http://prefix2.suffix2.org'

        In case that all the resolve attempts will fail,
        a DSLParsingLogicException will be raise.

    If ``fallback`` is set (the default), then in case all rules fail to
    be resolved, the original url will be tried anyway.
    """

    def __init__(self, rules=None, fallback=True):
        # set the rules
        self.rules = rules
        self._fallback = fallback
        if self.rules is None:
            self.rules = DEFAULT_RULES
        self._validate_rules()

    def resolve(self, import_url):
        failed_urls = {}
        # trying to find a matching rule that can resolve this url
        matched_any_rule = False
        for rule in self.rules:
            # the validate method checks that the dict has exactly 1 element
            prefix, value = dict(rule).popitem()
            prefix_len = len(prefix)
            if prefix == import_url[:prefix_len]:
                matched_any_rule = True
                # found a matching rule
                url_to_resolve = value + import_url[prefix_len:]
                # trying to resolve the resolved_url
                if url_to_resolve not in failed_urls:
                    # there is no point to try to resolve the same url twice
                    try:
                        return read_import(url_to_resolve)
                    except DSLParsingLogicException as ex:
                        # failed to resolve current rule,
                        # continue to the next one
                        failed_urls[url_to_resolve] = str(ex)

        if matched_any_rule and not self._fallback:
            msg = 'Failed to resolve the following urls: {0}'.format(
                failed_urls)
            ex = DSLParsingLogicException(13, msg)
            ex.failed_import = import_url
            raise ex

        # failed to resolve the url using the rules
        # trying to open the original url
        try:
            return read_import(import_url)
        except DSLParsingLogicException as ex:
            if not self.rules:
                raise
            if not failed_urls:
                # no matching rules
                msg = 'None of the resolver rules {0} was applicable, ' \
                      'failed to resolve the original import url: {1} '\
                    .format(self.rules, ex)
            else:
                # all urls failed to be resolved
                msg = 'Failed to resolve the following urls: {0}. ' \
                      'In addition, failed to resolve the original ' \
                      'import url - {1}'.format(failed_urls, ex)
            ex = DSLParsingLogicException(13, msg)
            ex.failed_import = import_url
            raise ex

    def _validate_rules(self):
        if not isinstance(self.rules, list):
            raise DefaultResolverValidationException(
                'Invalid parameters supplied for the default resolver: '
                'The `{0}` parameter must be a list but it is of type {1}.'
                .format(
                    DEFAULT_RESLOVER_RULES_KEY,
                    type(self.rules).__name__))
        for rule in self.rules:
            if not isinstance(rule, dict):
                raise DefaultResolverValidationException(
                    'Invalid parameters supplied for the default resolver: '
                    'Each rule must be a dictionary but the rule '
                    '[{0}] is of type {1}.'
                    .format(rule, type(rule).__name__))
            if not len(rule) == 1:
                raise DefaultResolverValidationException(
                    'Invalid parameters supplied for the default resolver: '
                    'Each rule must be a dictionary with one (key,value) pair '
                    'but the rule [{0}] has {1} keys.'
                    .format(rule, len(rule)))

    def retrieve_plugin(self, import_url, **kwargs):
        pass

    @staticmethod
    def _plugin_yamls_for_dsl_version(plugin_path, dsl_version):
        yaml_files = glob.glob(os.path.join(plugin_path, '*.yaml'))
        if len(yaml_files) <= 1:
            return yaml_files

        if dsl_version:
            # If file exists, return *{dsl_version}.yaml name
            result = [yaml_file for yaml_file in yaml_files
                      if '{0}.yaml'.format(dsl_version) in yaml_file]
            if result:
                return result

        # Return the first file that does not match *_\d_\d+\.yaml pattern
        for yaml_file in yaml_files:
            if not re.search(r"_\d_\d+\.yaml$", yaml_file):
                return [yaml_file]

        # If dsl_version is not provided, return the last yaml_file
        if not dsl_version:
            return [sorted(yaml_files)[-1]]
