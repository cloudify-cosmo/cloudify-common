# Copyright (c) 2017-2019 Cloudify Platform Ltd. All rights reserved
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from dsl_parser.framework import parser
from dsl_parser.elements import blueprint
from dsl_parser import functions, utils, constraints
from dsl_parser.import_resolver.default_import_resolver import \
    DefaultImportResolver


def parse_from_path(dsl_file_path,
                    resources_base_path=None,
                    resolver=None,
                    validate_version=True,
                    additional_resource_sources=()):
    with open(dsl_file_path, 'r') as f:
        dsl_string = f.read()
    plan, _ = _parse(dsl_string,
                     resources_base_path=resources_base_path,
                     dsl_location=dsl_file_path,
                     resolver=resolver,
                     validate_version=validate_version,
                     additional_resource_sources=additional_resource_sources)
    return plan


def parse(dsl_string,
          resources_base_path=None,
          dsl_location=None,
          resolver=None,
          validate_version=True,
          validate_intrinsic_function=True,
          validate_input_defaults=True):
    plan, _ = _parse(dsl_string,
                     resources_base_path=resources_base_path,
                     dsl_location=dsl_location,
                     resolver=resolver,
                     validate_version=validate_version,
                     validate_intrinsic_function=validate_intrinsic_function,
                     validate_input_defaults=validate_input_defaults)
    return plan


def parse_from_import_blueprint(dsl_string,
                                resources_base_path,
                                dsl_location,
                                resolver):
    _, resolved_blueprint = _parse(dsl_string,
                                   resources_base_path=resources_base_path,
                                   dsl_location=dsl_location,
                                   resolver=resolver,
                                   validate_version=False,
                                   validate_intrinsic_function=False,
                                   validate_input_defaults=False)
    return resolved_blueprint


def _parse(dsl_string,
           resources_base_path,
           dsl_location=None,
           resolver=None,
           validate_version=True,
           validate_intrinsic_function=True,
           additional_resource_sources=(),
           validate_input_defaults=True):
    """

    :param dsl_string: parsing input in form of a string.
    :param resources_base_path: resources base path.
    :param dsl_location: yaml location.
    :param resolver: resolver.
    :param validate_version: whether to validate version of the DSL.
    :param validate_intrinsic_function: whether to validate intrinsic
    functions.
    :param additional_resource_sources: additional resource sources.
    :param validate_input_defaults: whether to validate given input constraints
    on input default values.
    """
    resource_base, merged_blueprint_holder =\
        _resolve_blueprint_imports(dsl_location, dsl_string, resolver,
                                   resources_base_path, validate_version)
    resource_base = [resource_base]
    if additional_resource_sources:
        resource_base.extend(additional_resource_sources)

    # parse blueprint
    plan = parser.parse(
        value=merged_blueprint_holder,
        inputs={
            'resource_base': resource_base,
            'validate_version': validate_version
        },
        element_cls=blueprint.Blueprint)
    if validate_input_defaults:
        constraints.validate_input_defaults(plan)
    if validate_intrinsic_function:
        functions.validate_functions(plan)
    plan['requirements'] = functions.find_requirements(plan)
    return plan, merged_blueprint_holder


def _resolve_blueprint_imports(dsl_location,
                               dsl_string,
                               resolver,
                               resources_base_path,
                               validate_version):
    """
    Goes over all the blueprint's imports and constructs a merged blueprint
    from them.
    """
    parsed_dsl_holder = utils.load_yaml(raw_yaml=dsl_string,
                                        error_message='Failed to parse DSL',
                                        filename=dsl_location)
    if not resolver:
        resolver = DefaultImportResolver()
    # validate version schema and extract actual version used
    result = parser.parse(
        parsed_dsl_holder,
        element_cls=blueprint.BlueprintVersionExtractor,
        inputs={
            'validate_version': validate_version
        },
        strict=False)
    version = result['plan_version']
    # handle imports
    result = parser.parse(
        value=parsed_dsl_holder,
        inputs={
            'main_blueprint_holder': parsed_dsl_holder,
            'resources_base_path': resources_base_path,
            'blueprint_location': dsl_location,
            'version': version,
            'resolver': resolver,
            'validate_version': validate_version
        },
        element_cls=blueprint.BlueprintImporter,
        strict=False)

    return result['resource_base'], \
        result['merged_blueprint']
