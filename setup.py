########
# Copyright (c) 2013 GigaSpaces Technologies Ltd. All rights reserved
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


from setuptools import setup


install_requires = [
    'PyYAML==3.10',
    'jsonschema==2.3.0',
    'networkx==1.8.1',
]

try:
    from collections import OrderedDict  # NOQA
except ImportError, e:
    install_requires.append('ordereddict==1.1')


setup(
    name='cloudify-dsl-parser',
    version='3.2a8',
    author='Gigaspaces',
    author_email='cosmo-admin@gigaspaces.com',
    packages=['dsl_parser',
              'dsl_parser.interfaces'],
    license='LICENSE',
    description='Cloudify DSL parser',
    zip_safe=False,
    install_requires=install_requires
)
