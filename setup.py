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

from setuptools import setup, find_packages

install_requires = [
    'PyYAML==3.10',
    'networkx==1.9.1',
    'requests>=2.7.0,<3.0.0',
    'retrying==1.3.3',
    'pika==0.11.2',
    'proxy_tools==0.1.0',
    'bottle==0.12.7',
    'jinja2==2.10',
    'requests_toolbelt',
    'pysnmp==4.4.5',
    'opentracing_instrumentation==2.4.3',
    'jaeger-client==3.11.0'
]

try:
    from collections import OrderedDict  # NOQA
except ImportError, e:
    install_requires.append('ordereddict==1.1')

try:
    import importlib  # NOQA
except ImportError:
    install_requires.append('importlib')

try:
    import argparse  # NOQA
except ImportError as e:
    install_requires.append('argparse==1.2.2')


setup(
    name='cloudify-common',
    version='4.5',
    author='Cloudify',
    author_email='cosmo-admin@cloudify.co',
    packages=find_packages(),
    include_package_data=True,
    license='LICENSE',
    description='Cloudify Common',
    zip_safe=False,
    install_requires=install_requires,
    entry_points={
            'console_scripts': [
                'ctx = cloudify.proxy.client:main',
            ]
    },
    package_data={'cloudify.ctx_wrappers': ['ctx.py']},
    scripts=[
        'ctx_wrappers/ctx-sh'
    ]
)
