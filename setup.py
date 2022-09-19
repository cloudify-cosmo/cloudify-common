########
# Copyright (c) 2013-2019 Cloudify Platform Ltd. All rights reserved
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

import sys
from setuptools import setup, find_packages

install_requires = [
    'retrying==1.3.3',
    'proxy_tools==0.1.0',
    'bottle==0.12.19',
    'jinja2==2.11.3',
    'requests_toolbelt==0.9.1',
    'wagon>0.10',
    'pytz==2021.3'
]

if sys.version_info[:3] < (2, 7, 9):
    install_requires += [
        'pika==0.11.2',
        'requests==2.19.1',
        'fasteners==0.16.3',
    ]
    pyyaml_version = '5.4.1'
elif sys.version_info[:2] < (3, 6):
    install_requires += [
        'pika==1.1.0',
        'requests==2.25.1',
        'fasteners==0.16.3',
    ]
    pyyaml_version = '5.4.1'
else:
    install_requires += [
        'pika==1.1.0',
        'requests>=2.27.1,<3.0.0',
        'fasteners==0.17.3',
        'aiohttp==3.7.4.post0',
    ]
    pyyaml_version = '6.0'

try:
    from collections import OrderedDict  # NOQA
except ImportError:
    install_requires.append('ordereddict==1.1')

try:
    import importlib  # NOQA
except ImportError:
    install_requires.append('importlib')

try:
    import argparse  # NOQA
except ImportError:
    install_requires.append('argparse==1.4.0')


setup(
    name='cloudify-common',
    version='6.4.1',
    author='Cloudify',
    author_email='cosmo-admin@cloudify.co',
    packages=find_packages(exclude=('dsl_parser.tests*',
                                    'script_runner.tests*',)),
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
    ],
    extras_require={
        # for running workflows (in the mgmtworker and the cli), as opposed
        # to eg. just executing operations (in the agent)
        'dispatcher': [
            'PyYAML=={0}'.format(pyyaml_version),
            'networkx==1.11',
        ],
        # this is just a hack to allow running unittests on py26.
        # DO NOT USE THIS ANYWHERE ELSE.
        # to be removed ASAP whenever we can drop py26 agents.
        'dispatcher_py26': [
            'PyYAML==4.2b4',
            'networkx==1.9.1',
        ],
        'snmp': [
            'pysnmp==4.4.5'
        ]
    }
)
