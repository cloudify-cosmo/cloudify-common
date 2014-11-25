#########
# Copyright (c) 2014 GigaSpaces Technologies Ltd. All rights reserved
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


from setuptools import setup

install_requires = [
    'cloudify-plugins-common==3.1',
    'bottle==0.12.7'
]

try:
    import argparse  # NOQA
except ImportError, e:
    install_requires.append('argparse==1.2.2')

setup(
    name='cloudify-script-plugin',
    version='3.1',
    author='Gigaspaces',
    author_email='cloudify@gigaspaces.com',
    packages=['script_runner'],
    description='Plugin for running scripts',
    install_requires=install_requires,
    license='LICENSE',
    entry_points={
        'console_scripts': [
            'ctx = script_runner.ctx_proxy:main',
            'ctx-server = script_runner.ctx_server:main'
        ]
    }
)
