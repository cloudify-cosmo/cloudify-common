from setuptools import setup, find_packages

setup(
    name='cloudify-common',
    version='7.0.0.dev1',
    author='Cloudify',
    author_email='cosmo-admin@cloudify.co',
    packages=find_packages(exclude=('cloudify.tests*',
                                    'cloudify_rest_client.tests*',
                                    'dsl_parser.tests*',
                                    'script_runner.tests*',)),
    include_package_data=True,
    license='LICENSE',
    description='Cloudify Common',
    zip_safe=False,
    install_requires=[
        'proxy_tools==0.1.0',
        'bottle==0.12.23',
        'jinja2>3,<4',
        'requests_toolbelt==0.9.1',
        'wagon>0.10',
        'pytz==2021.3',
        'pika==1.1.0',
        'requests>=2.27.1,<3.0.0',
        'fasteners==0.17.3',
        'aiohttp==3.7.4.post0',
    ],
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
            'PyYAML==6.0',
            'networkx==1.11',
        ],
        'snmp': [
            'pysnmp==4.4.5'
        ]
    }
)
