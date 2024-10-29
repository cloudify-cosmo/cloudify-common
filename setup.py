from setuptools import setup, find_packages


setup(
    name='cloudify-common',
    version='7.0.5',
    author='Cloudify',
    author_email='cosmo-admin@cloudify.co',
    packages=find_packages(
        exclude=(
            'cloudify.tests*',
            'cloudify_rest_client.tests*',
            'dsl_parser.tests*',
            'script_runner.tests*',
        )
    ),
    include_package_data=True,
    license='LICENSE',
    description='Cloudify Common',
    zip_safe=False,
    install_requires=[
        'aiohttp==3.10.5',
        'bottle<1',
        'distro>=1.7.0,<2',
        'fasteners<1',
        'jinja2>=3.1.4,<4',
        'pika<2',
        'proxy_tools<1',
        'pytz',
        'urllib3>=2.0.7',
        'requests>=2.32.0,<3',
        'requests_toolbelt>=1,<2',
        'wagon>=1,<2',
    ],
    python_requires='>=3.6',
    entry_points={
        'console_scripts': [
            'ctx = cloudify.proxy.client:main',
        ]
    },
    package_data={'cloudify.ctx_wrappers': ['ctx.py']},
    scripts=[
        'ctx_wrappers/ctx-sh',
    ],
    extras_require={
        # for running workflows (in the mgmtworker and the cli), as opposed
        # to e.g. just executing operations (in the agent)
        'dispatcher': [
            'PyYAML>6,<7',
            'networkx>2,<3',
        ],
        'snmp': [
            'pysnmp>4,<5',
        ]
    }
)
