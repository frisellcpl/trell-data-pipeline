#!/usr/bin/env python
from setuptools import find_packages, setup
from pipeline import __version__

setup(
    name='trell-data-pipeline',
    version=__version__,
    description='Pipes various ingress streams to another egress stream. I.e. MQTT -> Kafka',
    packages=find_packages(exclude=['test', 'test.*']),
    url = 'https://github.com/frisellcpl/trell-data-pipeline',
    author = 'Johan Frisell',
    author_email = 'johan@trell.se',
    include_package_data=True,
    install_requires=[
        'aiokafka==0.5.2',
        'asyncpg==0.21.0',
        'PyYAML==5.3',
        'gmqtt==0.5.5',
        'jsonschema==3.2.0',
        'aiormq==3.2.1',
        'boto3==1.12.48',
    ],
    classifiers = [
        'Intended Audience :: Developers',
        'Topic :: Utilities',
        'License :: Public Domain',
        'Natural Language :: English',
        'Operating System :: OS Independent',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
    ],
    extras_require={
        'dev': [
            'pytest==2.9.2',
        ],
    },
    zip_safe=True
)
