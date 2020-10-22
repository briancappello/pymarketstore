#!/usr/bin/env python

import ast
import re
from setuptools import setup, find_packages

_version_re = re.compile(r'__version__\s+=\s+(.*)')

with open('pymarketstore/__init__.py', 'rb') as f:
    version = str(ast.literal_eval(_version_re.search(
        f.read().decode('utf-8')).group(1)))


with open('README.md') as readme_file:
    README = readme_file.read()


setup(
    name='pymarketstore',
    version=version,
    description='Marketstore python driver',
    long_description=README,
    long_description_content_type='text/markdown',
    author='Alpaca',
    author_email='oss@alpaca.markets',
    url='https://github.com/alpacahq/pymarketstore',
    keywords='database,pandas,financial,timeseries',
    packages=find_packages(exclude=('tests', 'docs')),
    zip_safe=False,
    install_requires=[
        'msgpack',
        'numpy',
        'requests',
        'pandas',
        'urllib3',
        'websocket-client',
        'protobuf>=3.13',
        'grpcio>=1.32.0',
    ],
    extras_require={
        'dev': [
            'pytest',
            'pytest-cov',
            'coverage>=4.4.1',
            'mock>=1.0.1',
            'grpcio-tools',
        ],
    },
    setup_requires=['pytest-runner', 'flake8'],
)
