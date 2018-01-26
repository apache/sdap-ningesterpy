# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from setuptools import setup, find_packages
from subprocess import check_call, CalledProcessError

try:
    check_call(['conda', 'info'], stdout=None, stderr=None)
except CalledProcessError as e:
    raise EnvironmentError("This module requires conda") from e

try:
    with open('conda-requirements.txt') as f:
        conda_requirements = f.readlines()
    check_call(['conda', 'install', '-y', *conda_requirements])
except (CalledProcessError, IOError) as e:
    raise EnvironmentError("Error installing conda packages") from e

__version__ = '1.0.0-SNAPSHOT'

setup(
    name="ningesterpy",
    version=__version__,
    url="https://github.com/apache/incubator-sdap-ningesterpy",

    author="dev@sdap.apache.org",
    author_email="dev@sdap.apache.org",

    description="Python modules that can be used for NEXUS ingest.",
    long_description=open('README.rst').read(),

    packages=find_packages(exclude=["*.tests", "*.tests.*", "tests.*", "tests"]),
    test_suite="tests",
    platforms='any',

    classifiers=[
        'Development Status :: 1 - Pre-Alpha',
        'Intended Audience :: Developers',
        'Operating System :: OS Independent',
        'Programming Language :: Python :: 3.5',
    ]
)
