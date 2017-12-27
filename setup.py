"""
Copyright (c) 2017 Jet Propulsion Laboratory,
California Institute of Technology.  All rights reserved
"""
from setuptools import setup, find_packages

__version__ = '0.1'

setup(
    name="ningesterpy",
    version=__version__,
    url="https://github.jpl.nasa.gov/thuang/nexus",

    author="Team Nexus",

    description="Python modules that can be used for NEXUS ingest.",
    # long_description=open('README.md').read(),

    packages=find_packages(),
    test_suite="tests",
    platforms='any',

    classifiers=[
        'Development Status :: 1 - Pre-Alpha',
        'Intended Audience :: Developers',
        'Operating System :: OS Independent',
        'Programming Language :: Python :: 3.5',
    ]
)
