#!/usr/bin/python
# Copyright (c) 2010-2011 OpenStack, LLC.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from setuptools import setup, find_packages

from slogging import __version__ as version


name = 'slogging'


setup(
    name=name,
    version=version,
    description='Slogging',
    license='Apache License (2.0)',
    author='OpenStack, LLC.',
    author_email='me@not.mn',
    url='https://github.com/notmyname/slogging',
    packages=find_packages(exclude=['test_slogging', 'bin']),
    test_suite='nose.collector',
    classifiers=[
        'Development Status :: 4 - Beta',
        'License :: OSI Approved :: Apache Software License',
        'Operating System :: POSIX :: Linux',
        'Programming Language :: Python :: 2.6',
        'Environment :: No Input/Output (Daemon)',
        ],
    install_requires=[],  # removed for better compat
    scripts=['bin/swift-account-stats-logger',
             'bin/swift-container-stats-logger',
             'bin/swift-log-stats-collector', 'bin/swift-log-uploader',
             'bin/swift-access-log-delivery'
        ],
    )
