Swift stats system
==================

The swift stats system is composed of three parts parts: log creation, log
uploading, and log processing. The system handles two types of logs (access
and account stats), but it can be extended to handle other types of logs.

How to Build to Debian Packages
===============================

    python setup.py --command-packages=stdeb.command bdist_deb
