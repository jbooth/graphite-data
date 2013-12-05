#!/usr/bin/env python

import os
import platform
from glob import glob

if os.environ.get('USE_SETUPTOOLS'):
  from setuptools import setup
  setup_kwargs = dict(zip_safe=0)

else:
  from distutils.core import setup
  setup_kwargs = dict()


# If we are building on RedHat, let's use the redhat init scripts.
setup(
  name='graphitedata',
  version='0.1',
  url='https://github.com/jbooth/graphite-data',
  author='Jay Booth',
  author_email='jaybooth@gmail.com',
  license='Apache Software License 2.0',
  description='Pluggable persistence layer for Graphite',
  packages=['graphitedata','graphitedata/hbase'],
  package_dir={'':'lib'},
  **setup_kwargs
)
