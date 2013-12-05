#!/usr/bin/env python

import os

if os.environ.get('USE_SETUPTOOLS'):
  from setuptools import setup
  setup_kwargs = dict(zip_safe=0)

else:
  from distutils.core import setup
  setup_kwargs = dict()

setup(
  name='graphitedata',
  version='0.1',
  url='https://github.com/jbooth/graphite-data',
  author='Jay Booth',
  author_email='jaybooth@gmail.com',
  license='Apache Software License 2.0',
  description='Pluggable persistence layer for Graphite',
  packages=['graphitedata','graphitedata/hbase'],
  **setup_kwargs
)
