import os
from os.path import realpath,join,dirname

GRAPHITE_ROOT = os.environ.get('GRAPHITE_ROOT')
if not GRAPHITE_ROOT:
    GRAPHITE_ROOT = '/opt/graphite'

GRAPHITE_CONF = os.environ.get('GRAPHITE_CONF_DIR')
if not GRAPHITE_CONF:
    GRAPHITE_CONF = join(GRAPHITE_ROOT,'conf')

GRAPHITE_STORAGE_DIR = os.environ.get('GRAPHITE_STORAGE_DIR')
if not GRAPHITE_STORAGE_DIR:
  GRAPHITE_STORAGE_DIR = join(GRAPHITE_ROOT, 'storage')
