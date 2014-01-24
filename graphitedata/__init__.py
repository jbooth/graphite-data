import os
import sys
from os.path import realpath,join,dirname,exists

GRAPHITE_ROOT = os.environ.get('GRAPHITE_ROOT')
if not GRAPHITE_ROOT:
    # assumes
    GRAPHITE_ROOT = realpath(join(dirname(__file__), '..'))

GRAPHITE_CONF = os.environ.get('GRAPHITE_CONF_DIR')
if not GRAPHITE_CONF:
    GRAPHITE_CONF = join(GRAPHITE_ROOT,'conf')

GRAPHITE_STORAGE_DIR = os.environ.get('GRAPHITE_STORAGE_DIR')
if not GRAPHITE_STORAGE_DIR:
  GRAPHITE_STORAGE_DIR = join(GRAPHITE_ROOT, 'storage')

GRAPHITE_WEB_DIR = join(GRAPHITE_ROOT,'webapp')
if exists(GRAPHITE_WEB_DIR):
    # add graphite-web to the pythonpath
    sys.path.insert(0, GRAPHITE_WEB_DIR)