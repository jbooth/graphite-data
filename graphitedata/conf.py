__author__ = 'jay'

import os
from os.path import realpath,join,dirname
from ConfigParser import ConfigParser

graphite_root = os.environ.get('GRAPHITE_ROOT')
if graphite_root is None:
    graphite_root = realpath(join(dirname(__file__), '..'))

graphite_conf = os.environ.get('GRAPHITE_CONF_DIR')
if graphite_conf is None:
    graphite_conf = join(graphite_root,'conf')

defaults = dict(
    GRAPHITE_ROOT=graphite_root,
    GRAPHITE_CONF_DIR=graphite_conf,
)

class Settings(dict):
  __getattr__ = dict.__getitem__

  def __init__(self,path,section):
    dict.__init__(self)
    self.update(defaults)
      self.readFrom(path,section)

  def readFrom(self, path, section):
    parser = ConfigParser()
    if not parser.read(path):
      raise Exception("Failed to read config file %s" % path)

    if not parser.has_section(section):
      return

    for key,value in parser.items(section):
      key = key.upper()

      # Detect type from defaults dict
      if key in defaults:
        valueType = type( defaults[key] )
      else:
        valueType = str

      if valueType is list:
        value = [ v.strip() for v in value.split(',') ]

      elif valueType is bool:
        value = parser.getboolean(section, key)

      else:
        # Attempt to figure out numeric types automatically
        try:
          value = int(value)
        except:
          try:
            value = float(value)
          except:
            pass

      self[key] = value




settings = ConfigParser.read(join(graphite_conf,'graphite-db.conf'))
