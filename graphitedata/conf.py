from os.path import join
from ConfigParser import ConfigParser
from . import GRAPHITE_CONF


class Settings(dict):
  __getattr__ = dict.__getitem__

  def __init__(self,defaults, section):
    dict.__init__(self)
    self.update(defaults)
    self.readFrom(join(GRAPHITE_CONF,'graphite-db.conf'),defaults,section)

  def readFrom(self, path, defaults, section):
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
