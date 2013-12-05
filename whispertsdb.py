import os
from os.path import sep, join, dirname, basename, isdir, islink, realpath, isfile
import time
import fnmatch

from node import BranchNode, LeafNode


def is_escaped_pattern(s):
  for symbol in '*?[{':
    i = s.find(symbol)
    if i > 0:
      if s[i-1] == '\\':
        return True
  return False

def find_escaped_pattern_fields(pattern_string):
  pattern_parts = pattern_string.split('.')
  for index,part in enumerate(pattern_parts):
    if is_escaped_pattern(part):
      yield index

class WhisperTSDB:
  __slots__ = ('dataDir')
  def __init__(self, dataDir):
    self.dataDir = dataDir



  def getFilesystemPath(self, metric):
    metric_path = metric.replace('.',sep).lstrip(sep) + '.wsp'
    return join(self.dataDir, metric_path)

  def info(self,metric):
    return whisper.info(self.getFilesystemPath(metric))

  def setAggregationMethod(self,metric, aggregationMethod, xFilesFactor=None):
    return whisper.setAggregationMethod(self.getFilesystemPath(metric),aggregationMethod,xFilesFactor)

  def create(self,metric,archiveConfig,xFilesFactor=None,aggregationMethod=None,sparse=False,useFallocate=False):
    dbFilePath = self.getFilesystemPath(metric)
    dbDir = dirname(dbFilePath)
    os.makedirs(dbDir, 0755)
    return whisper.create(dbFilePath, archiveConfig,xFilesFactor,aggregationMethod,sparse,useFallocate)

  def update_many(self,metric,datapoints):
    return whisper.update_many(self.getFilesystemPath(metric), datapoints)

  def exists(self,metric):
    return whisper.exists(self.getFilesystemPath(metric))

  def fetch(self,metric,startTime,endTime):
    return whisper.fetch(self.getFilesystemPath(metric),startTime,endTime)

  def get_intervals(self,metric):
    filePath = self.getFilesystemPath(metric)
    start = time.time() - whisper.info(filePath)['maxRetention']
    end = max( os.stat(filePath).st_mtime, start )
    return [start,end]

  def find_nodes(self, query):
    clean_pattern = query.pattern.replace('\\', '')
    pattern_parts = clean_pattern.split('.')

    for absolute_path in self._find_paths(self.dataDir, pattern_parts):
      if basename(absolute_path).startswith('.'):
        continue

      relative_path = absolute_path[ len(self.dataDir): ].lstrip('/')
      metric_path = fs_to_metric(relative_path)
      real_metric_path = get_real_metric_path(absolute_path, metric_path)

      metric_path_parts = metric_path.split('.')
      for field_index in find_escaped_pattern_fields(query.pattern):
        metric_path_parts[field_index] = pattern_parts[field_index].replace('\\', '')
      metric_path = '.'.join(metric_path_parts)

      # Now we construct and yield an appropriate Node object
      if isdir(absolute_path):
        yield BranchNode(metric_path)

      elif isfile(absolute_path):
        if absolute_path.endswith('.wsp'):
          yield LeafNode(metric_path, self)


  def _find_paths(self, current_dir, patterns):
    """Recursively generates absolute paths whose components underneath current_dir
    match the corresponding pattern in patterns"""
    pattern = patterns[0]
    patterns = patterns[1:]
    entries = os.listdir(current_dir)

    subdirs = [e for e in entries if isdir( join(current_dir,e) )]
    matching_subdirs = match_entries(subdirs, pattern)

    if patterns: #we've still got more directories to traverse
      for subdir in matching_subdirs:

        absolute_path = join(current_dir, subdir)
        for match in self._find_paths(absolute_path, patterns):
          yield match

    else: #we've got the last pattern
      files = [e for e in entries if isfile( join(current_dir,e) )]
      matching_files = match_entries(files, pattern + '.*')

      for basename in matching_files + matching_subdirs:
        yield join(current_dir, basename)


def fs_to_metric(path):
  dirpath = dirname(path)
  filename = basename(path)
  return join(dirpath, filename.split('.')[0]).replace('/','.')


def get_real_metric_path(absolute_path, metric_path):
  # Support symbolic links (real_metric_path ensures proper cache queries)
  if islink(absolute_path):
    real_fs_path = realpath(absolute_path)
    relative_fs_path = metric_path.replace('.', '/')
    base_fs_path = absolute_path[ :-len(relative_fs_path) ]
    relative_real_fs_path = real_fs_path[ len(base_fs_path): ]
    return fs_to_metric( relative_real_fs_path )

  return metric_path

def _deduplicate(entries):
  yielded = set()
  for entry in entries:
    if entry not in yielded:
      yielded.add(entry)
      yield entry

def match_entries(entries, pattern):
  """A drop-in replacement for fnmatch.filter that supports pattern
  variants (ie. {foo,bar}baz = foobaz or barbaz)."""
  v1, v2 = pattern.find('{'), pattern.find('}')

  if v1 > -1 and v2 > v1:
    variations = pattern[v1+1:v2].split(',')
    variants = [ pattern[:v1] + v + pattern[v2+1:] for v in variations ]
    matching = []

    for variant in variants:
      matching.extend( fnmatch.filter(entries, variant) )

    return list( _deduplicate(matching) ) #remove dupes without changing order

  else:
    matching = fnmatch.filter(entries, pattern)
    matching.sort()
    return matching
