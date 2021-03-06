import os
from os.path import exists,sep, join, dirname
import time
import whisper
from tsdb import TSDB


def is_escaped_pattern(s):
    for symbol in '*?[{':
        i = s.find(symbol)
        if i > 0:
            if s[i - 1] == '\\':
                return True
    return False


def find_escaped_pattern_fields(pattern_string):
    pattern_parts = pattern_string.split('.')
    for index, part in enumerate(pattern_parts):
        if is_escaped_pattern(part):
            yield index

class WhisperTSDB(TSDB):
    __slots__ = ('dataDir')

    def __init__(self, dataDir):
        self.dataDir = dataDir


    def getFilesystemPath(self, metric):
        metric_path = metric.replace('.', sep).lstrip(sep) + '.wsp'
        return join(self.dataDir, metric_path)

    def info(self, metric):
        return whisper.info(self.getFilesystemPath(metric))

    def setAggregationMethod(self, metric, aggregationMethod, xFilesFactor=None):
        return whisper.setAggregationMethod(self.getFilesystemPath(metric), aggregationMethod, xFilesFactor)

    def create(self, metric, archiveConfig, xFilesFactor=None, aggregationMethod=None, sparse=False,
               useFallocate=False):
        dbFilePath = self.getFilesystemPath(metric)
        dbDir = dirname(dbFilePath)

        try:
            if not (os.path.exists(dbDir)):
                os.makedirs(dbDir, 0755)
        except Exception as e:
            print("Error creating dir " + dbDir + " : " + e)
        return whisper.create(dbFilePath, archiveConfig, xFilesFactor, aggregationMethod, sparse, useFallocate)

    def update_many(self, metric, datapoints):
        return whisper.update_many(self.getFilesystemPath(metric), datapoints)

    def exists(self, metric):
        return exists(self.getFilesystemPath(metric))

    def fetch(self, metric, startTime, endTime):
        return whisper.fetch(self.getFilesystemPath(metric), startTime, endTime)

    def get_intervals(self, metric):
        filePath = self.getFilesystemPath(metric)
        start = time.time() - whisper.info(filePath)['maxRetention']
        end = max(os.stat(filePath).st_mtime, start)
        return [start, end]

    def find_nodes(self, query):
        from graphite.finders.standard import StandardFinder
        finder = StandardFinder([self.dataDir])
        for n in finder.find_nodes(query):
            yield n
