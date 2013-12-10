import json

from thrift.transport import TSocket

from graphitedata.hbase.ttypes import *
from graphitedata.hbase.Hbase import Client


# we manage a namespace table (NS) and then a data table (data)

# the NS table is organized to mimic a tree structure, with a ROOT node containing links to its children.as
# Nodes are either a BRANCH node which contains multiple child columns prefixed with c_, or a LEAF node
# containing a single INFO column

# IDCTR
#   - unique id counter

# ROOT
#   - c_branch1 -> m_branch1
#   - c_leaf1 -> m_leaf1

# m_branch1
#   - c_leaf2 -> m_branch1.leaf2

# m_leaf1
#    - INFO -> info json

# m_branch1.leaf2
#    - INFO -> info json

# the INFO json on branch nodes contains graphite info plus an ID field, consisting of a 32bit int

# we then maintain a data table with keys that are a compound of metric ID + unix timestamp for 8 byte keys



class HbaseTSDB:
    __slots__ = ('transport','client','metaTable','dataTable')

    def __init__(self, host,port,table_prefix):
        # set up client
        self.metaTable = table_prefix + "META"
        self.dataTable = table_prefix + "DATA"
        socket = TSocket.TSocket(host, port)
        self.transport = TTransport.TBufferedTransport(socket)
        protocol = TBinaryProtocol.TBinaryProtocol(self.transport)
        self.client = Client(protocol)
        self.transport.open()
        # ensure both our tables exist
        tables = self.client.getTableNames()
        if self.metaTable not in tables:
            self.client.createTable(self.metaTable,[ColumnDescriptor("cf:")])
            # add counter record
            self.client.atomicIncrement(self.metaTable,"CTR","cf:CTR",1)
        if self.dataTable not in tables:
            self.client.createTable(self.dataTable,[ColumnDescriptor("cf:")])


    # returns info for the underlying db (including 'aggregationMethod')

    # info returned in the format
    #info = {
    #  'aggregationMethod' : aggregationTypeToMethod.get(aggregationType, 'average'),
    #  'maxRetention' : maxRetention,
    #  'xFilesFactor' : xff,
    #  'archives' : archives,
    #}
    # where archives is a list of
    # archiveInfo = {
    #  'offset' : offset,
    #  'secondsPerPoint' : secondsPerPoint,
    #  'points' : points,
    #  'retention' : secondsPerPoint    * points,
    #  'size' : points * pointSize,
    #}
    #

    def info(self, metric):
        # info is stored as serialized map under META#METRIC
        key = "m_" + metric
        result = self.client.get(self.metaTable, "m_" + metric, "cf:INFO", None)
        if len(result) == 0:
            raise Exception("No metric " + metric)
        return json.loads(result[0].value)

    # aggregationMethod specifies the method to use when propogating data (see ``whisper.aggregationMethods``)
    # xFilesFactor specifies the fraction of data points in a propagation interval that must have known values for a propagation to occur.  If None, the existing xFilesFactor in path will not be changed
    def setAggregationMethod(self, metric, aggregationMethod, xFilesFactor=None):
        currInfo = self.info(metric)
        currInfo['aggregationMethod'] = aggregationMethod
        currInfo['xFilesFactor'] = xFilesFactor

        infoJson = json.dumps(currInfo)
        put = Put(row="m_"+metric, columnValues=[ColumnValue(family=self.cf,qualifier="INFO",value=infoJson)])
        result = self.client.put(self.table,put)
        return


    # archiveList is a list of archives, each of which is of the form (secondsPerPoint,numberOfPoints)
    # xFilesFactor specifies the fraction of data points in a propagation interval that must have known values for a propagation to occur
    # aggregationMethod specifies the function to use when propogating data (see ``whisper.aggregationMethods``)
    def create(self, metric, archiveConfig, xFilesFactor, aggregationMethod, isSparse, doFallocate):
        # first get our unique ID
        newId = self.client.atomicIncrement(self.metaTable,"CTR","cf:CTR",1)

        # then write the metanode
        info = {
            'aggregationMethod' : aggregationMethod,
            'maxRetention' : 21,
            'xFilesFactor' : xFilesFactor,
            'archives' : archiveConfig,
            'id': newId
        }
        self.client.mutateRow(self.metaTable,"m_" + metric,[Mutation(column="cf:INFO",value=json.dumps(info))],None)
        # finally, ensure links exist
        metric_parts = metric.split('.')
        priorParts = ""
        for part in metric_parts:
            metricParentKey,metricKey = ""
            # if parent is empty, special case for root
            if priorParts == "":
                metricParentKey = "ROOT"
                metricKey = "m_" + part
                priorParts = part
            else:
                metricParentKey = "m_" + priorParts
                metricKey = "m_" + priorParts + "." + part
                priorParts += "." + part

            # make sure parent of this node exists and is linked to us
            parentLink = self.client.get(self.metaTable,metricParentKey,"cf:" + part,None)
            if len(parentLink) == 0:
                self.client.mutateRow(self.metaTable,metricParentKey,[Mutation(column="cf:"+part,value=metricKey)],None)


    # datapoints is a list of (timestamp,value) points
    def update_many(self, metric, datapoints):
        pass

    def exists(self,metric):
        return len(self.client.get(self.metaTable,"m_" + metric,"cf:INFO",None)) > 0


    # fromTime is an epoch time
    # untilTime is also an epoch time, but defaults to now.
    #
    # Returns a tuple of (timeInfo, valueList)
    # where timeInfo is itself a tuple of (fromTime, untilTime, step)
    # Returns None if no data can be returned
    def fetch(self,metric,startTime,endTime):
        pass

    # returns [ start, end ] where start,end are unixtime ints
    def get_intervals(self,metric):
        pass

    # returns list of metrics as strings
    def find_nodes(self,query):
        # break query into parts
        clean_pattern = query.pattern.replace('\\', '')
        pattern_parts = clean_pattern.split('.')

        # walk the nodes in our namespace table
        pass


    def _find_paths(self, current_node, patterns):
        """Recursively generates absolute paths whose components underneath current_dir
        match the corresponding pattern in patterns"""
        pattern = patterns[0]
        patterns = patterns[1:]

        nodeRow = self.client.getRow(self.metaTable,current_node,None)
        if len(nodeRow) == 0:
            return



        subdirs = nodeRow[0].columns
        subdirs = []
        for k,v in nodeRow[0].columns:
            subdirs.append(k.split(":")[1])

        matching_subdirs = match_entries(subdirs,pattern)

        if patterns: # we've still got more directories to traverse
            for subdir in matching_subdirs:

        else:

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

def NewHbaseTSDB(arg="localhost:9090:graphite_"):
    host,port,prefix = arg.split(":")
    return HbaseTSDB(host,port,prefix)

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