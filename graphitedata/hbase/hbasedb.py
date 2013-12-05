import json

from thrift.transport import TSocket

from graphitedata.hbase.ttypes import *


# we store all TS data in a single CF on the graphite-data cluster
# TS datapoints are stored with key METRIC#UNIXTIME
# TS metadata is stored with META#METRIC
from graphitedata.hbase import THBaseService


class HbaseTSDB:
    __slots__ = ('transport','client','table','cf')

    def __init__(self, host,port,table,cf):
        self.table = table
        self.cf = cf
        socket = TSocket.TSocket(host, port)
        self.transport = TTransport.TBufferedTransport(socket)
        protocol = TBinaryProtocol.TBinaryProtocol(self.transport)
        self.client = client = THBaseService.Client(protocol)
        self.transport.open()

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
        key = "META#" + metric
        get = TGet(row=key)
        result = self.client.get(self.table,get)
        return json.loads(result["META"])

    # aggregationMethod specifies the method to use when propogating data (see ``whisper.aggregationMethods``)
    # xFilesFactor specifies the fraction of data points in a propagation interval that must have known values for a propagation to occur.  If None, the existing xFilesFactor in path will not be changed
    def setAggregationMethod(self, metric, aggregationMethod, xFilesFactor=None):
        currInfo = self.info(metric)
        currInfo['aggregationMethod'] = aggregationMethod
        currInfo['xFilesFactor'] = xFilesFactor
        infoJson = json.dumps(currInfo)
        put = TPut(row="META#"+metric, columnValues=[TColumnValue(family=self.cf,qualifier="META",value=infoJson)])
        pass

    # archiveList is a list of archives, each of which is of the form (secondsPerPoint,numberOfPoints)
    # xFilesFactor specifies the fraction of data points in a propagation interval that must have known values for a propagation to occur
    # aggregationMethod specifies the function to use when propogating data (see ``whisper.aggregationMethods``)
    def create(self, metric, archiveConfig, xFilesFactor, aggregationMethod, isSparse, doFallocate):
        #create meta node
        info = {
            'aggregationMethod' : aggregationMethod,
            'maxRetention' : 21,
            'xFilesFactor' : xFilesFactor,
            'archives' : archives,

        }

        # ensure links exist

        pass


    # datapoints is a list of (timestamp,value) points
    def update_many(self, metric, datapoints):
        pass

    def exists(self,metric):
        pass


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

        # walk the nodes in hbase by part

        pass

def NewHbaseTSDB(arg="localhost:9090:graphite:graphite"):
    host,port,table,cf = arg.split(":")
    return HbaseTSDB(host,port,table,cf)
