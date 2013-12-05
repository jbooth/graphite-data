from abc import ABCMeta,abstractmethod

# class DB is a generic DB layer to support graphite.  Plugins can provide an implementation satisfying the following functions
# by configuring DB_MODULE, DB_INIT_FUNC and DB_INIT_ARG

# the global variable APP_DB will be initialized as the return value of DB_MODULE.DB_INIT_FUNC(DB_INIT_ARG)
# we will throw an error if the provided value does not implement our abstract class DB below
class Node(object):
  __slots__ = ('name', 'path', 'local', 'is_leaf')

  def __init__(self, path):
    self.path = path
    self.name = path.split('.')[-1]
    self.local = True
    self.is_leaf = False

  def __repr__(self):
    return '<%s[%x]: %s>' % (self.__class__.__name__, id(self), self.path)


class BranchNode(Node):
  pass


class LeafNode(Node):
  __slots__ = ('db', 'intervals')

  def __init__(self, path, tsdb):
    Node.__init__(self, path)
    self.db = tsdb
    self.intervals = tsdb.get_intervals(path)
    self.is_leaf = True

  def fetch(self, startTime, endTime):
    return self.db.fetch(self.path, startTime, endTime)

  def __repr__(self):
    return '<LeafNode[%x]: %s >' % (id(self), self.path)

class TSDB:
    __metaclass__= ABCMeta

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
    @abstractmethod
    def info(self, metric):
        pass

    # aggregationMethod specifies the method to use when propogating data (see ``whisper.aggregationMethods``)
    # xFilesFactor specifies the fraction of data points in a propagation interval that must have known values for a propagation to occur.  If None, the existing xFilesFactor in path will not be changed
    @abstractmethod
    def setAggregationMethod(self, metric, aggregationMethod, xFilesFactor=None):
        pass

    # archiveList is a list of archives, each of which is of the form (secondsPerPoint,numberOfPoints)
    # xFilesFactor specifies the fraction of data points in a propagation interval that must have known values for a propagation to occur
    # aggregationMethod specifies the function to use when propogating data (see ``whisper.aggregationMethods``)
    @abstractmethod
    def create(self, metric, archiveConfig, xFilesFactor, aggregationMethod, isSparse, doFallocate):
        pass


    # datapoints is a list of (timestamp,value) points
    @abstractmethod
    def update_many(self, metric, datapoints):
        pass

    @abstractmethod
    def exists(self,metric):
        pass


    # fromTime is an epoch time
    # untilTime is also an epoch time, but defaults to now.
    #
    # Returns a tuple of (timeInfo, valueList)
    # where timeInfo is itself a tuple of (fromTime, untilTime, step)
    # Returns None if no data can be returned
    @abstractmethod
    def fetch(self,metric,startTime,endTime):
        pass

    # returns [ start, end ] where start,end are unixtime ints
    @abstractmethod
    def get_intervals(self,metric):
        pass