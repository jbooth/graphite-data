__author__ = 'jay'


graphite_root = os.environ.get('GRAPHITE_ROOT')
if graphite_root is None:


graphite_root = kwargs.get("ROOT_DIR")
if graphite_root is None:
if graphite_root is None:
    raise CarbonConfigException("Either ROOT_DIR or GRAPHITE_ROOT "
                     "needs to be provided.")