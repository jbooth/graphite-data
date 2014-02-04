graphite-data
=============

Pluggable data layer for graphite to support hbase and other stores.

Each store implements the interface outlined in abstract class TSDB.

Setup should be as easy as

    git clone https://github.com/jbooth/graphite-data.git
    cd graphite-data
    python setup.py install
    cp graphite-db.conf.example /opt/graphite/conf/graphite-db.conf

To integrate with graphite-web, we plug in as a finder.  In settings.py, plug in

    STORAGE_FINDERS = (
        'graphitedata.plugin.HbaseDB',
    )

or

    STORAGE_FINDERS = (
        'graphitedata.plugin.WhisperDB',
    )

Note that you can supply a list of finders, so feel free to use the StandardFinder in addition to a plugin for your migration strategy.

To integrate with carbon, you'll have to use our forked version at github.com/ohmdata/carbon

Put the following line in the [cache] section of your carbon.conf file (should be the top section of the file, see the example conf in our fork).

    DB_INIT_FUNC=graphitedata.plugin.HbaseDB

or

    DB_INIT_FUNC=graphitedata.plugin.WhisperDB
