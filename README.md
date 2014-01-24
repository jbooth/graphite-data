graphite-data
=============

Pluggable data layer for graphite to support hbase and other stores.

Each store implements the interface outlined in abstract class TSDB.

Setup should be as easy as

    python setup.py install

To integrate with graphite-web, we plug in as a finder.  In settings.py, plug in

    STORAGE_FINDERS = (
        'graphitedata.HbaseDB()',
    )

or
    STORAGE_FINDERS = (
        'graphitedata.WhisperDB()',
    )

Note that you can supply a list of finders, so feel free to use the StandardFinder in addition to a plugin for your migration strategy.

To integrate with carbon, you'll have to use our forked version at github.com/ohmdata/carbon

Put the following line in your carbon.conf

    DB_INIT_FUNC=graphitedata.HbaseDB

or

    DB_INIT_FUNC=graphitedata.WhisperDB