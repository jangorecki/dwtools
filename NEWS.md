# dwtools 1.0.0 (in dev)

* db simplified interface
  * TODO: use ETLUtils package
  * tested on csv (default if db conns not provided)
  * tested on SQLite, postgres (should works on any DBI driver)
  * TODO: test ODBC
  * TODO: couchdb
* joinbyv - batch join tables
* populate star schema data
* idxv - custom precalculated DT keys
* timing function including logging to db
* as.xts method for data.table
