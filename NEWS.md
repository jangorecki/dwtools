# dwtools 1.0.0 (in dev)

* db simplified interface
  * tested on csv (default), SQLite, postgres
  * should works on any DBI driver
  * TODO: test ODBC
  * setkey by col names or col positions
  * timing for vectorized args
  * auto timing
* dbCopy tables migration wrapper
  * batch copy tables between connections
  * support timing for vectorized args
  * auto timing
* joinbyv - batch join tables
* populate star schema data
* idxv - DT binary search on multiple keys
* timing function including logging to db
* as.xts method for data.table (to be moved to DT)
