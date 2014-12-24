# dwtools 0.8.1

* shiny app: shinyDW supports:
  * dw.explore TO DO
* dw.explore TO DO

# dwtools 0.8.0

* db simplified interface
  * tested on csv (default), SQLite, postgres
  * should works on any DBI/ODBC driver (TO DO test ODBC)
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
* as.xts.data.table conversion method and reverse
* pkgsVersion handy wrapper to compare packages version across libraries
