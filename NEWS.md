# dwtools 0.8.3

[x] new get.timing(), trunc.timing() functions for in-memory logs
[x] change in timing (non-db) approach
  [x] no more 'timing' attribute
  [x] a lot of code simplification
[ ] shinyDW hierarchy browsing using
  [x] denormalized data
  [ ] star schema query using joinbyv
  [ ] db denormalized
  [ ] db star, denormalize SQL side
[x] db - simplify code
[x] dbCopy - update for new timing
[x] updated tests, examples

# dwtools 0.8.2

[x] shinyDW supports build hierarchy
[x] build hierarchy
[x] data.equal.data.table
[x] anonymize
[x] improved db timing

# dwtools 0.8.0

[x] db simplified interface
  [x] tested on csv (default), SQLite, postgres
  [ ] should works on any DBI/ODBC driver (TO DO test ODBC)
  [x] setkey by col names or col positions
  [x] timing for vectorized args
  [x] auto timing
[x] dbCopy tables migration wrapper
  [x] batch copy tables between connections
[x] joinbyv - batch join tables
[x] populate star schema data
[x] idxv - DT binary search on multiple keys
[x] timing function including logging to db
[x] as.xts.data.table conversion method and reverse
[x] vwap - tick data to OHLC and VWAP
[x] pkgsVersion handy wrapper to compare packages version across libraries
