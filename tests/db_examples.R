suppressPackageStartupMessages(library(data.table))
library(dwtools)
options("dwtools.verbose"=3)  # turn on status messages printed to console

# Setup db connections --------------------------------------------------------------------

##### define your connections
# csv and SQLite works out of the box without configuration outside of R
# soon should be supported postgres, RODBC, they may work already but were not tested.

library(RSQLite) # install.packages("RSQLite")
sqlite1 = list(drvName="SQLite",dbname="sqlite1.db")
sqlite1$conn = dbConnect(SQLite(), dbname=sqlite1$dbname)
sqlite2 = list(drvName="SQLite",dbname="sqlite2.db")
sqlite2$conn = dbConnect(SQLite(), dbname=sqlite2$dbname)
sqlite3 = list(drvName="SQLite",dbname="sqlite3.db")
sqlite3$conn = dbConnect(SQLite(), dbname=sqlite3$dbname)
library(data.table)
csv1 = list(drvName = "csv")

# configure connections
options("dwtools.db.conns"=list(sqlite1=sqlite1,sqlite2=sqlite2,sqlite3=sqlite3,csv1=csv1))

# library(RPostgreSQL) # install.packages("RPostgreSQL")
# psql <- list(drvName="PostgreSQL", host="localhost", port="5432", dbname="", user="", schema="")
# psql$conn <- dbConnect(drv=psql$drvName, host=psql$host, port=psql$port, dbname=psql$dbname, user=psql$user, password="")
# library(RMySQL) # install.packages("RMySQL")
# mysql1 = list(drvName = "MySQL", dbname = "",user = "",pass = "",conn = "")
# library(RODBC) # install.packages("RODBC")
# odbc1 <- list(drvName="ODBC", user="usr", dbname="dbn", dsn = "mydsn")
# odbc1$conn <- odbcConnect(dsn = odbc1$dsn, uid=odbc1$user, pwd="mypass")

# Basic usage --------------------------------------------------------------------

(DT = dw.populate(scenario="fact")) # fact table

### write, aka INSERT + CREATE TABLE

db(DT,"my_tab1") # write to db, using default db connection (first in list)
db(DT,"my_tab2","sqlite2") # WRITE to my_tab_alt to sqlite2 connection
db(DT,"my_tab1.csv","csv1") # WRITE to my_tab1.csv
r1 = db(DT) # write to auto named table in default db connection (first in list)
attr(r1,'tablename',TRUE) # auto generated table name # ?auto.table.name
r2 = db(DT,NULL,"sqlite2") # the same above but another connection
attr(r2,'tablename',TRUE)
l = db(DT,c("my_tab11","my_tab22"),c("sqlite1","sqlite2")) # save into different connections and tables
sapply(l, function(x) attr(x,"tablename",TRUE))

### read, aka: SELECT * FROM 

db("my_tab1")
db("my_tab2","sqlite2")
db("my_tab1.csv","csv1") # READ from my_tab1.csv
r1 = db("my_tab1","sqlite1",key=c("prod_code","cust_code","state_code","date_code")) # set key on result, useful on chaining, see 'Chaining data.table' examples below
str(r1) # keys
db(DT, "my_tab2") # CREATE TABLE just for below line example
l = db("my_tab2", c("sqlite1","sqlite2")) # read my_tab2 table from two connections, return list, no `key` supported! #PR welcome
str(l)
l = db(c("my_tab11","my_tab22"), c("sqlite1","sqlite2")) # read my_tab1 and my_tab2 table from two connections, return list, no `key`
str(l)

### get, aka: SELECT ... FROM ... JOIN ...

db("SELECT * FROM my_tab1")
r = db("SELECT * FROM my_tab2","sqlite2",key=c("prod_code","cust_code","state_code","date_code"))
str(r)
l = db(c("SELECT * FROM my_tab1","SELECT * FROM my_tab2"),c("sqlite1","sqlite2"))
str(l)

### send, aka: UPDATE, INDEX, DROP, etc.

db(c("CREATE INDEX idx_my_tab1a ON my_tab1 (prod_code, state_code)","CREATE INDEX idx_my_tab1b ON my_tab1 (cust_code, date_code)")) # create two indices
db(c("DROP INDEX idx_my_tab1a","DROP INDEX idx_my_tab1b")) # drop two indices
db("DROP TABLE my_tab2") # drop the table which we created in above example #CREATE TABLE
db(c("DROP TABLE my_tab1","DROP TABLE my_tab2"),c("sqlite1","sqlite2")) # multiple statements into multiple connections

# Advanced usage ------------------------------------------------------

(DT = dw.populate(scenario="fact")) # fact table

### easy sql scripting: DROP ALL TABLES IN ALL DBs

options("dwtools.verbose"=3)
db.conns.names = c("sqlite1","sqlite2","sqlite3")

# populate 2 tables in sqlite3 while chaining: db(DT,NULL,"sqlite")
DT[,db(.SD,NULL,"sqlite3")][,db(.SD,NULL,"sqlite3")]

# populate 2 tables in each of connection, 6 tables created
DT[,{db(.SD,NULL,db.conns.names); .SD}][,{db(.SD,NULL,db.conns.names); .SD}]

# query all tables on all connections
(tbls = db("SELECT name FROM sqlite_master WHERE type='table'",db.conns.names))

# drop tables
ll = lapply(1:length(tbls), function(i, tbls){
  if(nrow(tbls[[i]]) > 0) data.table(conn_name = names(tbls[i]), tbls[[i]])
  else data.table(conn_name = character(), tbls[[i]])
}, tbls)
r = rbindlist(ll)[,list(sql=paste0("DROP TABLE ",name), conn_name=conn_name) # build statement
                  ][,db(sql,conn_name) # exec DROP TABLE ...
                    ]
# verify tables dropped
db("SELECT name FROM sqlite_master WHERE type='table'",db.conns.names)

### Chaining data.table: DT[...][...]

# populate star schema
dw.star = dw.populate(scenario="star schema") # list of 5 tables, 1 fact table and 4 dimensions
db(dw.star$time,"time") # save time to db
db(dw.star$geography,"geography") # save geography to db
db(dw.star$sales,"sales") # save sales FACT to db

# data.table join in R directly on external SQL database
db("geography",key="state_code")[db("sales",key="state_code")] # geography[sales]

## chaining including read and write directly on SQL database
# 0. predefine aggregate function for later use
# 1. query sales fact table from db
# 2. aggregate to 2 dimensions
# 3. save current state of data to db
# 4. query geography dimension table from db
# 5. left join geography dimension
# 6. aggregate to higher geography entity
# 7. save current state of data to db
# 8. query time dimension table from db
# 8. left join time dimension
# 9. aggregate to higher time entity
# 10. save current state of data to db
jj_aggr = quote(list(quantity=sum(quantity), value=sum(value)))
db("sales",key="state_code" # read fact table from db
   )[,eval(jj_aggr),keyby=list(state_code, date_code) # aggr by state_code and date_code
     ][,db(.SD) # write to db, auto.table.name
       ][,db("geography",key="state_code" # read lookup geography dim from db
             )[.SD # left join geography
               ][,eval(jj_aggr), keyby=list(date_code, region_name)] # aggr
         ][,db(.SD) # write to db, auto.table.name
           ][,db("time",key="date_code" # read lookup time dim from db
                 )[.SD # left join time
                   ][, eval(jj_aggr), keyby=list(region_name, month_code, month_name)] # aggr
             ][,db(.SD) # write to db, auto.table.name
               ]
# preview newly created tables
(tbls = db("SELECT name FROM sqlite_master WHERE type='table' AND name NOT IN ('time','geography','sales')"))
if(nrow(tbls)>1) lapply(db(tbls$name),head) # query multiple tables from one connection

### Copy tables

# dbCopy multiple tables from source to target #?dbCopy
dbCopy(
  c("sales","geography","time"),"sqlite1", # source
  c("sales","geography","time"),"sqlite2"  # target
)
(tbls = db("SELECT name FROM sqlite_master WHERE type='table'","sqlite2")) # sqlite2 check

# Close connections -------------------------------------------------------

sapply(getOption("dwtools.db.conns"), function(x) dbDisconnect(x[["conn"]])) # close connection
sapply(getOption("dwtools.db.conns"), function(x) file.remove(x[["dbname"]])) # remove db files
options("dwtools.db.conns"=NULL) # reset dwtools.db.conns option
