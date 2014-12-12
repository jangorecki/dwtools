suppressPackageStartupMessages(library(data.table))
library(dwtools)
options("dwtools.verbose"=3)  # turn on status messages printed to console

# populate DT
DT = dw.populate(N=1e4, scenario="fact")

# classic time measurement
system.time(
  DT[,lapply(.SD,sum),by=list(geog_code,time_code,curr_code),.SDcols=c("amount","value")]
  )

# timing as result attribute
r = timing(
  DT[,lapply(.SD,sum),by=list(geog_code,time_code,curr_code),.SDcols=c("amount","value")],
  nrow_in = nrow(DT)
)
print(r)
attr(r,"timing")

# timing to db
library(RSQLite)
sqlite1 = list(drvName="SQLite",dbname="sqlite1.db")
sqlite1$conn = dbConnect(SQLite(), dbname=sqlite1$dbname)
options("dwtools.db.conns"=list(sqlite1=sqlite1))
options("dwtools.timing.conn.name"="sqlite1")
#options("dwtools.timing.name"="dwtools_timing") # default value
r = timing(
  DT[,lapply(.SD,sum),by=list(geog_code,time_code,curr_code),.SDcols=c("amount","value")],
  nrow(DT)
)
print(r)
attr(r,"timing",TRUE)
db("dwtools_timing")

# timing db function, scalar
r = timing(db(DT, "sales"), nrow(DT))
#r = db(DT, "sales", "sqlite1", timing=TRUE)
db("dwtools_timing")
db("sales")
db("DROP TABLE sales")

# timing vectorized db function
r = db(DT, c("sales","sales_20141211"),timing=TRUE) # insert DT to two tables, including timing
db("dwtools_timing")

# auto timing any db calls
options("dwtools.timing"=TRUE)
r = db(DT, "sales")
db("dwtools_timing")
# for extended auto timing usage try now DT "chaining" example from ?db

# vectorized result as attribute - still by auto timing option
options("dwtools.timing.conn.name"=NULL)
r = db(DT, c("sales","sales_20141211")) # insert DT to two tables
attr(r,"timing",TRUE) # already combined

dbDisconnect(sqlite1$conn)
file.remove(sqlite1$dbname)
options("dwtools.db.conns"=NULL) # reset dwtools.db.conns option
options("dwtools.timing.conn.name"=NULL)
options("dwtools.timing"=FALSE)
