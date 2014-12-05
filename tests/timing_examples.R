suppressPackageStartupMessages(library(data.table))
library(dwtools)
options("dwtools.verbose"=3)  # turn on status messages printed to console

# populate DT
DT = dw.populate(N=1e6, scenario="fact")

# classic time measurement
system.time(
  DT[,lapply(.SD,sum),by=list(geog_code,time_code,curr_code),.SDcols=c("amount","value")]
  )

# timing as result attribute
r = timing(
  DT[,lapply(.SD,sum),by=list(geog_code,time_code,curr_code),.SDcols=c("amount","value")],
  nrow(DT)
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

dbDisconnect(sqlite1$conn)
file.remove(sqlite1$dbname)
options("dwtools.db.conns"=NULL) # reset dwtools.db.conns option
options("dwtools.timing.conn.name"=NULL)
