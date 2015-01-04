suppressPackageStartupMessages(library(dwtools))

# populate DT
DT = dw.populate(N=1e5, scenario="fact")

# classic time measurement
system.time(
  DT[,lapply(.SD,sum),by=list(geog_code,time_code,curr_code),.SDcols=c("amount","value")]
)

# timing to R session memory
options("dwtools.timing.conn.name"=NULL) # default
r = timing(
  DT[,lapply(.SD,sum),by=list(geog_code,time_code,curr_code),.SDcols=c("amount","value")],
  in.n = nrow(DT)
)
print(r)
get.timing() # trunc expression field
get.timing(FALSE) # return full expression field
get.timing(TRUE) # omit expr field

# some more timing
invisible(sapply(1:3/10, function(time) timing(Sys.sleep(time))))
get.timing()
# timing and keep parameters value instead of variable symbol
invisible(sapply(3:1/10, function(time) eval(bquote(timing(Sys.sleep(.(time)))))))
get.timing()
trunc.timing() # clear in-memory timing logs
get.timing()

# timing to db
library(RSQLite)
sqlite1 = list(drvName="SQLite",dbname="sqlite1.db")
sqlite1$conn = dbConnect(SQLite(), dbname=sqlite1$dbname)
options("dwtools.db.conns"=list(sqlite1=sqlite1))
options("dwtools.timing.conn.name"="sqlite1")
#options("dwtools.timing.name"="dwtools_timing") # default, timing table name
r = timing(
  DT[,lapply(.SD,sum),by=list(geog_code,time_code,curr_code),.SDcols=c("amount","value")],
  nrow(DT)
)
get.timing() # no in-memory logs
db("dwtools_timing") # query timing log from db

# timing db function, scalar
r = timing(db(DT, "sales"), nrow(DT))
db("dwtools_timing")
db("sales")
db("DROP TABLE sales")

# timing vectorized db function
r = db(DT, c("sales","sales_20141211"),timing=TRUE) # insert DT to two tables, including timing
db("dwtools_timing")

# auto timing, supported functions: db, build_hierarchy, dbCopy
options("dwtools.timing"=TRUE)
r = db(DT, "sales")
db("dwtools_timing")

# timing logs to in-memory (still by auto timing option)
options("dwtools.timing.conn.name"=NULL)
r = db(DT, c("sales","sales_20141211")) # insert DT to two tables
get.timing()

## clean up

dbDisconnect(sqlite1$conn)
file.remove(sqlite1$dbname)
options("dwtools.db.conns"=NULL,"dwtools.timing.conn.name"=NULL,"dwtools.timing"=FALSE)
