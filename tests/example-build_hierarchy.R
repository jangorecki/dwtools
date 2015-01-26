suppressPackageStartupMessages(library(dwtools))

## simple 2 dimension case

X = dw.populate(N=1e5, scenario="star")
x <- joinbyv(X$SALES, join=list(X$CURRENCY, X$GEOGRAPHY))
dw <- build_hierarchy(x, factname="fact_sales")
sapply(dw$tables,ncol)
sapply(dw$tables,nrow)

## 5 dimensions

x <- dw.populate(N=1e5, scenario="denormalize")
names(x)
system.time(dw <- build_hierarchy(x,factname="fact_sales"))
# cardinality matrix, top 10x10
dw$cardinality[1:10,1:10]
# normalized tables into star schema
lapply(dw$tables,head,3)
# relation defintion, processing meta data also available
names(dw)

# better timing, setup ?db connection to automatically store logs in db
dw <- build_hierarchy(x,factname="fact_sales",timing=TRUE)
# print timing expressions and the rest of log entry
get.timing(FALSE,last=2L)[,{cat(paste0("\n# ",tag,"\n",expr),"\n",sep=""); .SD}][,.SD,.SDcols=-c("expr")]

## shiny app to browse model, will use following vars in Global Env:
# x - denormalized table
# dw - normalization results
if(interactive()) shiny::runApp(system.file("shinyBI", package="dwtools"))

## deploy to db

library(RSQLite)
sqlite1 = list(drvName="SQLite",dbname="sqlite1.db") # setup connection to db
sqlite1$conn = dbConnect(SQLite(), dbname=sqlite1$dbname)
options("dwtools.db.conns"=list(sqlite1=sqlite1))
x <- dw.populate(N=1e5, scenario="denormalize")
dw <- build_hierarchy(x,factname="fact_sales",deploy=TRUE,db.conn.name="sqlite1",timing=TRUE,verbose=1L)
get.timing()
db(c("dim_geog","fact_sales"))
dbDisconnect(sqlite1$conn); file.remove(sqlite1$dbname); options("dwtools.db.conns"=NULL); purge.timing()

## different data quality use cases tests:

# reorder columns
#setcolorder(x,sample(names(x),length(x),FALSE)) # random column order

# alter column name to disable common_words feature
#setnames(x,c("curr_code","curr_type"),c("curr1_code","curr2_type")) # no common words within fields in dimension

# broken hierarchy
#`:=`

# NAs
#`:=`
