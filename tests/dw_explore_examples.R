suppressPackageStartupMessages(library(data.table))
library(dwtools)

X = dw.populate(scenario="star schema")
x <- joinbyv(X$SALES, join=list(X$CURRENCY, X$GEOGRAPHY))

r <- timing(dw.explore(x, filter=list("quantile"=0.025,"nrow.ratio"=0.05,"hierarchical.redundancy"=TRUE)),
            NA_integer_,"dw.explore")
# attr(r,"timing")

if(interactive()) shiny::runApp("dwtools/inst/shinyDW")

if(!(dont_run <- TRUE)){
  xcols <- names(x)
  colsclass <- x[,sapply(.SD,class)]
  numcols <- colsclass=="numeric"
  i <- 3
  x[,lapply(.SD, sum), by=c(r[i,bycols[[1]]]), .SDcols=c(xcols[numcols])]
  
  ## test broken hierarchy TO DO
  
  options("dwtools.timing.conn.name"=NULL)
  options("dwtools.db.conns"=NULL)
  
  # library(RSQLite)
  # sqlite = list(drvName="SQLite",dbname="dwtools.db")
  # sqlite$conn = dbConnect(SQLite(), dbname=sqlite$dbname)
  # options("dwtools.db.conns"=list(sqlite=sqlite))
  # options("dwtools.timing.conn.name"="sqlite")
  
  db("dwtools_timing")
  db('drop view dwtools')
  db('create view dwtools as select * from dwtools_timing order by timestamp desc') # lets make it shorter as order for easy choose last 5 etc.
  db('dwtools')[1][,{ # without view on sqlite you should use: i = timestamp==max(timestamp) # TO DO benchmark if better to sorting view on sqlite or timestamp==max(timestamp) on data.table.
    nm <- names(.SD)
    cat("timing log entry:\n")
    Sys.sleep(0.5)
    print(.SD[,nm[nm!="expr"],with=FALSE])
    Sys.sleep(1)
    cat("timing log expr:\n")
    Sys.sleep(0.5)
    cat(expr,"\n",sep="")
    NULL
  }]
}
