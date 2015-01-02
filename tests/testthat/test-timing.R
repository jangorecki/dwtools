context("timing tests")
library(RSQLite)

test_that("basic timing in attribute and timestamp precision", {
  r <- timing({Sys.sleep(1.5); 1L})
  nch <- 19L + if(getOption("digits.secs") > 0) getOption("digits.secs") + 1L else 0
  expect_true(is.data.table(attr(r,"timing",TRUE)) && (attr(r,"timing",TRUE)[,elapsed] %between% 1:2) && (nchar(as.character(attr(r,"timing",TRUE)$timestamp))==nch), info="test basic timing in attribute and timestamp precision")
})

test_that("catch existing timing attribute and append new", {
  f <- function(x){
    colclass <- sapply(x, class)
    r <- timing(x[,lapply(.SD,sum),by=c(names(x)[colclass!="numeric"]),.SDcols=c(names(x)[colclass=="numeric"])],
                tag = "inner aggr")
    r
  }
  g <- function(x){
    setkeyv(x,names(x))[]
  }
  DT <- dw.populate(N=1e4, scenario="fact")
  r <- timing(g(f(DT)), tag="setkeyv")
  #attr(r,"timing")[,{cat(paste0("\n# ",tag,"\n",expr),"\n",sep=""); .SD}][,.SD,.SDcols=-c("expr")]
  expect_true(isTRUE(getOption("dwtools.timing.append")) && identical(nrow(attr(r,"timing")), 2L), info="test if there are two logs in 'timing' attribute")
})

test_that("correct timing in db", {
  sqlite1 = list(drvName="SQLite",dbname="sqlite1.db")
  sqlite1$conn = dbConnect(SQLite(), dbname=sqlite1$dbname)
  options("dwtools.db.conns"=list(sqlite1=sqlite1))
  DT1 = dw.populate(1e4, scenario="fact")
  options("dwtools.timing.conn.name"=NULL)
  DTattr <- timing(db(DT1,c("my_tab1","my_tab2"),timing=TRUE))
  # attr(DTattr,"timing",TRUE)
  options("dwtools.timing.conn.name"="sqlite1")
  z <- timing(db(DT1,c("my_tab3","my_tab4"),timing=TRUE))
  dblogs <- db("dwtools_timing")
  dbDisconnect(sqlite1[["conn"]])
  file.remove(sqlite1[["dbname"]])
  options("dwtools.db.conns"=NULL,"dwtools.timing.conn.name"=NULL)
  expect_true(is.data.table(attr(DTattr,"timing",TRUE)) && (nrow(attr(DTattr,"timing",TRUE))==3L) && (nrow(attr(DTattr,"timing",TRUE))==nrow(dblogs)), info="test correct timing in db")
})
