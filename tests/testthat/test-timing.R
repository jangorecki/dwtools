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
  options("dwtools.timing.conn.name"=NULL, "dwtools.timing"=TRUE)
  DTattr_write <- timing(db(DT1,c("my_tab1","my_tab2"))) # attr(DTattr_write,"timing",TRUE)
  DTattr_read <- timing(db(c("my_tab1","my_tab2"))) # attr(DTattr_read,"timing",TRUE)
  options("dwtools.timing.conn.name"="sqlite1")
  DTnonattr_write <- timing(db(copy(DT1),c("my_tab3","my_tab4"))) # attr(DTnonattr_write,"timing",TRUE) # NULL
  DTnonattr_read <- timing(db(c("my_tab3","my_tab4"))) # attr(DTnonattr_read,"timing",TRUE) # NULL
  dblogs <- db("dwtools_timing",timing=FALSE)
  dbDisconnect(sqlite1[["conn"]])
  file.remove(sqlite1[["dbname"]])
  options("dwtools.db.conns"=NULL,"dwtools.timing.conn.name"=NULL, "dwtools.timing"=FALSE)
  expect_true(all(is.data.table(attr(DTattr_write,"timing",TRUE))
                  , is.null(attr(DTnonattr_write,"timing",TRUE))
                  , is.null(attr(DTnonattr_read,"timing",TRUE))
                  , (nrow(attr(DTattr_write,"timing",TRUE))==3L)
                  , (nrow(attr(DTattr_read,"timing",TRUE))==3L)
                  , identical(nrow(attr(DTattr_write,"timing",TRUE))+nrow(attr(DTattr_read,"timing",TRUE)), nrow(dblogs)))
              , info="test correct timing in db")
})
