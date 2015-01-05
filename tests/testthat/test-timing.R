context("timing tests")
library(RSQLite)

test_that("basic timing and timestamp precision", {
  options("dwtools.timing.conn.name"=NULL); purge.timing()
  r <- timing(Sys.sleep(0.1))
  nch <- 19L + if(getOption("digits.secs") > 0) getOption("digits.secs") + 1L else 0
  expect_true(is.data.table(get.timing()) && nrow(get.timing())==1L && (get.timing()[,elapsed] %between% c(0.05,0.15)) && (nchar(as.character(get.timing()$timestamp))==nch), info="test basic timing and timestamp precision")
})

test_that("nested timing", {
  options("dwtools.timing.conn.name"=NULL); purge.timing()
  f <- function(x){
    colclass <- sapply(x, class)
    r <- timing(x[,lapply(.SD,sum),by=c(names(x)[colclass!="numeric"]),.SDcols=c(names(x)[colclass=="numeric"])],
                tag = "inner aggr")
    r
  }
  g <- function(x){
    setkeyv(x,names(x))[]
  }
  DT <- dw.populate(N=1e3, scenario="fact")
  r <- timing(g(f(DT)), tag="setkeyv")
  expect_true(is.data.table(r) && nrow(get.timing())==2L, info="test nested timing")
})

test_that("log timing to db and batch timing", {
  sqlite1 = list(drvName="SQLite",dbname="sqlite1.db")
  sqlite1$conn = dbConnect(SQLite(), dbname=sqlite1$dbname)
  options("dwtools.db.conns"=list(sqlite1=sqlite1))
  options("dwtools.timing.conn.name"="sqlite1")
  try(db("DROP TABLE dwtools_timing"),silent = TRUE)
  DT = dw.populate(1e3, scenario="fact")
  r = db(DT, c("sales","sales_20141211"),timing=TRUE)
  r1 = timing(db(DT, c("sales2")))
  dwtools_timing <- db("dwtools_timing")
  dbDisconnect(sqlite1$conn); file.remove(sqlite1$dbname)
  options("dwtools.db.conns"=NULL,"dwtools.timing.conn.name"=NULL,"dwtools.timing"=FALSE)
  expect_true(nrow(dwtools_timing)==3L, info="test log timing to db and batch timing")
})

test_that("auto timing option", {
  options("dwtools.timing.conn.name"=NULL); purge.timing(); options("dwtools.timing"=TRUE)
  sqlite1 = list(drvName="SQLite",dbname="sqlite1.db")
  sqlite1$conn = dbConnect(SQLite(), dbname=sqlite1$dbname)
  options("dwtools.db.conns"=list(sqlite1=sqlite1))
  DT <- dw.populate(N=1e3, scenario="fact")
  r <- db(DT, c("sales","sales_20141211"))
  options("dwtools.db.conns"=NULL,"dwtools.timing"=FALSE)
  expect_true(nrow(get.timing())==2L, info="test auto timing option")
})

test_that("get.timing arguments", {
  options("dwtools.timing.conn.name"=NULL); purge.timing()
  invisible(sapply(5:10/100, function(time) eval(bquote(timing(Sys.sleep(.(time)))))))
  expect_true(nrow(get.timing())==6L && nrow(get.timing(last=1L))==1L && nrow(get.timing(last=7L))==6L && ncol(get.timing(FALSE))==10L && ncol(get.timing(TRUE))==9L, info="test get.timing arguments")
})
