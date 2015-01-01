context("db tests")
library(RSQLite)

test_that("single write and read match", {
  sqlite1 = list(drvName="SQLite",dbname="sqlite1.db")
  sqlite1$conn = dbConnect(SQLite(), dbname=sqlite1$dbname)
  options("dwtools.db.conns"=list(sqlite1=sqlite1))
  DT1 = dw.populate(1e4, scenario="fact")
  db(DT1,"my_tab1")
  DT2 <- db("my_tab1")
  dbDisconnect(sqlite1[["conn"]])
  file.remove(sqlite1[["dbname"]])
  expect_true(data.equal.data.table(DT1,DT2,ignore_row_order=TRUE), info="test data match after single write-read")
})

test_that("multiple tables write and read match", {
  sqlite1 = list(drvName="SQLite",dbname="sqlite1.db")
  sqlite1$conn = dbConnect(SQLite(), dbname=sqlite1$dbname)
  options("dwtools.db.conns"=list(sqlite1=sqlite1))
  DT1 = dw.populate(1e4, scenario="fact")
  db(DT1,c("my_tab1","my_tab2"))
  DT23 <- db(c("my_tab1","my_tab2"))
  dbDisconnect(sqlite1[["conn"]])
  file.remove(sqlite1[["dbname"]])
  expect_true(all(data.equal.data.table(DT1,DT23[[1]],ignore_row_order=TRUE),data.equal.data.table(DT1,DT23[[2]],ignore_row_order=TRUE)), info="test data match after multiple tables write-read")
})

test_that("multiple connections and multiple tables write and read match", {
  sqlite1 = list(drvName="SQLite",dbname="sqlite1.db")
  sqlite1$conn = dbConnect(SQLite(), dbname=sqlite1$dbname)
  sqlite2 = list(drvName="SQLite",dbname="sqlite2.db")
  sqlite2$conn = dbConnect(SQLite(), dbname=sqlite2$dbname)
  options("dwtools.db.conns"=list(sqlite1=sqlite1,sqlite2=sqlite2))
  DT1 = dw.populate(1e4, scenario="fact")
  db(DT1,c("my_tab1","my_tab2"),c("sqlite1","sqlite2"))
  DT23 <- db(c("SELECT * FROM my_tab1","SELECT * FROM my_tab2"),c("sqlite1","sqlite2"))
  dbDisconnect(sqlite1[["conn"]]); dbDisconnect(sqlite2[["conn"]])
  file.remove(sqlite1[["dbname"]]); file.remove(sqlite2[["dbname"]])
  expect_true(all(data.equal.data.table(DT1,DT23[["sqlite1"]],ignore_row_order=TRUE),data.equal.data.table(DT1,DT23[["sqlite2"]],ignore_row_order=TRUE)), info="test data match after multiple connections and multiple tables write-read")
})

test_that("setkey on read", {
  sqlite1 = list(drvName="SQLite",dbname="sqlite1.db")
  sqlite1$conn = dbConnect(SQLite(), dbname=sqlite1$dbname)
  options("dwtools.db.conns"=list(sqlite1=sqlite1))
  DT1 = dw.populate(1e4, scenario="fact")
  db(DT1,"my_tab1")
  DT2 <- db("my_tab1",key=c("cust_code", "prod_code", "geog_code", "time_code", "curr_code"))
  dbDisconnect(sqlite1[["conn"]])
  file.remove(sqlite1[["dbname"]])
  expect_equal(key(DT2),c("cust_code", "prod_code", "geog_code", "time_code", "curr_code"), info="test setkey on read")
})

test_that("tablename attribute on write", {
  sqlite1 = list(drvName="SQLite",dbname="sqlite1.db")
  sqlite1$conn = dbConnect(SQLite(), dbname=sqlite1$dbname)
  options("dwtools.db.conns"=list(sqlite1=sqlite1))
  DT1 = dw.populate(1e4, scenario="fact")
  DT2 = db(copy(DT1))
  DT2db = db(attr(DT2,"tablename",TRUE))
  dbDisconnect(sqlite1[["conn"]])
  file.remove(sqlite1[["dbname"]])
  expect_true(all(
    is.null(attr(DT1,"tablename",TRUE)),
    !is.null(attr(DT2,"tablename",TRUE)),
    data.equal.data.table(DT2,DT2db,ignore_row_order=TRUE)
  ),
  info="test tablename attribute on write")
})
