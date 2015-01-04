context("db tests")
library(RSQLite)

test_that("single write and read match", {
  sqlite1 = list(drvName="SQLite",dbname="sqlite1.db")
  sqlite1$conn = dbConnect(SQLite(), dbname=sqlite1$dbname)
  options("dwtools.db.conns"=list(sqlite1=sqlite1))
  DT1 = dw.populate(1e3, scenario="fact")
  DT1 <- db(DT1,"my_tab1") # this additionally checks single write return one DT
  DT2 <- db("my_tab1")
  dbDisconnect(sqlite1$conn); file.remove(sqlite1$dbname); options("dwtools.db.conns"=NULL)
  expect_true(data.equal.data.table(DT1,DT2), info="test single write and read match")
})

test_that("single write and read match on timing", {
  sqlite1 = list(drvName="SQLite",dbname="sqlite1.db")
  sqlite1$conn = dbConnect(SQLite(), dbname=sqlite1$dbname)
  options("dwtools.db.conns"=list(sqlite1=sqlite1))
  DT1 = dw.populate(1e3, scenario="fact")
  DT1 <- db(DT1,"my_tab1",timing=TRUE) # this additionally checks single write return one DT
  DT2 <- db("my_tab1",timing=TRUE)
  dbDisconnect(sqlite1$conn); file.remove(sqlite1$dbname); options("dwtools.db.conns"=NULL)
  expect_true(data.equal.data.table(DT1,DT2), info="test single write and read match on timing")
})

test_that("multiple tables write and read match", {
  sqlite1 = list(drvName="SQLite",dbname="sqlite1.db")
  sqlite1$conn = dbConnect(SQLite(), dbname=sqlite1$dbname)
  options("dwtools.db.conns"=list(sqlite1=sqlite1))
  DT1 = dw.populate(1e3, scenario="fact")
  db(DT1,c("my_tab1","my_tab2"))
  DT23 <- db(c("my_tab1","my_tab2"))
  dbDisconnect(sqlite1$conn); file.remove(sqlite1$dbname); options("dwtools.db.conns"=NULL)
  expect_true(data.equal.data.table(DT1,DT23[[1]]) && data.equal.data.table(DT1,DT23[[2]]), info="test multiple tables write and read match")
})

test_that("multiple connections and multiple tables write and read match", {
  sqlite1 = list(drvName="SQLite",dbname="sqlite1.db")
  sqlite1$conn = dbConnect(SQLite(), dbname=sqlite1$dbname)
  sqlite2 = list(drvName="SQLite",dbname="sqlite2.db")
  sqlite2$conn = dbConnect(SQLite(), dbname=sqlite2$dbname)
  options("dwtools.db.conns"=list(sqlite1=sqlite1,sqlite2=sqlite2))
  DT1 = dw.populate(1e3, scenario="fact")
  DT1 <- db(DT1,c("my_tab1","my_tab2"),c("sqlite1","sqlite2")) # this additionally checks that multiple writes return only one DT
  DT23 <- db(c("SELECT * FROM my_tab1","SELECT * FROM my_tab2"),c("sqlite1","sqlite2"))
  dbDisconnect(sqlite1$conn); file.remove(sqlite1$dbname)
  dbDisconnect(sqlite2$conn); file.remove(sqlite2$dbname); options("dwtools.db.conns"=NULL)
  expect_true(data.equal.data.table(DT1,DT23$sqlite1) && data.equal.data.table(DT1,DT23$sqlite2), info="test multiple connections and multiple tables write and read match")
})

test_that("setkey on read", {
  sqlite1 = list(drvName="SQLite",dbname="sqlite1.db")
  sqlite1$conn = dbConnect(SQLite(), dbname=sqlite1$dbname)
  options("dwtools.db.conns"=list(sqlite1=sqlite1))
  DT1 = dw.populate(1e3, scenario="fact")
  db(DT1,"my_tab1")
  DT2 <- db("my_tab1",key=c("cust_code", "prod_code", "geog_code", "time_code", "curr_code")) # might not go via lapply
  DT3 <- db("my_tab1",key=c("cust_code", "prod_code", "geog_code", "time_code", "curr_code"),timing=TRUE) # this must go via lapply
  dbDisconnect(sqlite1$conn); file.remove(sqlite1$dbname); options("dwtools.db.conns"=NULL)
  expect_true(identical(key(DT2),c("cust_code", "prod_code", "geog_code", "time_code", "curr_code")) && identical(key(DT2),key(DT3)), info="test setkey on read")
})

test_that("tablename attribute on write and multiple writes with copy", {
  sqlite1 = list(drvName="SQLite",dbname="sqlite1.db")
  sqlite1$conn = dbConnect(SQLite(), dbname=sqlite1$dbname)
  options("dwtools.db.conns"=list(sqlite1=sqlite1))
  DT1 = dw.populate(1e3, scenario="fact")
  DT2 = db(copy(DT1))
  DT34 = db(copy(DT1),NULL,rep("sqlite1",2))
  DT2db = db(attr(DT2,"tablename",TRUE))
  DT34db = db(attr(DT34,"tablename",TRUE))
  dbDisconnect(sqlite1$conn); file.remove(sqlite1$dbname); options("dwtools.db.conns"=NULL)
  expect_true(is.null(attr(DT1,"tablename",TRUE)) && !is.null(attr(DT2,"tablename",TRUE)) && length(attr(DT34,"tablename",TRUE))==2L && data.equal.data.table(DT1,DT2db) && data.equal.data.table(DT1,DT34db[[1]]) && data.equal.data.table(DT1,DT34db[[2]]), info="test tablename attribute on write and multiple writes with copy")
})

test_that("tablename attribute on sequence write with no copy", {
  # documented in Note of db function manual
  sqlite1 = list(drvName="SQLite",dbname="sqlite1.db")
  sqlite1$conn = dbConnect(SQLite(), dbname=sqlite1$dbname)
  options("dwtools.db.conns"=list(sqlite1=sqlite1))
  DT1 = dw.populate(1e3, scenario="fact")
  DT1_backup <- copy(DT1)
  DT2 = db(DT1,c("my_tab1","my_tab2"))
  DT2_backup <- copy(DT2)
  DT3 = db(DT1,c("my_tab3"))
  dbDisconnect(sqlite1$conn); file.remove(sqlite1$dbname); options("dwtools.db.conns"=NULL)
  expect_true(identical(attr(DT1,"tablename",TRUE),c(sqlite1="my_tab3")) && identical(attr(DT2,"tablename",TRUE),c(sqlite1="my_tab3")) && identical(attr(DT3,"tablename",TRUE),c(sqlite1="my_tab3")) && identical(attr(DT1_backup,"tablename",TRUE),NULL) && identical(attr(DT2_backup,"tablename",TRUE),c(sqlite1="my_tab1",sqlite1="my_tab2")) && all.equal(address(DT1),address(DT2),address(DT3)), info="test tablename attribute on sequence write with no copy")
})

test_that("auto.table.name unq names on write", {
  sqlite1 = list(drvName="SQLite",dbname="sqlite1.db")
  sqlite1$conn = dbConnect(SQLite(), dbname=sqlite1$dbname)
  options("dwtools.db.conns"=list(sqlite1=sqlite1))
  DT1 = dw.populate(1e3, scenario="fact")
  tbl1 <- attr(db(DT1),"tablename",TRUE)
  tbl2 <- attr(db(DT1),"tablename",TRUE)
  dbDisconnect(sqlite1$conn); file.remove(sqlite1$dbname); options("dwtools.db.conns"=NULL)
  expect_true(!identical(tbl1,tbl2), info="test auto.table.name unq names on write")
})

test_that("dbCopy copy multiple tables and match", {
  sqlite1 = list(drvName="SQLite",dbname="sqlite1.db")
  sqlite1$conn = dbConnect(SQLite(), dbname=sqlite1$dbname)
  sqlite2 = list(drvName="SQLite",dbname="sqlite2.db")
  sqlite2$conn = dbConnect(SQLite(), dbname=sqlite2$dbname)
  options("dwtools.db.conns"=list(sqlite1=sqlite1,sqlite2=sqlite2))
  DT1 = dw.populate(1e3, scenario="fact")
  db(DT1,c("my_tab1","my_tab2"),"sqlite1")
  trunc.timing()
  dbCopy(c("my_tab1","my_tab2"),"sqlite1",c("some_tab1","some_tab2"),"sqlite2",timing=TRUE)
  DT23 <- db(c("some_tab1","some_tab2"),"sqlite2")
  dbDisconnect(sqlite1$conn); file.remove(sqlite1$dbname)
  dbDisconnect(sqlite2$conn); file.remove(sqlite2$dbname); options("dwtools.db.conns"=NULL)
  expect_true(data.equal.data.table(DT1,DT23[[1]]) && data.equal.data.table(DT1,DT23[[2]]) && nrow(get.timing())==4L, info="test dbCopy copy multiple tables and match")
})
