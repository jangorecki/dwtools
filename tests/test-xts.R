suppressPackageStartupMessages(library(dwtools))
library(testthat)

context("data.table-xts conversion #882")

test_that("validate_match_on_conversion", {
  # Date
  dt <- data.table(index = as.Date((as.Date("2014-12-12")-49):as.Date("2014-12-12"),origin="1970-01-01"),
                   quantity = rep(c(1:5),10),
                   value = rep(c(1:10)*100,5))
  xt <- xts::as.xts(matrix(data = c(dt$quantity, dt$value),
                           ncol = 2,
                           dimnames = list(NULL,c("quantity","value"))),
                    order.by = dt$index)
  dt_xt = as.data.table(xt)
  xt_dt = as.xts.data.table(dt)
  expect_equal(dt, dt_xt,
               check.attributes = FALSE, # xts has more attributes of index, potentially user can also provide them in 'index' column. So it cannot be just trimmed.
               info="test data.table match to data.table from xts, Date index")
  expect_equal(xt, xt_dt,
               info="test xts match to xts from data.table, Date index")
  
  # POSIXct
  dt <- data.table(index = as.POSIXct(as.Date((as.Date("2014-12-12")-49):as.Date("2014-12-12"),origin="1970-01-01"),origin="1970-01-01"),
                   quantity = rep(c(1:5),10),
                   value = rep(c(1:10)*100,5))
  xt <- xts::as.xts(matrix(data = c(dt$quantity, dt$value),
                           ncol = 2,
                           dimnames = list(NULL,c("quantity","value"))),
                    order.by = dt$index)
  dt_xt = as.data.table(xt)
  xt_dt = as.xts.data.table(dt)
  expect_equal(dt, dt_xt,
               check.attributes = FALSE,
               info="test data.table match to data.table from xts, POSIXct index")
  expect_equal(xt, xt_dt,
               info="test xts match to xts from data.table, POSIXct index")
})