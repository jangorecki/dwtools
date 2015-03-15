context("joinbyv")

expect_error({
  joinbyv(master = X$SALES,
          join = list(time = X$TIME),
          by = list("time_code"),
          row.subset = list(quote(time_year_code2 >= 2013)))[]
}, info = "invalid expression in roow.subset should results error")
