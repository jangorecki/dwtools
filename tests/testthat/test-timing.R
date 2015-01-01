context("timing tests")

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
  
  DT <- dw.populate(N=1e5, scenario="fact")
  r <- timing(g(f(DT)), tag="setkeyv")
  #attr(r,"timing")[,{cat(paste0("\n# ",tag,"\n",expr),"\n",sep=""); .SD}][,.SD,.SDcols=-c("expr")]
  
  expect_equal(nrow(attr(r,"timing")), 2L, info="test if there are two logs in 'timing' attribute")
})