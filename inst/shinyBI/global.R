suppressPackageStartupMessages(library(dwtools))
library(shinyTree)

if(!exists("x")) x <- dw.populate(N=1e5, scenario="denormalize")
if(!exists("dw")) dw <- build_hierarchy(x,factname="fact_sales")

stopifnot(is.data.table(x), is.list(dw))
numcols <- sapply(x,class)=="numeric"

## dev
# [ ] query on shinyBI on normalized and denormalized data, control type by checkbox, query interface in shinyTree the same for both
# [ ] display timing of processing
