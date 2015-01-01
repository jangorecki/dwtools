suppressPackageStartupMessages(library(dwtools))
library(shinyTree)

stopifnot(is.data.table(x), is.list(dw))
numcols <- sapply(x,class)=="numeric"

## dev
# [ ] query on shinyDW on normalized and denormalized data, control type by checkbox, query interface in shinyTree the same for both
# [ ] display timing of processing
