suppressPackageStartupMessages(library(dwtools))

DT1 <- dw.populate(scenario="fact")
DT2 <- copy(DT1)[sample(1:nrow(DT1),nrow(DT1),FALSE)]
data.equal.data.table(DT1,DT2,ignore_row_order=TRUE)

setcolorder(DT2,sample(names(DT2),length(DT2),FALSE))
data.equal.data.table(DT1,DT2,ignore_row_order=TRUE,ignore_col_order=TRUE)
