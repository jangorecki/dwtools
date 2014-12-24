suppressPackageStartupMessages(library(data.table))
library(dwtools)

X = dw.populate(scenario="star schema")
x <- joinbyv(X$SALES, join=list(X$CURRENCY, X$GEOGRAPHY))

r <- timing(dw.explore(x, filter=list("quantile"=0.025,"nrow.ratio"=0.05,"hierarchical.redundancy"=TRUE)),
            NA_integer_,"dw.explore")
attr(r,"timing")

if(interactive()) shiny::runApp("dwtools/inst/shinyDW")
