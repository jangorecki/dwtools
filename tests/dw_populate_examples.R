suppressPackageStartupMessages(library(data.table))
library(dwtools)
options("dwtools.verbose"=3)  # turn on status messages printed to console

X = dw.populate() # scenario="star schema"
lapply(X, head)

Z = dw.populate(scenario="denormalized star schema")
print(Z)

SALES = dw.populate(scenario="fact")
print(SALES)
