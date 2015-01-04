suppressPackageStartupMessages(library(dwtools))

X = dw.populate() # scenario="star"
lapply(X, head)

Z = dw.populate(scenario="denormalize")
print(Z)

SALES = dw.populate(scenario="fact")
print(SALES)
