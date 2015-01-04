suppressPackageStartupMessages(library(dwtools))

X = dw.populate() # scenario="star"
lapply(X, head)

DT = joinbyv(master = X$SALES,
             join = list(time = X$TIME, product = X$PRODUCT, currency = X$CURRENCY),
             by = list("time_code","prod_code","curr_code"))
str(DT)

# joinbyv including timing
DT = timing(
  joinbyv(master = X$SALES,
          join = list(time = X$TIME, product = X$PRODUCT, currency = X$CURRENCY),
          by = list("time_code","prod_code","curr_code"))
)
get.timing()
