suppressPackageStartupMessages(library(data.table))
library(dwtools)
options("dwtools.verbose"=3)  # turn on status messages printed to console

X = dw.populate() # scenario="star"
lapply(X, head)

DT = joinbyv(master = X$SALES,
             join = list(time = X$TIME, product = X$PRODUCT, currency = X$CURRENCY),
             by = list("time_code","prod_code","curr_code"))
str(DT)

# by currency type in time
DT[,list(value=sum(value,na.rm=TRUE)),keyby=list(time_code, curr_type)
   ][,list({
     plot(x=time_code, y=value, type="n")
     lines(x=time_code[curr_type=="crypto"], y=value[curr_type=="crypto"], col="green")
     lines(x=time_code[curr_type=="fiat"], y=value[curr_type=="fiat"], col="blue")
   })] # random data

# joinbyv including timing
DT = timing(
  joinbyv(master = X$SALES,
          join = list(time = X$TIME, product = X$PRODUCT, currency = X$CURRENCY),
          by = list("time_code","prod_code","curr_code"))
)
attr(DT,"timing")
