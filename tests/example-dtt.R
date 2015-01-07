suppressPackageStartupMessages(library(dwtools)); dtt(purge=TRUE); options("datatable.timing"=FALSE)

DT <- data.table(a = 1L:5L)
DT[1:4,list(a)][2:3,list(a)]
dtt() # no timing yet

options("datatable.timing"=TRUE)
DT[1:4,list(a)][2:3,list(a)]
dtt()
dtt(arg.names=FALSE)
dtt(unchain=FALSE)

# measure overhead
dtt(purge=TRUE)
N <- 1e3
options("datatable.timing"=FALSE)
system.time(for(i in 1:N) DT[1:4,list(a)][2:3,list(a)])

options("datatable.timing"=TRUE)
system.time(for(i in 1:N) DT[1:4,list(a)][2:3,list(a)])

dtt()
dtt(purge=TRUE)
dtt()
