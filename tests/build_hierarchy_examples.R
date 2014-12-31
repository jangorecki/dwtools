suppressPackageStartupMessages(library(dwtools))

X = dw.populate(N=1e5, scenario="star")
x <- joinbyv(X$SALES, join=list(X$CURRENCY, X$GEOGRAPHY))
dw <- build_hierarchy(x)
names(dw$tables)

x <- dw.populate(N=1e6, scenario="denormalize")
names(x)
system.time(dw <- build_hierarchy(x))
# cardinality matrix
dw$cardinality[1:10,1:10]
# normalized tables into star schema
lapply(dw$tables,head,3)
# relation defintion, processing meta data also available
names(dw)

# connect db

# build

# exceptions:

# reorder columns

# alter column name to disable common_words feature
#setnames()

# break hierarchies
#`:=`

# add NA in the data
#`:=`

# shiny app to browse model
#if(interactive()) shiny::runApp("dwtools/inst/shinyDW")
