suppressPackageStartupMessages(library(data.table))
library(dwtools)
options("dwtools.verbose"=3)  # turn on status messages printed to console
library(RSQLite) # install.packages("RSQLite")

sqlite1 = list(drvName="SQLite",dbname="sqlite1.db")
sqlite1$conn = dbConnect(SQLite(), dbname=sqlite1$dbname)
options("dwtools.db.conns"=list(sqlite1=sqlite1))
str(getOption("dwtools.db.conns"))

# add csv
csv1 = list(drvName = "csv")
# add sqlite second
sqlite2 = list(drvName="SQLite",dbname="sqlite2.db")
sqlite2$conn = dbConnect(SQLite(), dbname=sqlite2$dbname)

add.db.conns(csv1=csv1, sqlite2=sqlite2)
getOption("dwtools.db.conns")
