% Generated by roxygen2 (4.1.0): do not edit by hand
% Please edit documentation in R/add_db_conns.R
\name{add.db.conns}
\alias{add.db.conns}
\title{Append connection to dictionary}
\usage{
add.db.conns(...)
}
\arguments{
\item{\dots}{named connections to append on current connections dictionary at \code{getOption('dwtools.db.conns')}.}
}
\value{
Invisibly direct return from \code{options("dwtools.db.conns"=...)} - the previous value for an option.
}
\description{
Append connection to dictionary
}
\examples{
suppressPackageStartupMessages(library(dwtools))
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
}

