suppressPackageStartupMessages(library(data.table))
library(dwtools)
options("dwtools.verbose"=3)  # turn on status messages printed to console

data(sample_matrix, package = "xts")
sample.xts <- xts::as.xts(sample_matrix)
# print head of xts
print(head(sample.xts))
# print dt
print(as.data.table(sample.xts))
