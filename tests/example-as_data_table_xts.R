suppressPackageStartupMessages(library(dwtools))

data(sample_matrix, package = "xts")
sample.xts <- xts::as.xts(sample_matrix)
# print head of xts
print(head(sample.xts))
# print dt
print(as.data.table(sample.xts))
