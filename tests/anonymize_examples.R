suppressPackageStartupMessages(library(dwtools))

X = dw.populate(N=1e5, scenario="star")
DT <- joinbyv(X$SALES, list(X$CUSTOMER))
print(DT)
cols_to_mask <- c("cust_code", "cust_name", "cust_mail")
DT[, (cols_to_mask) := lapply(.SD, anonymize, "crc32"), .SDcols=c(cols_to_mask)]
print(DT)
