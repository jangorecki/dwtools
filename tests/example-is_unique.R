suppressPackageStartupMessages(library(dwtools))

x1 <- c("a","b","c","d","e")
x2 <- c(x1,"d")
is.unique(x1)
is.unique(x2)

# my previous approach
is.unq <- function(x) length(x)==length(unique(x))

x <- sample(1e2, 1e7, TRUE) # non-unq
system.time(is.unq(x))
system.time(is.unique(x))
x <- sample(1e7, 1e7, FALSE) # unq
system.time(is.unq(x))
system.time(is.unique(x))
