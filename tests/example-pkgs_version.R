suppressPackageStartupMessages(library(dwtools))

pkgs <- c("dplyr","data.table","RSQLite","dwtools")
pkgsVersion(pkgs)

# custom libraries
pkgsVersion(pkgs, libs = c(dev = .libPaths()[1], prod = .libPaths()[2]))
