#' @title Data Warehouse tools
#' @description Extension for \link{data.table} package for Data Warehouse related functionalities.
#' @details The core functions includes:
#' \itemize{
#' \item \link{db} as Extracting and Loading tool in ETL terms.
#' \item \link{joinbyv} a denormalization of star schema and snowflake schema to flat table.
#' \item \link{dw.populate} populate star schema data.
#' \item \link{timing} measure timing and rows in-out.
#' \item \link{idxv} custom indices for in-memory processing.
#' }
#' @note All dot prefixed arguments are designed to be taken from the options, use them only in special cases, they may be removed from functions input args in future.
#' @docType package
#' @import data.table
#' @name dwtools
NULL

# technical ---------------------------------------------------------------

#' @title POSIXct
#' @description 0 length POSIXct wrapper
#' @keywords internal
POSIXct <- function(length = 0, tz = "UTC"){
  structure(numeric(length), class = c("POSIXct","POSIXt"), tzone = tz)
}

#' @title as.POSIXct
#' @description Setting default for UTC and 1970.
#' @keywords internal
as.POSIXct <- function(x,tz="UTC",origin="1970-01-01"){
  base::as.POSIXct(x,tz=tz,origin=origin)
}

#' @title is.int.POSIXct
#' @description Check if is integer and could be POSIX between 1970 and 2100.
#' @keywords internal
is.int.POSIXct <- function(x, date_from = as.POSIXct("1970-01-01"), date_to = as.POSIXct("2100-01-01")){
  is.integer(x) && all(as.POSIXct(x) %between% c(date_from,date_to))
}

#' @title nrowDT
#' @description Return nrow if DT else NA.
#' @keywords internal
nrowDT <- function(x){
  if(any(c("data.frame","data.table") %in% class(x))) nrow(x) else NA_integer_
}

#' @title nrowDTlengthVec
#' @description Return *nrow* if DT, *length* if Vec else *NA*.
#' @keywords internal
nrowDTlengthVec <- function(x){
  if(any(c("data.frame","data.table") %in% class(x))) nrow(x)
  else if(is.vector(x)) length(x)
  else NA_integer_
}

# maintanance -------------------------------------------------------------

#' @title pkgsVersion
#' @description Batch package version compare between libraries.
#' @param pkgs character vector of packages names.
#' @param libs character vector of libraries paths to compare, vector names will be column names.
#' @export
#' @example tests/pkgs_version.R
pkgsVersion <- function(pkgs, libs = .libPaths()){
  l = lapply(libs, function(lib){
    l = lapply(pkgs, function(pkg, lib){
      tryCatch(as.character(packageVersion(pkg, lib.loc = lib)),
               error = function(e) NA_character_)
    }, lib)
    setNames(l,pkgs)
  })
  if(length(names(libs)) > 0){
    l = setNames(l,names(libs))
  }
  else l = setNames(l,libs)
  setDT(l)[,pkg:=pkgs]
  setcolorder(l,c("pkg",names(l)[names(l)!="pkg"]))[]
}
