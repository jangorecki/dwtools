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