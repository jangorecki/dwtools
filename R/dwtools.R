#' @title Data Warehouse tools
#' @description Extension for \link{data.table} package for Data Warehouse related functionalities.
#' @details The core functions includes:
#' \itemize{
#' \item \link{db} as Extracting and Loading tool in ETL terms.
#' \item \link{joinbyv} a denormalization of star schema and snowflake schema to single table.
#' \item \link{dw.populate} populate star schema data.
#' \item \link{timing} measure timing and rows in-out, including logging to db and verbose messages.
#' \item \link{build_hierarchy} transform single dataset to star schema, columns allocation based on cardinalities in unique groupings of each pair.
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
  else if(is.list(x) || is.vector(x)) length(x)
  else NA_integer_
}

# maintanance -------------------------------------------------------------

#' @title pkgsVersion
#' @description Batch package version compare between libraries.
#' @param pkgs character vector of packages names.
#' @param libs character vector of libraries paths to compare, vector names will be column names.
#' @export
#' @example tests/example-pkgs_version.R
pkgsVersion <- function(pkgs, libs = .libPaths()){
  # TO DO optimize code below
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

# data.table helpers ------------------------------------------------------

#' @title Data equal in two data.tables
#' @description Test if data equal in two data.tables, can ignore order of rows or columns.
#' @param DT1 data.table.
#' @param DT2 data.table.
#' @param ignore_row_order logical.
#' @param ignore_col_order logical.
#' @param check.attributes logical, only \emph{FALSE} supported.
#' @note Duplicate names in DTs were not tested. All attributes all ignored.
#' @export
#' @example tests/example-data_equal_data_table.R
data.equal.data.table <- function(DT1, DT2, ignore_row_order=FALSE, ignore_col_order=FALSE, check.attributes=FALSE){
  if(check.attributes) stop("check.attributes TRUE is not supported, function test only the data.")
  if(!identical(class(DT1),class(DT2))) return(FALSE)
  if(!identical(length(DT1),length(DT2))) return(FALSE)
  DT1 <- copy(DT1)
  DT2 <- copy(DT2)
  if(!ignore_row_order && !ignore_col_order) return(identical(DT1,DT2))
  if(ignore_col_order){
    if(!identical(names(DT1),names(DT2))){
      if(!identical(names(DT1)[order(names(DT1))],names(DT2)[order(names(DT2))])) return(FALSE) # check if identical after sorting
      else{
        setcolorder(DT1,names(DT1)[order(names(DT1))])
        setcolorder(DT2,names(DT2)[order(names(DT2))])
      }
    }
  }
  DT1[,`__dwtools_N`:=.N,by=c(names(DT1))]
  DT2[,`__dwtools_N`:=.N,by=c(names(DT2))]
  setkeyv(DT1,names(DT1))
  setkeyv(DT2,names(DT2))
  all(sapply(list(DT2[!DT1], DT1[!DT2]), function(x) nrow(x)==0L))
}
