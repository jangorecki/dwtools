#' @title as.data.table.xts
#' @description convert xts to data.table
#' @param x xts to convert to data.table
#' @param keep.index keep xts index as \emph{index} column in result data.table
#' @seealso \link{as.xts.data.table}
#' @export
#' @example tests/as_data_table_xts_examples.R
as.data.table.xts <- function(x, keep.index = TRUE){
  stopifnot(requireNamespace("xts"), !missing(x), xts::is.xts(x))
  if(!keep.index) return(setDT(as.data.frame(x, row.names=FALSE))[])
  if("index" %in% names(x)) stop(paste0("Input xts object should not have 'index' column because it would result in duplicate column names. Rename 'index' column in xts or use `keep.index=FALSE` and add index manually as another column."))
  r = setDT(as.data.frame(x, row.names=FALSE))
  r[, index := xts:::index.xts(x)]
  setcolorder(r,c("index",names(r)[names(r)!="index"]))[]
}

#' @title as.xts.data.table
#' @description convert data.table to xts
#' @param x data.table to convert to xts, must have \emph{POSIXct} or \emph{Date} in the first column. All others non-numeric columns will be omitted with warning.
#' @seealso \link{as.data.table.xts}
#' @export
#' @example tests/as_xts_data_table_examples.R
as.xts.data.table <- function(x){
  stopifnot(requireNamespace("xts"), !missing(x), is.data.table(x))
  if(!any(class(x[[1]]) %in% c("POSIXct","Date"))) stop("data.table must have a POSIXct or Date column on first position, use `setcolorder` function.")
  colsNumeric = sapply(x, is.numeric)[-1] # exclude first col, xts index
  if(any(!colsNumeric)) warning(paste("Following columns are not numeric and will be omitted:",paste(names(colsNumeric)[!colsNumeric],collapse=", ")))
  r = setDF(x[,.SD,.SDcols=names(colsNumeric)[colsNumeric]])
  xts::as.xts(r, order.by=x[[1]])
}
