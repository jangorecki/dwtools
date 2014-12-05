#' @title as.data.table.xts
#' @description convert xts to data.table
#' @param x xts to convert to data.table
#' @param keep.rownames keep xts index as \emph{index} column in result data.table
#' @seealso \link{as.xts.data.table}
#' @export
#' @example tests/as_data_table_xts_examples.R
as.data.table.xts <- function(x, keep.rownames = TRUE){
  stopifnot(requireNamespace("xts") || !missing(x) || xts::is.xts(x))
  r = setDT(as.data.frame(x), keep.rownames = keep.rownames)
  if(!keep.rownames) return(r[])
  setnames(r,"rn","index")
  setkeyv(r,"index")[]
}

#' @title as.xts.data.table
#' @description convert data.table to xts
#' @param x data.table to convert to xts, must have \emph{POSIXct} or \emph{Date} in the first column. All others non-numeric columns will be omitted with warning.
#' @seealso \link{as.data.table.xts}
#' @export
#' @example tests/as_xts_data_table_examples.R
as.xts.data.table <- function(x){
  stopifnot(requireNamespace("xts") || !missing(x) || is.data.table(x) || any(class(x[[1]] %in% c("POSIXct","Date"))))
  colsNumeric = sapply(x, is.numeric)[-1] # exclude first col, xts index
  if(any(!colsNumeric)){
    warning(paste("Following columns are not numeric and will be omitted:",paste(names(colsNumeric)[!colsNumeric],collapse=", ")))
  }
  r = setDF(x[,.SD,.SDcols=names(colsNumeric)[colsNumeric]])
  rownames(r) <- x[[1]]
  xts::as.xts(r)
}
