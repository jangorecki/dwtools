#' @title Append connection to dictionary
#' @param \dots named connections to append on current connections dictionary at \code{getOption('dwtools.db.conns')}.
#' @return Invisibly direct return from \code{options("dwtools.db.conns"=...)} - the previous value for an option.
#' @export
#' @example tests/example-add_db_conns.R
add.db.conns <- function(...){
  conns = list(...)
  if(any(is.null(names(conns)) || is.na(names(conns)) || (nchar(names(conns))==0 ))){
    stop("all elements passed to ... must be named")
  }
  O = getOption("dwtools.db.conns")
  if(any(names(conns) %in% names(O))){
    stop("db.conn names not unqiue, check current connections by: getOption('dwtools.db.conns')\nYou can always manipulate this list manually.")
  }
  invisible(options("dwtools.db.conns"=c(O,conns)))
}
