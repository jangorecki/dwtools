#' @title SQL query to data.table query mapping
#' @description Push data.table arguments into sql friendly way, see examples. 
#' @param select list or variables or single variable to select or update.
#' @param from data.table.
#' @param where expression which should return logical when executed in \code{[i=...]}.
#' @param group list of variables/functions to group by.
#' @param order expression custom order, usually \code{order(colA)}.
#' @param .SDcols character vector, to be used when using it in \emph{select}, e.g. in \code{lapply}.
#' @details This function is just a simple wrapper of SQL argument sequence and name to data.table argument sequence and name. All language syntax in each args must be valid R statement e.g. \code{list(cust_code, curr_code)} which will be forwarded to \code{data.table} arguments.
#' @export
#' @example tests/example-sql_dt.R
sql.dt <- function(select, from, where, group, order, .SDcols){
  `[.data.table` = data.table:::`[.data.table`
  if(missing(select)) select = quote(.SD) # select *
  if(missing(from)) stop("data.table was not provided in _from_ argument") # table does not exists
  if(missing(where)) where = quote(TRUE) # where 1=1
  if(missing(group)) group = quote(list())
  if(missing(order)) order = quote(TRUE)
  jj = substitute(select) # select ...
  xx = substitute(from)   # from ...
  ii = substitute(where)  # where ...
  by = substitute(group)  # group by ...
  or = substitute(order)  # order by ...
  rm(order, envir = environment())
  `[.data.table`(
    x = if(missing(.SDcols)) `[.data.table`(
      x = eval(xx),
      i = eval(ii),
      j = eval(jj),
      by = eval(by)
      ) else `[.data.table`(
        x = eval(xx),
        i = eval(ii),
        j = eval(jj),
        by = eval(by),
        .SDcols = .SDcols),
    i = eval(or))
}
