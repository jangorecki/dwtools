
# timing ------------------------------------------------------------------

#' @title Measure timing
#' @description Collect timings and nrows when possible.
#' @param expr expression
#' @param nrow_in integer manually provided input object nrow
#' @param .timing logical
#' @param .timing.name character
#' @param .timing.conn.name character
#' @details Use option \code{options("dwtools.timing"=TRUE)} to turn on timing measurment in functions which supports timing measurement. To log timing to db connection, setup \code{options("dwtools.db.conns"}, provide connection name to \code{options("dwtools.timing.conn.name"="sqlite1")} and target table \code{options("dwtools.timing.name"="dwtools_timing")} (default) otherwise timing will be returned as \code{"timing"} attribute to the expression result.
#' @seealso \link{db}
#' @export
#' @example tests/timing_examples.R
timing <- function(expr, nrow_in = NA_integer_,
                   .timing = TRUE,
                   .timing.name = getOption("dwtools.timing.name"),
                   .timing.conn.name = getOption("dwtools.timing.conn.name")){
  if(!.timing) return(eval.parent(expr))
  subx = substitute(expr)
  l = system.time(r <- eval.parent(expr))
  x = setDT(as.list(l))[,list(timestamp = Sys.time(),
                              dwtools_session = getOption("dwtools.session"),
                              expr = paste(deparse(subx, width.cutoff=500L),collapse="\n"),
                              expr_crc32 = digest(subx,algo="crc32"),
                              nrow_in = nrow_in,
                              nrow_out = nrowDT(r),
                              user_self = user.self,
                              sys_self = sys.self,
                              elapsed = elapsed,
                              user_child = user.child,
                              sys_child = sys.child)]
  if(!is.null(.timing.name) && !is.null(.timing.conn.name)) db(x, .timing.name, .timing.conn.name, timing=FALSE)
  else setattr(r, "timing", x)
  return(r)
}