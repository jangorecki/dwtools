
# timing ------------------------------------------------------------------

#' @title Measure timing
#' @description Collect timings and nrows when possible.
#' @param expr expression.
#' @param in.n integer manually provided input object nrow.
#' @param tag character scalar custom processing message to be logged with that entry.
#' @param .timing logical
#' @param .timing.name character
#' @param .timing.conn.name character
#' @param verbose integer, if greater than 0 then print debugging messages.
#' @details Use option \code{options("dwtools.timing"=TRUE)} to turn on timing measurment in functions which supports timing measurement (e.g. \link{db}, \link{dbCopy}). To log timing to db connection, setup \code{options("dwtools.db.conns"}, provide connection name to \code{options("dwtools.timing.conn.name"="sqlite1")} and target table \code{options("dwtools.timing.name"="dwtools_timing")} (default) otherwise timing will be returned as \code{"timing"} attribute to the expression result.
#' @seealso \link{db}
#' @import digest devtools
#' @export
#' @example tests/timing_examples.R
timing <- function(expr, in.n = NA_integer_, tag = NA_character_,
                   .timing = TRUE,
                   .timing.name = getOption("dwtools.timing.name"),
                   .timing.conn.name = getOption("dwtools.timing.conn.name"),
                   verbose = getOption("dwtools.verbose")){
  if(!.timing) return(eval.parent(expr)) # easy espace
  stopifnot(length(tag)==1)
  subx = substitute(expr)
  l = system.time(r <- eval.parent(expr))
  x = setDT(as.list(l))[,list(timestamp = devtools::with_options(options('digits.secs'=3),Sys.time()),
                              dwtools_session = getOption("dwtools.session"),
                              expr = paste(deparse(subx, width.cutoff=500L),collapse="\n"),
                              expr_crc32 = digest::digest(subx,algo="crc32"),
                              in_n = in.n,
                              out_n = nrowDTlengthVec(r),
                              user_self = user.self,
                              sys_self = sys.self,
                              elapsed = elapsed,
                              tag = tag),
                        verbose=FALSE] # suppress data.table verbose
  if(!is.null(.timing.name) && !is.null(.timing.conn.name)) db(x, .timing.name, .timing.conn.name, timing=FALSE, verbose=verbose-1)
  else setattr(r, "timing", x)
  return(r)
}

#' @title timingv
#' @description rbindlist and setattr
#' @keywords internal
timingv <- function(r) setattr(r, "timing", rbindlist(lapply(r,function(x) attr(x,"timing",TRUE))))[]
