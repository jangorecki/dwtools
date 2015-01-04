
# timing ------------------------------------------------------------------

#' @title Measure timing and expression metadata
#' @description Collect expressions, timings, nrows in-out when possible. Optionally log to database.
#' @param expr expression.
#' @param in.n integer manually provided input object nrow.
#' @param tag character custom processing message to be logged with that entry. Vector will be collapse to scalar by \code{getOption("dwtools.tag.sep",";")}.
#' @param .timing logical easy escape \emph{timing} function without timing.
#' @param .timing.name character
#' @param .timing.conn.name character, when \emph{NULL} then timings logs are stored in-memory, see \link{get.timing}.
#' @param verbose integer, if greater than 0 then print debugging messages. It will use \emph{tag} argument. It is designed to serve \emph{verbose} functionallity for user processes, not for the \emph{dwtools} functions.
#' @details Use option \code{options("dwtools.timing"=TRUE)} to turn on timing measurment in functions which supports timing measurement (e.g. \link{db}, \link{build_hierarchy}). To log timing to db connection, setup \code{options("dwtools.db.conns"}, provide connection name to \code{options("dwtools.timing.conn.name"="sqlite1")} and target table \code{options("dwtools.timing.name"="dwtools_timing")} (default) otherwise timing will logged to in-memory table, which can be accessed by \code{get.timing()}.
#' @note Timing as attribute is available for non NULL results of \emph{expr}, to make timing of NULL result function provide connection name in \code{getOption("dwtools.timing.conn.name")}, may be also \emph{csv} connection, read more at \link{db}.\cr When using verbose user should use either \code{dwtools.timing.verbose} option or \code{dwtools.verbose} option, using both at the same time may result a mess, see last example.
#' @seealso \link{get.timing}, \link{db}, \link{build_hierarchy}, \link{dbCopy}
#' @import digest devtools
#' @export
#' @example tests/example-timing.R
timing <- function(expr, in.n = NA_integer_, tag = NA_character_,
                   .timing = TRUE,
                   .timing.name = getOption("dwtools.timing.name"),
                   .timing.conn.name = getOption("dwtools.timing.conn.name"),
                   verbose = getOption("dwtools.timing.verbose")){
  # easy espace when .timing=FALSE
  if(!.timing && verbose <= 0) return(eval.parent(expr))
  if(length(tag)>1) tag = paste(tag,collapse=getOption("dwtools.tag.sep",";"))
  tagtext <- paste(unlist(strsplit(x = tag, split = getOption("dwtools.tag.sep",";"))), collapse=paste0(getOption("dwtools.tag.sep",";")," ")) # increase readability by turning ";" into "; "
  stopifnot(length(tag)==1)
  timestamp_start <- Sys.time()
  # easy espace when .timing=FALSE using verbose
  if(!.timing && verbose > 0){
    cat(as.character(timestamp_start),": ",tagtext,"...\n",sep="")
    return(eval.parent(expr))
  }
  subx = substitute(expr)
  if(verbose > 0){
    cat(as.character(timestamp_start),": ",tagtext," took... ",sep="") 
    l = system.time(r <- eval.parent(expr))
    cat(l["user.self"]+l["sys.self"]," sec\n",sep="")
  } else {
    l = system.time(r <- eval.parent(expr))
  }
  x = setDT(as.list(l))[,list(timestamp = Sys.time(), # timestamp_end
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
  add.timing(x)
  return(r)
}

# timing helpers ----------------------------------------------------------

#' @title Get timing logs
#' @param trunc_expr integer or logical, when \emph{integer} then cut \emph{expr} field after nchar, or when \emph{TRUE} remove \emph{expr} from result, when \emph{FALSE} include full \emph{expr}.
#' @param last integer or logical, if \emph{FALSE} then full log returned, when \emph{TRUE} then only last log entry is returned, when integer then last n entries returned.
#' @seealso \link{trunc.timing}
#' @export
get.timing <- function(trunc_expr = 16L, last=FALSE){
  # [ ] minor, consider add get.timing from db
  # , .timing.name = getOption("dwtools.timing.name"), .timing.conn.name = getOption("dwtools.timing.conn.name")
  #if(!is.null(.timing.conn.name)){
  #  stopifnot(is.character(.timing.conn.name), is.character(.timing.name))
  #  tt <- db(.timing.name, .timing.conn.name, .db.batch.action="read", timing=FALSE, verbose=0)[order(timestamp)]
  #}
  # [ ] minor, do `with(dwtools.cache, ...)` to filter for last within the dwtools.cache environment, then copy
  dwtools.timing <- dwtools.cache[["dwtools_timing"]]
  # dwtools.timing
  if(is.null(dwtools.timing)) return(empty.timing())
  else if(is.numeric(last) && last > 0L) tt <- rbindlist(dwtools.timing[(length(dwtools.timing)-(as.integer(last)-1L)):length(dwtools.timing)])
  else if(isTRUE(last)) tt <- dwtools.timing[[length(dwtools.timing)]]
  else if(!isTRUE(last)) tt <- rbindlist(dwtools.timing)
  # trunc_expr
  if(nrow(tt)==0L) return(tt)
  else if(is.numeric(trunc_expr)) return(tt[nchar(expr) > trunc_expr, expr:=paste0(substr(expr,1L,trunc_expr),"...")][])
  else if(isTRUE(trunc_expr)) return(tt[,.SD,.SDcols=-"expr"])
  else if(!isTRUE(trunc_expr)) return(tt)
}

#' @title Add timing to cache
#' @keywords internal
add.timing <- function(x, .timing.name = getOption("dwtools.timing.name"), .timing.conn.name = getOption("dwtools.timing.conn.name")){
  if(!is.null(.timing.conn.name)){
    stopifnot(is.character(.timing.conn.name), is.character(.timing.name))
    db(x, .timing.name, .timing.conn.name, .db.batch.action="write", timing=FALSE, verbose=0)
  }
  else {
    # [ ] minor, assign within dwtools.cache env
    assign("dwtools_timing", c(dwtools.cache[["dwtools_timing"]], list(x)), envir=dwtools.cache)
  }
  invisible(TRUE)
}

#' @title Truncate in-memory timing logs cache
#' @export trunc.timing
trunc.timing <- function(){
  assign("dwtools_timing", NULL, envir=dwtools.cache)
}

#' @title Empty timing table
#' @keywords internal
empty.timing <- function(){
  data.table(timestamp = structure(numeric(0), class = c("POSIXct", "POSIXt")), dwtools_session = integer(0), expr = character(0), expr_crc32 = character(0), in_n = integer(0), out_n = integer(0), user_self = numeric(0), sys_self = numeric(0), elapsed = numeric(0), tag = character(0))
}