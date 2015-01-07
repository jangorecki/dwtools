datatable.timing <- new.env()
datatable.timing$timing <- NULL

#' @title data.table query with timing
#' @description Read \code{?data.table:::`[.data.table`} for data.table query manual. 
#' @param x data.table
#' @param i
#' @param j
#' @param by
#' @param keyby
#' @param with
#' @param nomatch
#' @param mult
#' @param roll
#' @param rollends
#' @param which
#' @param .SDcols
#' @param verbose
#' @param allow.cartesian
#' @param drop
#' @param rolltolast
#' @return Result from data.table, as side effect timing logs in \emph{datatable.timing} environment, accessible by \link{dtt}.
#' @seealso \link{dtt}
#' @export
#' @example tests/example-dtt.R
"[.data.table" <- function(x, i, j, by, keyby, with = TRUE, nomatch = getOption("datatable.nomatch"), 
                           mult = "all", roll = FALSE, rollends = if (roll == "nearest") c(TRUE, TRUE) else if (roll >= 0) c(FALSE, TRUE) else c(TRUE, FALSE), which = FALSE, .SDcols, verbose = getOption("datatable.verbose"), 
                           allow.cartesian = getOption("datatable.allow.cartesian"), 
                           drop = NULL, rolltolast = FALSE){
  .timing <- getOption("datatable.timing",FALSE)
  CALL <- match.call()
  DT.CALL <- as.dt.call(CALL)
  if(.timing) TT <- proc.time()
  r <- devtools::with_options(c("datatable.timing"=FALSE), eval.parent(DT.CALL))
  if(.timing){
    if(verbose) cat("insert timing\n")
    datatable.timing$TT <- c(list(timestamp=Sys.time()), as.list(unclass(proc.time()-TT)),fun.call=list(list(CALL)))
    evalq(timing <- c(timing,list(TT)), envir=datatable.timing)
  }
  return(r)
}

as.dt.call <- function(x){
  x[[1]] <- quote(data.table:::`[.data.table`)
  x
}

#' @title data.table timing
#' @param unchain logical print only single chain elements, default \emph{TRUE}. When \emph{FALSE} it will print direct call.
#' @param arg.names logical, print also \code{DT[i,j,...]} arguments, affects only \code{unchain=TRUE}.
#' @param purge logical truncate current data.table timing logs.
#' @export
#' @example tests/example-dtt.R
dtt <- function(unchain=TRUE, arg.names=TRUE, purge=FALSE){
  if(purge){
    datatable.timing$timing <- NULL
    return(invisible(data.table()))
  }
  dt <- rbindlist(datatable.timing$timing)
  N <- nrow(dt)
  if(N==0L) return(data.table())
  if(unchain){
    # turn `[.data.table(...)` into `[...]`
    deparse.unchain <- function(fun.call, arg.names){
      args <- as.list(fun.call[-c(1L,2L)]) # exclude function call name and 'x' argument
      arg.queries <- if(arg.names){
        vapply(names(args), function(arg, args) paste(arg,paste(deparse(args[[arg]], width.cutoff=500L),collapse="\n"),sep="="), "", args, USE.NAMES=FALSE)
      } else {
        vapply(args, function(qarg) paste(deparse(qarg, width.cutoff=500L),collapse="\n"), "", USE.NAMES=FALSE)
      }
      paste0("[",paste(arg.queries,collapse=", "),"]")
    }
    data.table:::`[.data.table`(dt, j = fun.call.char:=deparse.unchain(fun.call[[1L]],arg.names=arg.names), by = 1L:N)
  } else {
    data.table:::`[.data.table`(dt, j = fun.call.char:=unlist(paste(deparse(fun.call[[1L]]))), by = 1L:N)
  }
  data.table:::`[.data.table`(dt) # non-invisible: dt[]
}
