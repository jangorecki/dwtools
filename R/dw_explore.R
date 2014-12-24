
# dt imports --------------------------------------------------------------

forderv <- data.table:::forderv
is.unique <- function(x) identical(attr(forderv(x, retGrp=TRUE),"maxgrpn",TRUE),1L)
subsetDT <- function(x, i, cols) .Call(data.table:::CsubsetDT,x,i,cols,PACKAGE = "data.table")

# dw.explore --------------------------------------------------------------

#' @title DW explore
#' @description Explore hierarchies in data.table by groupings combinations and cardinality check. To improve performance the function uses internal data.table 1.9.6 functions.
#' @param x data.table
#' @param filter list
#' @param materialize logical
#' @param materialize.FUN function
#' @param .db.conn.name character
#' @param timing logical
#' @seealso \link{joinbyv}
#' @export
#' @example tests/dw_explore_examples.R
dw.explore <- function(x, filter = list("quantile"=0.025,"nrow.ratio"=0.05,"hierarchical.redundancy"=TRUE),
                       materialize = FALSE, materialize.FUN = sum, .db.conn.name = names(getOption("dwtools.db.conns"))[1],
                       timing = getOption("dwtools.timing")){
  # args validation
  if(isTRUE(materialize) && is.null(.db.conn.name)){
    stop("setup db connection to materialize data, read ?db")
  }
  xcols <- names(x)
  if(!is.unique(xcols)){
    warning("some column names has been renamed due to duplicates")
    setnames(x,which(duplicated(xcols)),paste0(xcols[duplicated(xcols)],"__dwtools"))
    xcols <- names(x)
    if(!is.unique(xcols)) stop("auto fixing duplicate column names did not fix all duplicates, clean your data.table for no dups in col names or do not use '__dwtools' suffix in column names")
  }
  # process
  xnr <- nrow(x)
  colsclass <- x[,sapply(.SD,class)]
  numcols <- colsclass=="numeric"
  bycols <- xcols[!numcols]
  bylen <- length(bycols)
  byi <- 1:bylen
  rbyi <- rev(byi)
  setattr(rbyi,"names",rbyi)
  
  byli <- lapply(rbyi, function(x) combn(bylen, x, simplify = FALSE))
  # TO DO - same as above but using `for`, do not calc subgrp but reuse already calculated from the result list
  options("datatable.auto.index"=FALSE)
  byN <- sum(sapply(byli,length))
  
  # precalc xaggr for better lapply memory allocation
  byii <- byli[[1]][[1]]
  o <- forderv(x, by=bycols[], sort=FALSE, retGrp=TRUE)
  starts <- attr(o,"starts",TRUE)
  xaggr <- subsetDT(x,o[starts],byii)
  
  grpsdt <- data.table(byn=rep(NA_integer_,byN), byi=list(), byichar=NA_character_, bycols=list(), bycolschar=NA_character_, grps=NA_integer_, maxgrpn=NA_integer_)
  
  i <- 0L
  timing({
    for(byn in names(byli)){
      lapply(byli[[byn]], function(byii){
        i <<- i + 1L
        o <- forderv(xaggr, by=bycols[byii], sort=FALSE, retGrp=TRUE)
        set(grpsdt, i, 1:7, data.table(byn=as.integer(byn), byi=list(byii), byichar=paste(byii,collapse=","), bycols=list(bycols[byii]), bycolschar=paste(bycols[byii],collapse=","), grps=length(attr(o,"starts",TRUE)), maxgrpn=attr(o,"maxgrpn",TRUE)))
      })
    }
    invisible(grpsdt)
  }, in.n = byN, tag = "groupings", .timing = timing)
  if(!(not_run <- TRUE)){
    #browser()
    #byli <- unlist(byli, recursive = FALSE, use.names=FALSE)
    bylname <- sapply(byli, function(byi) paste(byi,collapse="_"))
    bylcols <- lapply(setattr(byli,"names",bylname), function(byi) bycols[byi])
    # calc groups
    grpsl <- timing(lapply(byli, function(byi){
      #if(length(byi)==1) return(NULL) # dev, skip to 2 columns by
      #if(identical(byi,c(5L,6L))) browser()
      o <- forderv(x, by=bycols[byi], sort=FALSE, retGrp=TRUE)
      starts <- attr(o, "starts", TRUE)
      if(length(byi)==1) subgrplen <- NA_integer_
      else{
        sub_o <- forderv(subsetDT(x,o[starts],byi), by=bycols[byi[-length(byi)]], sort=FALSE, retGrp=TRUE)
        subgrplen <- length(attr(sub_o, "starts", TRUE))
        #sub_maxgrpn <- attr(sub_o, "maxgrpn", TRUE)
        # if(identical(sub_maxgrpn),1L) NULL # mark as redundand field
        #browser()
      }
      data.table(byi=paste(byi,collapse=","), bycols=paste(bycols[byi],collapse=","), grps=length(starts), subgrps=subgrplen)
    }), in.n = length(byli), tag = "groupings", .timing = timing)
    grpsdt <- rbindlist(grpsl)
  }
  
  # hierarchical redundancy TO DO?
  hierarchical.redundancy <- function(dt){
    return(list(filter.expr = TRUE))
    local.bylcols <- strsplit(dt[,bycols],",")
    setattr(local.bylcols,"names",gsub(",","_",dt[,byi]))
    # lapply?
    #length(attr(forderv(x, by=bycols, retGrp=TRUE),"starts"))
    #grplen <- timing(sapply(bylcols, function(bycols){ }), in.n = length(bylcols), tag = "groups_length", .timing = timing)
  }
  
  # keep order of filters, it can be important parameter when using hierarchical.redundancy, TO DO document
  if(length(filter)){
    grpsNall <- nrow(grpsdt)
    filter.expr <- list()
    for(fn in names(filter)){
      if("quantile"==fn){
        filter.expr[[fn]] <- bquote(grps <= quantile(grps,.(filter[[fn]])))
        grpsdt <- grpsdt[eval(filter.expr[[fn]])]
      } else  if("nrow.ratio"==fn){
        filter.expr[[fn]] <- bquote(grps <= (.(xnr) * .(filter[[fn]])))
        grpsdt <- grpsdt[eval(filter.expr[[fn]])]
      } else if("hierarchical.redundancy"==fn){
        hierarchy <- hierarchical.redundancy(grpsdt)
        filter.expr[[fn]] <- bquote(.(hierarchy[["filter.expr"]]))
        grpsdt <- grpsdt[eval(filter.expr[[fn]])]
      }
    }
    setattr(grpsdt,"filter",filter)
    setattr(grpsdt,"filter.expr",filter.expr)
    setattr(grpsdt,"filter.n",grpsNall-nrow(grpsdt))
  }
  
  # filter by detect hierarchy within col subset?? TO DO
  # curr_code, currency_type, cude_code - to be filtered out due to:
  # currency_type, currency_code, cust_code - case serve
  
  # TO DO move to shiny
  # grplendt[, plot(density(grplen), main = paste("filtered",attr(grplendt,"filtered",TRUE),"obs using ratio",attr(grplendt,"filter.ratio",TRUE)," of nrow"))]
  
  setorderv(grpsdt, c("grps","byichar"))
  
  if(isTRUE(materialize) && !is.null(.db.conn.name)){
    # TO DO test
    db(grpsdt,"dwtools_index",db.conn.name)
    for(i in 1:nrow(grpsdt)){ # i <- 1 # i <- i + 1
      db(x[,lapply(.SD, materialize.FUN), by=c(grpsdt[i,bycols]), .SDcols=c(xcols[numcols])],
         paste0("dwtools_",grpsdt[i,gsub(",","_",byi)]),
         db.conn.name)
    }
  }
  return(grpsdt[])
}
