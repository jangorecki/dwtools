
# sql relations -----------------------------------------------------------

sql_relations <- function(rel, factname="fact"){
  sapply(rel, function(rel){
    sprintf("ALTER TABLE %s ADD CONSTRAINT %s FOREIGN KEY (%s) REFERENCES %s (%s)", factname, rel$fk_name, rel$fk_col_name, rel$fk_table, rel$fk_col_name)
  })
}

# common words ---------------------------------------------------------------------------------

common_words <- function(x, split="_"){
  if(length(x)==1) return(x)
  l <- lapply(x, function(x) unlist(strsplit(x,split=split)))
  ll <- lapply(l[-1], function(x) l[[1]] %in% x)
  v <- logical()
  for(w in 1:length(l[[1]])){
    v <- c(v,all(sapply(ll,`[[`,w)))
  }
  if(all(!isTRUE(v))){
    r <- paste(l[[1]][v],collapse="_")
  } else{
    r <- paste(l[[1]],collapse="_")
  }
  r
}

# build hierarchy -------------------------------------------------------

#' @title Build hierarchy
#' @description Detect hierarchies in the dataset and normalize to star schema, optionally deploy new model to db.
#' @param x data.table source dataset.
#' @param factname character, default \emph{fact}.
#' @param dimnames character currently only \emph{auto} supported, default. Dimension names will be created based on the common words in the fields which forms the dimension.
#' @param setkey logical if return tables with keys.
#' @param deploy logical, \emph{TRUE} will \strong{overwrite} the data in the target tables in connection \emph{db.conn.name}.
#' @param db.conn.name character deploy db connection name.
#' @param .db.conns list of connections uniquely named. See \link{db} function.
#' @param timing logical measure timing for vectorized usage, read \link{timing}, for single row timing summary use \code{timing(build_hierarchy(...))}.
#' @param verbose integer, if greater than 0 then print debugging messages.
#' @details Only basic star schema normalization will be created. All numeric fields is considered as measures, others as dimensions (including integer fields). Allocation of columns to dimension is performed based on the groupings count unique of all variable pairs. See \emph{cardinality} element of the result for cardinality matrix. Due to extensive computional processing of the function be aware it can take some time to return the results. You can use \emph{timing} argument to register sub processes time or \emph{verbose} to display processing messages.
#' @return
#' List of:
#' tables (multiple normalized R data.tables)
#' cardinality matrix represents groupings between all columns (computionally extensive for big datasets) like \code{length(unq(col1))/nrow(unique(data.table(col1,col2)))}.
#' lists of parents (including same entity attrs with \strong{any} cardinality) and list of childs (including same entity attrs with \strong{exact} cardinality) for each field.
#' @seealso \link{joinbyv}, \link{db}, \link{timing}
#' @export
#' @aliases dw.explore
#' @example tests/example-build_hierarchy.R
build_hierarchy <- function(x, factname = "fact", dimnames = "auto", setkey = TRUE,
                            deploy = FALSE,
                            db.conn.name,
                            .db.conns = getOption("dwtools.conns"),
                            timing = getOption("dwtools.timing"),
                            verbose = getOption("dwtools.verbose")){
  # input validation
  if(deploy && missing(db.conn.name)) stop("Deploy to db requires defined connections, check examples in ?build_hierarchy. You can use also csv as connecetion. To use the first defined connection try `db.conn.name = names(getOption('dwtools.conns'))[1]`.")
  if(!is.data.table(x)) stop("Argument x must be data.table")
  
  # main
  xcols <- names(x)
  colsclass <- x[,sapply(.SD,class)]
  numcols <- colsclass=="numeric"
  bycols <- xcols[!numcols]
  
  if(any(sapply(x, function(col) length(unique(col))==1L))){
    warning("remove redundant identity variable as a single column")
    browser()
    # TO DO test and workaround?
  }
  
  mx <- array(NA_real_, dim=c(length(bycols),length(bycols)), dimnames=list(select=bycols,group=bycols))
  pairs <- combn(bycols,2,simplify = FALSE)
  
  mx <- timing({
    for(pair in pairs){
      xaggr <- x[,unique(.SD),.SDcols=pair]
      select2by1 <- xaggr[,lapply(.SD, function(col) length(unique(col))), by=c(pair[1L]), .SDcols=c(pair[2L])][[2]]
      select1by2 <- xaggr[,lapply(.SD, function(col) length(unique(col))), by=c(pair[2L]), .SDcols=c(pair[1L])][[2]]
      mx[pair,pair] <- c(NA_real_,length(select1by2)/nrow(xaggr),length(select2by1)/nrow(xaggr),NA_real_)
    }
    mx
  }, length(pairs)*2, tag=paste("build_hierarchy","cardinality matrix",sep=getOption("dwtools.tag.sep",";")), .timing=timing, verbose=verbose)
  
  # parent/child lists
  lp <- apply(mx,1,function(x) which(x==1)) # parents including same entity attrs with any cardinality
  lc <- apply(mx,2,function(x) which(x==1)) # childs including same entity attrs with exact cardinality
  
  # TO DO vectorize columns to dimensions allocation
  l <- lapply(setNames(names(lp),names(lp)), function(child) c(child,names(lp[[child]])))
  lengs <- sapply(l, length)
  fk <- character()
  for(col in names(lp)){ # col <- names(lp)[1]
    ins <- which(sapply(l, function(l) col %in% l))
    fk <- c(fk,setNames(names(lengs[ins])[which.max(lengs[ins])],col))
  }
  
  if(identical(dimnames,"auto")){
    fk_dim_names <- sapply(unique(fk), function(fki) common_words(names(fk[fk==fki])))
  } else {
    stop("dimnames argument supports only 'auto' at the moment, it will create dimension names based on common words in the column names")
  }
  dw <- list(cardinality = mx,
             parents = lp, childs = lc,
             tables = list(), relations = list())
  dw <- timing({
    for(fki in 1:length(fk_dim_names)){ # fki <- 1#:length(fk_dim_names)
      dim_name <- fk_dim_names[fki]
      dw[["tables"]][[paste("dim",dim_name,sep="_")]] <- copy(x[,unique(.SD),.SDcols=c(names(fk)[fk==names(dim_name)])])
      fk_col_name <- names(dim_name)
      if(setkey) setkeyv(dw[["tables"]][[paste("dim",dim_name,sep="_")]], fk_col_name)
      fk_name <- paste("fk","dim",dim_name,fk_col_name,sep="_")
      dw[["relations"]][[fk_name]] <- list(fk_table=paste("dim",dim_name,sep="_"), fk_name=fk_name, fk_col_name=fk_col_name)
    }
    dw[["tables"]][[factname]] <- copy(x[,.SD,.SDcols=-c(bycols[!(bycols %in% unique(fk))])])
    if(setkey) setkeyv(dw[["tables"]][[factname]], unique(fk))
    dw
  }, length(fk_dim_names)+1L, tag=paste("build_hierarchy","build tables",sep=getOption("dwtools.tag.sep",";")), .timing=timing, verbose=verbose)
  if(!deploy) return(dw)
  # drop existing tables
  try(db(paste("DROP TABLE ",names(dw$tables)[length(dw$tables)]),db.conn.name), silent=TRUE) # fact table first
  try(db(paste("DROP TABLE ",names(dw$tables)[-length(dw$tables)]),db.conn.name), silent=TRUE) # dims in second step
  # create db with data
  ct <- tryCatch(invisible(sapply(names(dw$tables), function(table_name) db(dw[["tables"]][[table_name]],table_name,db.conn.name,timing=timing,verbose=verbose-1))),
                 error = function(e) stop(paste0("Error when write following tables: ",paste(names(dw$tables),collapse=","),". Error details: ",e$call,": ",e$message)))
  # SQLite does not support ALTER TABLE ADD CONSTRAINT, suppress warning
  fk <- if(getOption("dwtools.db.conns")[[db.conn.name]][["drvName"]]!="SQLite"){
    tryCatch(invisible(db(sql_relations(dw[["relations"]],names(dw$tables)[length(dw$tables)]),db.conn.name,timing=timing,verbose=verbose-1)),
             error = function(e){
               warning(paste0("Error when create following FK: ",paste(names(dw$relations),collapse=","),". Error details: ",e$call[[1]],": ",e$message[[1]]),call. = FALSE)
               NULL
             })
  } else NULL
  return(dw)
}

## TO DO
# [x] identify lowest granularity (FK) - no childs
# [x] link dimensions to FK
# [x] produce dim names by column names match common words
# [x] split into dim tables
# [ ] redundant identity variable - handle duplicates
# [ ] move core of cardinality matrix calculation to forderv
# [ ] tests: check if nrow on aggregation on FK match to nrow of aggregation on all bycols, if not raise error/debug
# [x] relations to sql fk script
# [x] currency dim, currency_type rename to curr_type - not related to dw.explore
# [x] timing
# [ ] dimnames, default common words, if none then the fk column name, otherwise character vector of dimension names in fixed order.
# [ ] vectorize process of allocation columns to dimensions
