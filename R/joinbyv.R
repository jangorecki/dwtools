#' @title Batch join multiple tables
#' @description Perform batch join of multiple tables to one \emph{master} table.
#' @param master data.table, optionally also a single data.table nested in the list.
#' @param join list of data.tables which to join to \emph{master} data.table.
#' @param by list of character vectors. Default: \code{lapply(join, key)}. Required when not all of \emph{join} data.tables has key.
#' @param col.subset list of character vectors. Default: \code{lapply(join, names)}.
#' @param row.subset list of \link{expression}s to be passed to corresponding \emph{join} data.table \code{i} argument. Default: \code{as.list(rep(TRUE,length(join)))}. To subset result \emph{master} data.table, use \emph{row.subset} together with corresponding \emph{nomatch} argument equal to \code{0} (inner join). By default when providing \emph{row.subset} list element the corresponding \emph{nomatch} argument will be changed to \code{0} to perform inner join, otherwise it will be \code{getOption("datatable.nomatch")}. If you really want to do outer join to already filtered \emph{join} data.table you need to override corresponding \emph{nomatch} argument for \code{NA}. Cross table expressions and not supported inside \emph{joinbyv}.
#' @param nomatch list of integer scalars \code{NA} or \code{0} elements corresponding \code{join} data.tables. Default: \code{lapply(row.subset, function(x) if(is.expression(x)) 0 else getOption("datatable.nomatch"))}. Indicates outer join for \code{NA} and inner join for \code{0}. When \emph{data.table} extends allowed argument in the \code{getOption("datatable.nomatch")} then it should accept not only integer scalar but any value supported by \emph{data.table} as \emph{nomatch} argument.
#' @param allow.cartesian list of logical scalar elements corresponding to \code{join} data.tables to define which of the joins are allowed to do cartesian product. Default: \code{as.list(rep(getOption("datatable.allow.cartesian"),length(join)))}.
#' @details 
#' Any \code{NULL} inside the lists provided to \emph{by, col.subset, row.subset, nomatch, allow.cartesian} will be replaced by the default value for particular \code{NULL} element. Therefore it is possible to pass partially filled lists, length of each must match to length of \emph{join}, example \code{col.subset=list(NULL,c("join2_col1","join2_col2"),NULL,c("join4_col1"))}.
#' Function do not allow cross tables \emph{row.subset} filtering expressions. User should apply such filter after \code{joinbyv}, example: \code{joinbyv(master, join)[join1_colA > join2_colA * 2]}.
#' Arguments \emph{nomatch, allow.cartesian} by default will be setup according to \emph{data.table} options. An exception case described in \emph{row.subset} argument will by default override the \emph{nomatch} argument to value \code{0} to perform inner join.
#' Possibly the performance might be improved after implementing data.table FR #691 and #692.
#' @return
#' \emph{data.table}, denormalized master table joined to defined \emph{join} data.tables. Column order according to \code{col.subset}, no key.
#' @author Jan Gorecki
#' @seealso \link{build_hierarchy}, \link{dw.populate}
#' @export
#' @example tests/example-joinbyv.R
joinbyv <- function(master, join, by, col.subset, row.subset, nomatch, allow.cartesian){
  # basic input check: master and join
  stopifnot(all(!missing(master),!missing(join))) # non missing mandatory args
  if(!is.data.table(master)) master <- master[[1]] # conver master list(DT) to DT
  stopifnot(all(
    is.data.table(master), !is.data.table(join), is.list(join) # master is DT, join is list
  ))
  
  # defaults
  if(missing(by)) by = lapply(join, key)
  if(missing(col.subset)) col.subset = lapply(join, names)
  if(missing(row.subset)) row.subset = as.list(rep(TRUE,length(join)))
  if(missing(nomatch)) nomatch = lapply(row.subset, function(x) if(is.expression(x)) 0 else getOption("datatable.nomatch"))
  if(missing(allow.cartesian)) allow.cartesian = as.list(rep(getOption("datatable.allow.cartesian"),length(join)))
  
  # basic input check: all others
  stopifnot(all(
    is.list(by), is.list(col.subset), is.list(row.subset), is.list(nomatch), is.list(allow.cartesian) # all other args are lists
  ))
  
  # at least key(join) or by provided - raise error on missing
  valid_key <- mapply(join = join, by = by, 
                      FUN = function(join, by) if(is.null(by) & is.null(key(join))) FALSE else TRUE,
                      SIMPLIFY = FALSE)
  if(any(!(unlist(valid_key)))) stop(paste0("Missing key on 'join' data.table AND corresponding setkeyv vector in 'by' argument for join DT#: ",paste(which(!(unlist(valid_key))),collapse=", "),". Read: ?joinbyv."),call.=FALSE) # raise error cause it is mandatory
  # col.subset names match
  valid_col <- mapply(join = join, col.subset = col.subset, 
                      FUN = function(join, col.subset) if(!all(col.subset %in% names(join))) FALSE else TRUE,
                      SIMPLIFY = FALSE)
  if(any(!(unlist(valid_col)))) stop(paste0("Column names provided in 'col.subset' does not exists in corresponding 'join' object. Read: ?joinbyv"),call.=FALSE)
  # equal length for args
  stopifnot(length(unique(c(length(join),length(by),length(col.subset),length(row.subset),length(nomatch),length(allow.cartesian))))==1)
  
  # fill provided NULL with default value for each arg
  by <- mapply(join = join, by = by, FUN = function(join, by) if(is.null(by)) key(join) else by, SIMPLIFY=FALSE)
  col.subset <- mapply(join = join, col.subset = col.subset, FUN = function(join, col.subset) if(is.null(col.subset)) names(join) else col.subset, SIMPLIFY=FALSE)
  row.subset <- lapply(row.subset, FUN = function(row.subset) if(is.null(row.subset)) TRUE else row.subset)
  nomatch <- mapply(row.subset = row.subset, nomatch = nomatch, 
                    FUN = function(row.subset, nomatch){
                      return(if(is.null(nomatch)){
                        if(is.expression(row.subset)) 0 else getOption("datatable.nomatch")
                      } else nomatch)
                    }, SIMPLIFY = FALSE)
  allow.cartesian <- lapply(allow.cartesian,
                            FUN = function(allow.cartesian){
                              if(is.null(allow.cartesian)) getOption("datatable.allow.cartesian") else allow.cartesian
                            })
  
  # setkey on join tables
  invisible(mapply(join = join, by = by, i = seq_along(join),
                   FUN = function(join, by, i){
                     if(is.null(key(join))) setkeyv(join,by) else{
                       if(!identical(key(join),by)){
                         setkeyv(join,by)
                         warning(paste("Origin key of join element overwritten by key defined in 'by' argument for DT#:",i), call.=FALSE)
                       } else NULL
                     }
                   }, SIMPLIFY = FALSE))
  
  # check col.subset column names overlap handling
  lookup.cols <- unlist(col.subset)[!(unlist(col.subset) %in% unlist(lapply(join, key)))]
  if(length(unique(lookup.cols))!=length(lookup.cols)){
    non_unq <- unique(lookup.cols[lookup.cols %in% names(which(table(lookup.cols)>1))])
    warning(paste0("Column names overlaps in 'col.subset': ",paste(non_unq,collapse=", "),". Columns will be removed from the result. Check the uniqueness of 'col.subset' elements, specify unique column names to be kept, alternatively rename columns on input. Read: ?joinbyv"), call.=FALSE)
    col.subset <- lapply(col.subset, function(col.subset) col.subset[!(col.subset %in% non_unq)])
  }
  
  # joinby fun
  joinby <- function(master, join, by, col.subset, row.subset, nomatch, allow.cartesian){
    # loop level check: key(join) %in% names(master) - check case placed here to handle also dimension hierarchy in Snowflake schema where columns used for next joins are taken from previous joins, not from the initial master table.
    if(!all(key(join) %in% names(master))) stop(paste0("Missing columns ",paste(key(join)[!(key(join) %in% names(master))],collapse=", ")," in master table, cannot perform join."),call.=FALSE)
    # setkey on master
    if(!identical(key(master),key(join))) setkeyv(master,key(join)) # resorting issue to each join, possible improvement after FR: #691, #692.
    # join
    join[tryCatch(expr = eval(row.subset), # row.subset eval expression error handling
                  error = function(e) stop(paste0("Provided 'row.subset' expression results error: ",paste(as.character(c(e$call,e$message)),collapse=" : ")),call.=FALSE)),
         .SD, # col.subset
         .SDcols = unique(c(key(join),col.subset))
         ][master, # join master 
           nomatch = nomatch, # outer / inner join
           allow.cartesian = allow.cartesian # cartesian product allowed?
           ][, 
             .SD, # col.subset post-join, keep only columns asked in joinbyv function call
             .SDcols = unique(c(col.subset,names(master)))
             ]
  }
  # exec main loop
  for(i in 1:length(join)){
    master <- joinby(master = master, join[[i]], by[[i]], col.subset[[i]], row.subset[[i]], nomatch[[i]], allow.cartesian[[i]])
  }
  # clean last used key
  setkeyv(master,NULL)
}
