
# technical ---------------------------------------------------------------

#' @title has.spaces
#' @description test if character has spaces, accept vector, return scalar logical.
#' @keywords internal
has.spaces <- function(x){
  r = grepl(" ", x)
  if(length(unique(r)) > 1){
    stop("Provided character vector should be a vector of table names (must not contains spaces) or vector of sql statements (must contains spaces). Now it contains both types (some contains spaces, the others are not contains spaces).")
  }
  return(unique(r))
}

#' @title is.sql
#' @description test if character is.sql (has spaces), accept vector, return scalar logical.
#' @keywords internal
is.sql <- function(x){
  r = has.spaces(x)
  return(r)
}

#' @title is.sql.get
#' @description test if character is.sql.get, accept vector, return scalar logical.
#' @keywords internal
is.sql.get <- function(x){
  r = toupper(substr(x,1,7))=="SELECT "
  if(length(unique(r)) > 1){
    stop("Provided character vector of sql statements should start with 'SELECT ' for all the elements or for none of them.")
  }
  return(unique(r))
}

#' @title is.sql.send
#' @description test if character is.sql.send, accept vector, return scalar logical.
#' @keywords internal
is.sql.send <- function(x){
  r = toupper(substr(x,1,7))!="SELECT "
  if(length(unique(r)) > 1){
    stop("Provided character vector of sql statements should start with 'SELECT ' for all the elements or for none of them.")
  }
  return(unique(r))
}

#' @title is.table.name
#' @description test if character is.table.name (no spaces), accept vector, return scalar logical.
#' @keywords internal
is.table.name <- function(x){
  r = !has.spaces(x)
  return(r)
}

#' @title list.sub
#' @description Take \code{i} element of the list \code{x} if not present or NULL then return \code{fill}. Used in \code{db} for \dots argument processing.
#' @keywords internal
list.sub <- function(x, i, fill=NULL){
  if(length(x)<i || is.null(x[[i]])) r = fill
  else r = x[[i]]
  return(r)
}

# db helpers --------------------------------------------------------------

#' @title Auto table name generate
#' @description Automatic table name generation based on first ncol and first nchar of those cols, all pasted, suffix added as timestamp*1e3.
#' @param x character names of data.table.
#' @param ncol integer number of cols to use.
#' @param nchar integer number of characters of each col to use.
#' @export auto.table.name
auto.table.name <- function(x, ncol=getOption("dwtools.db.auto.table.name.ncol"), nchar=getOption("dwtools.db.auto.table.name.nchar")){
  col.names <- gsub(",","_",gsub(" ","_",tolower(substring(na.omit(x[1:ncol]), 1, nchar))))
  now <- as.POSIXlt(Sys.time())
  suffix <- paste0(as.character(now,"%Y%m%d%H%M"),sprintf("%05d", trunc(now$sec*1e3)))
  if(nchar(suffix)<17) paste0(suffix,paste(rep("0",17-nchar(suffix)),collapse=""))
  paste(paste(col.names,collapse="_"),suffix,sep="_")
}

# db -------------------------------------------------------------------

#' @title Simple database interface
#' @description Common db interface for DBI, RODBC and other custom defined off-memory storage. So far it was tested with SQLite, postgres and csv.
#' @param x data.table (to save in db) or character of table names or character of sql commands.
#' @param \dots if \code{x} is data.table then \dots expects character table names and character connection names else \dots expects only character connection names.
#' @param key character or integer, character vector to be used to set key or integer columns position to setkey.
#' @param .db.preprocess logical.
#' @param .db.postprocess logical.
#' @param .db.conns list of connections uniquely named.
#' @param .db.dict data.table db interface dictionary.
#' @param .db.batch.action character action name, use only when no recycling required, no action detection required, no timing required.
#' @param timing logical measure timing, make timings for each query in case of vectorized input, read \link{timing}.
#' @param verbose integer, if greater than 0 then print debugging messages.
#' @details Function is designed to be slim and chainable in data.table \code{`[`} operator. It accept vectorized input for all combinations of character arguments, see \emph{Multiple tables/connections} section.
#' \itemize{
#' \item \code{dbWriteTable} - \code{x} is data.table: \code{db(DT,"my_tab")}
#' \item \code{dbReadTable} - \code{x} character table name: \code{db("my_tab")}
#' \item \code{dbGetQuery} - \code{x} character with spaces and starts with \code{"SELECT "}: \code{db("SELECT col1 FROM my_tab1")}
#' \item \code{dbSendQuery} - \code{x} character with spaces and \strong{not} starts with \code{"SELECT "}: \code{db("UPDATE my_tab1 SET col1 = NULL")}
#' }
#' @note In case of \emph{get} and \emph{send} actions any semicolon \emph{;} sign as the last char will be removed from query. When \emph{write} is used it will also set \emph{tablename} attribute by reference to a data.table which was written to database, because it is done by reference it will alter input data.table also and overwrite any previous \emph{tablename} attribute, you can always use \code{newDT <- db(copy(DT),NULL)} to keep both.
#' @return In case of \strong{write / read / get} the data.table object (possibly with some extra attributes). In case of \strong{send} action the send query results.
#' @section Multiple tables/connections:
#' Table names, sql commands, connection names can be character vectors. It allows processing into multiple connections and tables at once. The list of results will be returned, except the \emph{write} action where single data.table will be always returned (for chaining). It will be named by the connection names, so if the connecion name was recycled (e.g. \code{db(c("my_tab1","my_tab2"))}) then there will be duplicated names in the resulted list.
#' @section Limitations:
#' Table names must not contains spaces (which are accepted by some db vendors).\cr
#' SQL send statements should contains spaces, e.g. sqlite \code{.tables} command needs to be written as \code{db("SELECT * FROM sqlite_master WHERE type='table'")}.\cr
#' Below are the per driver name limitations:
#' \itemize{
#' \item \code{csv}: No \strong{get} and \strong{send} actions. Extension \emph{.csv} is automatically added to provided table name character (or to \link{auto.table.name} if table name was not provided).
#' \item \code{JDBC}: Might not support \emph{append} for all jdbc drivers.
#' }
#' @section Auto table name:
#' If writing to db and table name is missing or NULL then the \link{auto.table.name} will be used, auto generated tablename can be catched for later use by \code{(attr(DT,"tablename",TRUE)}, read note section.
#' @section DB interface dictionary:
#' If you read/write to non-default schema you should use \code{"my_schema1.my_tab1"} table names, it will be translated to expected format for target db, e.g. for postgres: \code{c("my_schema1","my_tabl1")}.\cr
#' SQL statements are of course not unified but most of the syntax is already common across different db.\cr
#' There are preprocessing and postprocessing functions available per defined db driver. Those functions can be used for seemless integration in case if write/read to db lose classes of the data. This gives R ability to act as data hub and gain value as ETL tool.\cr
#' You can add new db interfaces by extending \link{db_dict}. Pull Requests are welcome.
#' @seealso \link{dbCopy}, \link{timing}
#' @export
#' @example tests/example-db.R
db <- function(x, ..., key,
               .db.preprocess = getOption("dwtools.db.preprocess"),
               .db.postprocess = getOption("dwtools.db.postprocess"),
               .db.conns = getOption("dwtools.db.conns"),
               .db.dict = getOption("dwtools.db.dict"),
               .db.batch.action = getOption("dwtools.db.batch.action"),
               timing = getOption("dwtools.timing"),
               verbose = getOption("dwtools.verbose")){
  
  if(is.null(.db.batch.action)){
    
    ### validate input
    
    if(missing(x)){
      stop("x argument must be provided to db function")
    } # stop on missing 'x'
    if(is.null(.db.conns) || length(.db.conns)==0){
      if(!getOption("dwtools.db.silent.drvName.csv",FALSE)) warning("You should define 'dwtools.db.conns' option to route db requests, `options('dwtools.db.conns'=list(source1=source1,source2=source2))`, read ?db examples. It will use csv connection now. You can suppress the warning by setting `options('dwtools.db.silent.drvName.csv'=TRUE)`")
      .db.conns = list(csv1 = list(drvName = "csv"))
    } # warning if connections not defined, set csv connection
    if(is.list(.db.conns) && !is.list(.db.conns[[1]])){
      stop("Correct 'dwtools.db.conns' option should be list of uniquely named lists, one for each connection, even if there is only one. Fix your connections definition, see examples.")
    } # stop on incorrect 'dwtools.db.conns'
    if(!is.character(x) && !is.data.table(x)){
      stop("Argument 'x' should be single data.table or character (can be vector) sql statement or table name.")
    } # stop when 'x' is not character or data.table
    
    ### catch dots
    
    dots = list(...) # magic ui decoder
    
    ### recycle
    
    ## write:
    # 1 DT save to 1 table in 1 conn
    # 1 DT save to 1 table in X conns
    # 1 DT save to X tables in X conns
    # 1 DT save to X tables in 1 conn
    ## read:
    # 1 table in 1 conn
    # 1 table in X conns
    # X tables in X conns
    # X tables in 1 conn
    ## get/send:
    # 1 sql in 1 conn
    # 1 sql in X conns
    # X sqls in X conns
    # X tables in 1 conn
    name <- NULL
    if(missing(key)) key <- NULL
    if(is.data.table(x)){
      action = "write"
      name = list.sub(x=dots,i=1,fill=NULL)
      conn.name = list.sub(x=dots,i=2,fill=names(.db.conns[1]))
      if(!is.null(name) && length(conn.name)==1 && length(name)>1) conn.name = rep(conn.name,length(name))
      if(!is.null(name) && length(conn.name)>1 && length(name)==1) name = rep(name,length(conn.name))
      if(is.null(name)) name = vector(mode="list",length(conn.name))
    } # write
    else if(is.character(x) && is.table.name(x)){
      action = "read"
      conn.name = list.sub(x=dots,i=1,fill=names(.db.conns[1]))
      if(length(conn.name)==1 && length(x)>1) conn.name = rep(conn.name,length(x))
      if(length(conn.name)>1 && length(x)==1) x = rep(x,length(conn.name))
    } # read
    else if(is.character(x) && is.sql(x) && is.sql.get(x)){
      action = "get"
      conn.name = list.sub(x=dots,i=1,fill=names(.db.conns[1]))
      x <- vapply(x, function(x) if(substr(x,nchar(x),nchar(x))==";") x <- substr(x,1,nchar(x)-1) else x, "", USE.NAMES=FALSE)
      if(length(conn.name)==1 && length(x)>1) conn.name = rep(conn.name,length(x))
      if(length(conn.name)>1 && length(x)==1) x = rep(x,length(conn.name))
    } # get
    else if(is.character(x) && is.sql(x) && is.sql.send(x)){
      action = "send"
      conn.name = list.sub(x=dots,i=1,fill=names(.db.conns[1]))
      x <- vapply(x, function(x) if(substr(x,nchar(x),nchar(x))==";") x <- substr(x,1,nchar(x)-1) else x, "", USE.NAMES=FALSE)
      if(length(conn.name)==1 && length(x)>1) conn.name = rep(conn.name,length(x))
      if(length(conn.name)>1 && length(x)==1) x = rep(x,length(conn.name))
    } # send
    else{
      stop("Unsupported input, `db` expects `x` as data.table / character sql statement / character table name.")
    } # else error
    
    ### skip batch processing for scalar input
    
    if(!isTRUE(timing) && verbose == 0L && length(conn.name)==1L) .db.batch.action <- action
    
  } # run only once for db(), skip on on vectorized input
  else{ # single process
    # same as above but no validate input and no recycle
    dots = list(...) # magic ui decoder
    if(.db.batch.action=="write"){
      action = "write"
      name = list.sub(x=dots,i=1,fill=NULL)
      conn.name = list.sub(x=dots,i=2,fill=names(.db.conns[1]))
    }
    else if(.db.batch.action %in% c("read","get","send")){
      action = "read"
      name = NULL
      conn.name = list.sub(x=dots,i=1,fill=names(.db.conns[1]))
    }
  } # run only for vectorized input, or on demand by param
  
  ### batch processing
  
  if(is.null(.db.batch.action)){
    pretty_log_on_timing <- function(i, x, name, conn.name, key, .timing, verbose){
      action <- getOption("dwtools.db.batch.action")
      if(action=="write" && is.null(name[[i]])) name[[i]] <- auto.table.name(names(x))
      r <- eval(bquote(
        switch(action,
               write = invisible(timing(db(x,.(name[[i]]),.(conn.name[[i]])), nrow(x), tag=paste("db",action,name[[i]],conn.name[[i]],sep=getOption("dwtools.tag.sep",";")), .timing=.timing, verbose=verbose)),
               read = if(is.null(key)){
                 timing(db(.(x[[i]]),.(conn.name[[i]])), NA_integer_, tag=paste("db",action,x[[i]],conn.name[[i]],sep=getOption("dwtools.tag.sep",";")), .timing=.timing, verbose=verbose)[]
               } else {
                 timing(db(.(x[[i]]),.(conn.name[[i]]), key=.(key)), NA_integer_, tag=paste("db",action,x[[i]],conn.name[[i]],sep=getOption("dwtools.tag.sep",";")), .timing=.timing, verbose=verbose)[]
               },
               get = if(is.null(key)){
                 timing(db(.(x[[i]]),.(conn.name[[i]])), NA_integer_, tag=paste("db",action,x[[i]],conn.name[[i]],sep=getOption("dwtools.tag.sep",";")), .timing=.timing, verbose=verbose)[]
                } else {
                  timing(db(.(x[[i]]),.(conn.name[[i]]), key=.(key)), NA_integer_, tag=paste("db",action,x[[i]],conn.name[[i]],sep=getOption("dwtools.tag.sep",";")), .timing=.timing, verbose=verbose)[]
                },
               send = invisible(timing(db(.(x[[i]]),.(conn.name[[i]])), NA_integer_, tag=paste("db",action,x[[i]],conn.name[[i]],sep=getOption("dwtools.tag.sep",";")), .timing=.timing, verbose=verbose)),
               stop("not supported action, reset `options('dwtools.db.batch.action'=NULL)` may help, read ?db"))
      ))
      # for batch processing on write the single process should not return DT, whole batch must return single DT, so it will return tablename of DT, yet retain correct out_n in timings
      if(action=="write") r <- name[[i]]
      return(r)
    }
    r <- devtools::with_options( # using options it is possible to have cleaner expression field in timing logs
      new=c("dwtools.db.batch.action"=action, # used in inner db(), NOT ONLY in pretty_log_on_timing where setting options would be redundant
            "dwtools.timing"=FALSE),
      code=lapply(setNames(1:length(conn.name),conn.name), pretty_log_on_timing, x=x, name=name, conn.name=conn.name, key=key, .timing=timing, verbose=verbose)
    )
    if(action=="write"){
      tbls <- unlist(r)
      r <- x
      setattr(r,"tablename",tbls)
    }
    else if(length(r) == 1L){ # single process but with timing
      r <- r[[1]]
    }
    return(if(action %in% c("write","send")) invisible(r) else r)
  }
  
  ### single processing
  
  # write: 1 DT save to 1 table in 1 conn
  # read: 1 table in 1 conn
  # get/send: 1 sql in 1 conn
  
  .db.conn = .db.conns[[conn.name]]
  if(.db.batch.action=="write"){ # write
    if(is.list(name)) name <- name[[1]]
    if(is.null(name)) name <- auto.table.name(names(x))
    r = .db.dict[.(.db.conn$drvName), write[[1]](conn = .db.conn$conn, name = tablename[[1]](name), value = if(.db.preprocess) preprocess[[1]](x) else x)]
    if(is.null(getOption("dwtools.db.batch.action"))) setattr(x,"tablename",setNames(name,conn.name)) # only for non batch process
  } # write
  else if(.db.batch.action=="read"){ # read
    x = .db.dict[.(.db.conn$drvName), read[[1]](conn = .db.conn$conn, name = tablename[[1]](x))]
    if(is.data.frame(x)){
      setDT(x)
      if(.db.postprocess) x = .db.dict[.(.db.conn$drvName), postprocess[[1]]](x)
    }
  } # read
  else if(.db.batch.action=="get"){ # get
    x = .db.dict[.(.db.conn$drvName), get[[1]]](conn = .db.conn$conn, statement = x)
    if(is.data.frame(x)){
      setDT(x)
      if(.db.postprocess) x = .db.dict[.(.db.conn$drvName), postprocess[[1]]](x)
    }
  } # get
  else if(.db.batch.action=="send"){ # send
    x = .db.dict[.(.db.conn$drvName), send[[1]](conn = .db.conn$conn, statement = x)] # here x may not be a data.table but also a non-table send query results
    if(is.data.frame(x)){
      setDT(x)
      if(.db.postprocess) x = .db.dict[.(.db.conn$drvName), postprocess[[1]]](x)
    }
  } # send
  
  # setkey
  if(!missing(key) && !is.null(key) && .db.batch.action %in% c("read","get") && is.data.table(x)){
    if(is.numeric(key)) nosetkey <- (max(key) > length(x))
    else if(is.character(key)) nosetkey <- !all(key %in% names(x))
    if(nosetkey){
      warning("key argument provided to db function attempt to setkey on non existing columns")
    } else {
      if(is.numeric(key)) key <- names(x)[as.integer(key)]
      setkeyv(x,key)[]
    }
  }
  
  return(if(.db.batch.action %in% c("write","send")) invisible(x) else x)
}

# db migration --------------------------------------------------------

#' @title Copy tables between databases
#' @param source.table.name character vector of tables names to copy from source connection
#' @param source.conn.name character scalar
#' @param target.table.name character vector of tables names to copy to target connection
#' @param target.conn.name character scalar
#' @param timing logical measure timing
#' @param verbose integer status messages
#' @seealso \link{db}, \link{timing}
#' @export
#' @examples
#' # see the last example in ?db
dbCopy <- function(source.table.name, source.conn.name, target.table.name, target.conn.name, timing=getOption("dwtools.timing"), verbose=getOption("dwtools.verbose")){
  stopifnot(length(source.conn.name)==1 && length(target.conn.name)==1)
  stopifnot(length(source.table.name)==length(target.table.name))
  # do one copy
  dbCopy.one <- function(source.table.name, source.conn.name, target.table.name, target.conn.name, timing, verbose){
    db(
      db(source.table.name, source.conn.name, timing=timing, verbose=verbose),
      target.table.name,
      target.conn.name,
      timing=timing,
      verbose=verbose
    )
  }
  # batch copy
  mx = mapply(dbCopy.one, source.table.name, target.table.name, MoreArgs = list(source.conn.name=source.conn.name, target.conn.name=target.conn.name, timing=timing, verbose=verbose-1), SIMPLIFY = FALSE)
  invisible(mx)
}
