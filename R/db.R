
# technical ---------------------------------------------------------------

#' @title auto.table.name
#' @description Automatic table name generation based on first ncols and first nchars of those cols, all pasted, suffix added as timestamp*1e3.
#' @keywords internal
auto.table.name <- function(x, ncol=getOption("dwtools.db.auto.table.name.ncol"), nchar=getOption("dwtools.db.auto.table.name.nchar")){
  # TO DO: better regex
  col.names <- gsub(",","_",gsub(" ","_",tolower(substring(na.omit(x[1:ncol]), 1, nchar))))
  now <- as.POSIXlt(Sys.time())
  suffix <- paste0(as.character(now,"%Y%m%d%H%M"),as.character(trunc(now$sec*1e3)))
  if(nchar(suffix)<17) paste0(suffix,paste(rep("0",17-nchar(suffix)),collapse=""))
  paste(paste(col.names,collapse="_"),suffix,sep="_")
}

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

# db -------------------------------------------------------------------

#' @title Simple database interface
#' @description Common db interface for DBI, RODBC and other custom defined off-memory storage. So far it was tested with SQLite, postgres and csv.
#' @param x data.table (to save in db) or character of table names or character of sql commands.
#' @param \dots if \code{x} is data.table then \dots expects character table names and character connection names else \dots expects only character connection names.
#' @param key character vector to be used to set key or integer columns position, cannot be mixed with multiple connections queries, see examples for chaining in DT syntax.
#' @param .db.preprocess logical.
#' @param .db.postprocess logical.
#' @param .db.conns list of connections uniquely named.
#' @param .db.dict data.table db interface dictionary.
#' @param .db.action character action name, use only when no recycling required and action detection not required. Supported values: write, read, get, send. User should not use following option with any other value then `options("dwtools.tmp.db.action"=NULL)` as it will lose the timing if turned on.
#' @param timing logical measure timing for vectorized usage, read \link{timing}, for scalar arguments it might be better to use \code{timing(db(...))}.
#' @param verbose integer, if greater than 0 then print debugging messages.
#' @details Function is designed to be slim and chainable in data.table \code{`[`} operator.
#' \itemize{
#' \item \code{dbWriteTable} - \code{x} is data.table: \code{db(DT,"my_tab")}
#' \item \code{dbReadTable} - \code{x} character table name: \code{db("my_tab")}
#' \item \code{dbGetQuery} - \code{x} character with spaces and starts with \code{"SELECT "}: \code{db("SELECT col1 FROM my_tab1")}
#' \item \code{dbSendQuery} - \code{x} character with spaces and \strong{not} starts with \code{"SELECT "}: \code{db("UPDATE my_tab1 SET col1 = NULL")}
#' }
#' @note In case of \emph{get} and \emph{send} actions any semicolon \emph{;} sign as the last char will be removed from query.
#' @return In case of \strong{write / read / get} the data.table object (possibly with some extra attributes), in case of \strong{send} action the send query results.
#' @section Multiple connections:
#' Table names, sql commands, connection names can be character vectors. It allows processing into multiple connections and tables at once. The list of results will be returned, it will be named by the connection names, so if the connecion name was recycled (e.g. \code{db(c("my_tab1","my_tab2"))}) then there will be duplicated names in the resulted list.
#' @section Limitations:
#' Table names must not contains spaces (which are accepted by some db vendors).\cr
#' SQL send statements should contains spaces. E.g. sqlite \code{.tables} command will need to be written as \code{db("SELECT * FROM sqlite_master WHERE type='table'")}.\cr
#' Below are the per driver name limitations:
#' \itemize{
#' \item \code{csv}: No \strong{get} and \strong{send} actions. Extension \emph{.csv} is automatically added to provided table name character (or to \link{auto.table.name} if table name was not provided).
#' }
#' @section Auto table name:
#' If writing to db and table name is missing or NULL then the \link{auto.table.name} will be used. The table name used in \strong{write} will be provided as \code{"tablename"} attribute of the function result so it can be catched for later use.
#' @section DB interface dictionary:
#' If you read/write to non-default schema you should use \code{"my_schema1.my_tab1"} table names, it will be translated to expected format for target db (e.g. for postgres: \code{c("my_schema1","my_tabl1")}).\cr
#' SQL statements are not unified but most of the syntax is already common across different db.\cr
#' There are preprocessing and postprocessing functions available per defined db driver. Those functions can be used for seemless integration in case if write/read to db lose classes of the data.\cr
#' This gives R ability to act as data hub and gain value as ETL tool.\cr
#' You can add new interfaces by extending \link{db_dict}. Pull Requests are welcome.
#' @seealso \link{dbCopy}, \link{timing}
#' @export
#' @example tests/example-db.R
db <- function(x, ..., key,
               .db.preprocess = getOption("dwtools.db.preprocess"),
               .db.postprocess = getOption("dwtools.db.postprocess"),
               .db.conns = getOption("dwtools.db.conns"),
               .db.dict = getOption("dwtools.db.dict"),
               .db.action = getOption("dwtools.tmp.db.action"),
               timing = getOption("dwtools.timing"),
               verbose = getOption("dwtools.verbose")){
  
  ### single-batch indicator
  
  # Designed using options to achieve cleaner (deparsed expressions) and well readable logs. Use `timing=TRUE` to see effect.
  
  ### validat input, catch dots, recycle, set defaults, skip for batch processing
  
  if(is.null(.db.action) || !is.character(.db.action) || !(.db.action %in% c("write","read","get","send"))){
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
    
    dots = list(...) # magic ui decoder
    
    ## recycle
    # write:
    # 1 DT save to 1 table in 1 conn
    # 1 DT save to 1 table in X conns
    # 1 DT save to X tables in X conns
    # 1 DT save to X tables in 1 conn
    # read:
    # 1 table in 1 conn
    # 1 table in X conns
    # X tables in X conns
    # X tables in 1 conn
    # get/send:
    # 1 sql in 1 conn
    # 1 sql in X conns
    # X sqls in X conns
    # X tables in 1 conn
    
    name <- NULL
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
  }
  
  ### batch processing
  
  if(is.null(.db.action)){
    pretty_log_on_timing <- function(i, x, name, conn.name, timing, verbose){
      action <- getOption("dwtools.tmp.db.action")
      if(action=="write" && is.null(name[[i]])) name[[i]] <- auto.table.name(names(x))
      r <- eval(bquote(
        switch(action,
               write = invisible(timing(db(x,.(name[[i]]),.(conn.name[[i]])), nrow(x), tag=paste("db",action,name[[i]],conn.name[[i]],sep=getOption("dwtools.tag.sep",";")), .timing=timing, verbose=verbose)),
               read = timing(db(.(x[[i]]),.(conn.name[[i]])), NA_integer_, tag=paste("db",action,x[[i]],conn.name[[i]],sep=getOption("dwtools.tag.sep",";")), .timing=timing, verbose=verbose)[],
               get = timing(db(.(x[[i]]),.(conn.name[[i]])), NA_integer_, tag=paste("db",action,x[[i]],conn.name[[i]],sep=getOption("dwtools.tag.sep",";")), .timing=timing, verbose=verbose)[],
               send = invisible(timing(db(.(x[[i]]),.(conn.name[[i]])), NA_integer_, tag=paste("db",action,x[[i]],conn.name[[i]],sep=getOption("dwtools.tag.sep",";")), .timing=timing, verbose=verbose)),
               stop("not supported action, reset `options('dwtools.tmp.db.action'=NULL)` may help, read ?db"))
      ))
      if(timing && is.null(getOption("dwtools.timing.conn.name"))){ # suppress union of attr to handle outside of lapply, to do not double entries when setattr by reference
        rtiming[[i]] <<- attr(r,"timing",TRUE)
        setattr(r,"timing",NULL)
      }
      r
    }
    return({
      if(timing && is.null(getOption("dwtools.timing.conn.name"))) rtiming <- vector(mode="list",length(conn.name))
      r <- devtools::with_options( # using options it is possible to have cleaner tags in timing logs
        new=c("dwtools.tmp.db.action"=action,"dwtools.timing"=FALSE,"dwtools.verbose"=verbose-1L,"dwtools.timing.append"=FALSE),
        code=lapply(setNames(1:length(conn.name),conn.name), pretty_log_on_timing, x=x, name=name, conn.name=conn.name, timing=timing, verbose=verbose)
      )
      if(action=="write") r <- x
      else if(length(r)==1){ # setkey only for scalar inputs
        r <- r[[1]]
        if(is.data.table(r) && !missing(key) && !is.null(key)){
          if(is.numeric(key)) key <- names(x)[as.integer(key)] # support for column index instead of name
          setkeyv(r,key)[]
        }
      }
      if(timing && is.null(getOption("dwtools.timing.conn.name"))){ # union timing in attributes
        setattr(r, "timing", rbindlist(rtiming))[]
      }
      if(action %in% c("write","send")) invisible(r) else r
    })
  }
  
  ### single processing
  
  ## already single execution
  # write: 1 DT save to 1 table in 1 conn
  # read: 1 table in 1 conn
  # get/send: 1 sql in 1 conn
  
  dots = list(...)
  
  # action
  if(.db.action=="write"){ # write
    name = list.sub(x=dots,i=1,fill=NULL)
    conn.name = list.sub(x=dots,i=2,fill=NULL)
    stopifnot(!is.null(name),!is.null(conn.name))
    .db.conn = .db.conns[[conn.name]]
    r = .db.dict[.(.db.conn$drvName), write[[1]](conn = .db.conn$conn, name = tablename[[1]](name), value = if(.db.preprocess) preprocess[[1]](x) else x)]
    setattr(x,"tablename",name)
  } # write
  else if(.db.action=="read"){ # read
    conn.name = list.sub(x=dots,i=1,fill=NULL)
    stopifnot(!is.null(conn.name))
    .db.conn = .db.conns[[conn.name]]
    x = .db.dict[.(.db.conn$drvName), read[[1]](conn = .db.conn$conn, name = tablename[[1]](x))]
    if(is.data.frame(x)){
      setDT(x)
      if(.db.postprocess) x = .db.dict[.(.db.conn$drvName), postprocess[[1]]](x)
    }
  } # read
  else if(.db.action=="get"){ # get
    conn.name = list.sub(x=dots,i=1,fill=NULL)
    stopifnot(!is.null(conn.name))
    .db.conn = .db.conns[[conn.name]]
    x = .db.dict[.(.db.conn$drvName), get[[1]]](conn = .db.conn$conn, statement = x)
    if(is.data.frame(x)){
      setDT(x)
      if(.db.postprocess) x = .db.dict[.(.db.conn$drvName), postprocess[[1]]](x)
    }
  } # get
  else if(.db.action=="send"){ # send
    conn.name = list.sub(x=dots,i=1,fill=NULL)
    stopifnot(!is.null(conn.name))
    .db.conn = .db.conns[[conn.name]]
    x = .db.dict[.(.db.conn$drvName), send[[1]](conn = .db.conn$conn, statement = x)] # here x may not be a data.table but also a non-table send query results
    if(is.data.frame(x)){
      setDT(x)
      if(.db.postprocess) x = .db.dict[.(.db.conn$drvName), postprocess[[1]]](x)
    }
  } # send
  
  return(x)
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
  dbCopy.one <- function(source.table.name, source.conn.name, target.table.name, target.conn.name, .timing, verbose){
    if(.timing==TRUE) x = eval(bquote(timing(
      db(
        x <- db(.(source.table.name), .(source.conn.name)), # x <- db() # required for in.n
        .(target.table.name),
        .(target.conn.name)
      ),
      in.n = nrowDTlengthVec(x),
      .timing = .timing,
      verbose = 0
    ))) # log argument values
    else x = db(
      db(source.table.name, source.conn.name),
      target.table.name,
      target.conn.name
    )
    if(verbose > 0) cat(paste0(as.character(Sys.time()),": dbCopy.one; copy: ",paste(source.table.name,source.conn.name,sep="@")," to ",paste(target.table.name,target.conn.name,sep="@")),"\n",sep="")
    x
  }
  # batch copy
  mx = mapply(dbCopy.one, source.table.name, target.table.name, MoreArgs = list(source.conn.name=source.conn.name, target.conn.name=target.conn.name, .timing=timing, verbose=verbose-1), SIMPLIFY = FALSE)
  if(verbose > 0) cat(paste0(as.character(Sys.time()),": dbCopy; copy from ",paste(source.conn.name,target.conn.name,sep=" to ")," completed for ",length(target.table.name)," tables"),"\n",sep="")
  invisible(mx)
}
