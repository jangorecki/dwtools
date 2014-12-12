
# technical ---------------------------------------------------------------

#' @title auto.table.name
#' @description Automatic table name generation based on first ncols and first nchars of those cols, all pasted, suffix added as timestamp*1e3.
#' @keywords internal
auto.table.name <- function(x, ncol=getOption("dwtools.db.auto.table.name.ncol"), nchar=getOption("dwtools.db.auto.table.name.nchar")){
  # TO DO: better regex
  col.names <- gsub(",","_",gsub(" ","_",toupper(substring(na.omit(x[1:ncol]), 1, nchar))))
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
#' @param key character vector to be used to set key, cannot be mixed with multiple connections queries, see examples for chaining in DT syntax.
#' @param .db.preprocess logical.
#' @param .db.postprocess logical.
#' @param .db.conns list of connections uniquely named.
#' @param .db.dict data.table db interface dictionary.
#' @param timing logical measure timing for vectorized usage, read \link{timing}, for scalar arguments it might be better to use \code{timing(db(...))}.
#' @param verbose integer, if greater than 0 then print debugging messages.
#' @details Function is designed to be slim and chainable in data.table \code{`[`} operator.
#' \itemize{
#' \item \code{dbWriteTable} - \code{x} is data.table: \code{db(DT,"my_tab")}
#' \item \code{dbReadTable} - \code{x} character table name: \code{db("my_tab")}
#' \item \code{dbGetQuery} - \code{x} character with spaces and starts with \code{"SELECT "}: \code{db("SELECT col1 FROM my_tab1")}
#' \item \code{dbSendQuery} - \code{x} character with spaces and \strong{not} starts with \code{"SELECT "}: \code{db("UPDATE my_tab1 SET col1 = NULL")}
#' }
#' @return In case of \strong{write / read / get} the data.table object (possibly with some extra attributes), in case of \strong{send} action the send query results.
#' @section Multiple connections:
#' Table names, sql commands, connection names can be character vectors. It allows processing into multiple connections and tables at once. The list of results will be returned, it will be named by the connection names, so if the connecion name was recycled (e.g. \code{db(c("my_tab1","my_tab2"))}) then there will be duplicated names in the resulted list.
#' @section Limitations:
#' Table names must not contains spaces (which are accepted by some db vendors).\cr
#' SQL send statements should contains spaces. E.g. sqlite \code{.tables} command will need to be written as \code{db("SELECT * FROM sqlite_master WHERE type='table'")}.\cr
#' Below are the per driver name limitations:
#' \itemize{
#' \item \code{csv}: No \strong{get} and \strong{send} actions. Extension \emph{.csv} is automatically added to provided table name character (or to \link{auto.table.name} in table name was not provided).
#' }
#' @section Auto table name:
#' If writing to db and table name is missing or NULL then the \link{auto.table.name} will be used. The table name used in \strong{write} will be provided as \code{"tablename"} attribute of the function result so it can be catched for later use.
#' @section DB interface dictionary:
#' If you read/write to non-default schema use the \code{"my_schema1.my_tab1"} table names, it will be translated to expected format for target db (e.g. for postgres: \code{c("my_schema1","my_tabl1")}).\cr
#' SQL statements are not unified.\cr
#' There are preprocessing and postprocessing functions available per defined db driver. Those functions can be used for seemless integration in case if write/read to db lose classes of the data.\cr
#' This gives R ability to act as data hub and gain value as ETL tool.\cr
#' You can add new interfaces by extending \link{db_dict}. Pull Requests are welcome.
#' @seealso \link{dbCopy}, \link{timing}
#' @export
#' @example tests/db_examples.R
db <- function(x, ..., key,
               .db.preprocess = getOption("dwtools.db.preprocess"),
               .db.postprocess = getOption("dwtools.db.postprocess"),
               .db.conns = getOption("dwtools.db.conns"),
               .db.dict = getOption("dwtools.db.dict"),
               timing = getOption("dwtools.timing"),
               verbose = getOption("dwtools.verbose")){
  
  #### Validate inputs, catch name and conn.name, set defaults
  
  if(missing(x)){
    stop("x argument must be provided to db function")
  } # stop on missing 'x'
  if(is.null(.db.conns) || length(.db.conns)==0){
    if(!getOption("dwtools.db.silent.drvName.csv",FALSE)) warning("You must define 'dwtools.db.conns' option to route db requests, `options('dwtools.db.conns'=list(source1=source1,source2=source2))`. Read ?db examples")
    .db.conns = list(csv1 = list(drvName = "csv"))
  } # warning if connections not defined, set csv temp file
  if(is.list(.db.conns) && !is.list(.db.conns[[1]])){
    stop("Correct 'dwtools.db.conns' option should be list of uniquely named lists each by connection, even if there is only one. Fix your connections definition, see examples.")
  } # stop on incorrect 'dwtools.db.conns'
  if(is.list(x) && !is.data.table(x)){
    stop("Argument 'x' should be single data.table or character (can be vector) sql statement or table name.")
  } # stop when 'x' is list but not data.table, list of DTs not accepted
  
  dots = list(...) # magic ui decoder
   
  if(is.data.table(x)){
    action = "write"
    name = list.sub(x=dots,i=1,fill=auto.table.name(names(x)))
    conn.name = list.sub(x=dots,i=2,fill=names(.db.conns[1]))
    sql = list(NULL)
    visibility = expression(invisible(x[])) # closing function, write returns invisibly
    if(length(conn.name)==1 && length(name)>1) conn.name = rep(conn.name,length(name))
  } # write
  else if(is.character(x) && is.table.name(x)){
    action = "read"
    name = x
    conn.name = list.sub(x=dots,i=1,fill=names(.db.conns[1])) # 
    sql = list(NULL)
    visibility = expression(x[]) # closing function, get returns visibily
    if(length(conn.name)==1 && length(name)>1) conn.name = rep(conn.name,length(name))
  } # read
  else if(is.character(x) && is.sql(x) && is.sql.get(x)){
    action = "get"
    name = list(NULL)
    conn.name = list.sub(x=dots,i=1,fill=names(.db.conns[1]))
    sql = x
    visibility = expression(x[]) # closing function, query returns visibily
    if(length(conn.name)==1 && length(sql)>1) conn.name = rep(conn.name,length(sql))
  } # get
  else if(is.character(x) && is.sql(x) && is.sql.send(x)){
    action = "send"
    name = list(NULL)
    conn.name = list.sub(x=dots,i=1,fill=names(.db.conns[1]))
    sql = x
    visibility = expression(invisible(x)) # closing function, send returns visibily
    if(length(conn.name)==1 && length(sql)>1) conn.name = rep(conn.name,length(sql))
  } # send
  else{
    stop("Unsupported input, `db` expects `x` as data.table / character sql statement / character table name.")
  } # else error 
  
  #### Define single execution
  # write: 1 DT save to 1 table in 1 conn
  # read: 1 table in 1 conn
  # get/send: 1 sql in 1 conn
  
  db.one <- function(conn.name, sql, name, action, DT, .db.conns, .db.dict, verbose){
    .db.conn = .db.conns[[conn.name]]
    msg <- paste0(as.character(Sys.time()),": db.one")
    if(getOption("dwtools.db.one.debug",FALSE)) browser()
    # action
    if(action=="write"){ # write
      if(verbose > 0) msg <- paste0(msg,"; write into db table ",name," in ",conn.name)
      r = .db.dict[.(.db.conn$drvName), write[[1]](conn = .db.conn$conn, name = tablename[[1]](name), value = if(.db.preprocess) preprocess[[1]](DT) else DT)]
      setattr(DT,"tablename",name)
    } # write
    else if(action=="read"){ # read
      if(verbose > 0) msg <- paste0(msg,"; read from db table ",name," in ",conn.name)
      DT = .db.dict[.(.db.conn$drvName), read[[1]](conn = .db.conn$conn, name = tablename[[1]](name))]
      #DT = .db.dict[.(.db.conn$drvName), read[[1]]](conn = .db.conn$conn, name = name) # TODO name translation to c("schema","tbl") # TODO remove
      if(is.data.frame(DT)){
        setDT(DT)
        if(.db.postprocess) DT = .db.dict[.(.db.conn$drvName), postprocess[[1]]](DT)
      }
    } # read
    else if(action=="get"){ # get
      if(verbose > 0) msg <- paste0(msg,"; get from db statement@",conn.name,": ",sql)
      DT = .db.dict[.(.db.conn$drvName), get[[1]]](conn = .db.conn$conn, statement = sql)
      if(is.data.frame(DT)){
        setDT(DT)
        if(.db.postprocess) DT = .db.dict[.(.db.conn$drvName), postprocess[[1]]](DT)
      }
    } # get
    else if(action=="send"){ # send
      if(verbose > 0) msg <- paste0(msg,"; send to db statement@",conn.name,": ",sql)
      DT = .db.dict[.(.db.conn$drvName), send[[1]](conn = .db.conn$conn, statement = sql)] # here DT may not a DT but send query results
      if(is.data.frame(DT)){
        setDT(DT)
        if(.db.postprocess) DT = .db.dict[.(.db.conn$drvName), postprocess[[1]]](DT)
      }
      } # send
    if(verbose > 0) cat(msg,"; processed ",action," in ",conn.name,"\n",sep="")
    return(DT)
  }
  db.one.is.timing <- function(conn.name, sql, name, action, DT, .db.conns, .db.dict, .timing, verbose){
    if(.timing==TRUE) return(eval(bquote(
      timing(db.one(conn.name=.(conn.name), sql=.(sql), name=.(name), action=.(action), DT=DT, .db.conns=.db.conns, .db.dict=.db.dict, verbose=.(verbose)),
             nrow_in = nrowDT(DT),
             .timing = .timing,
             verbose = verbose-1)
    ))) # log argument values
    db.one(conn.name=conn.name, sql=sql, name=name, action=action, DT=DT, .db.conns=.db.conns, .db.dict=.db.dict, verbose=verbose)
  }
  
  #### Recycle inputs and execute
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
  
  N = length(conn.name)
  msg <- paste0(as.character(Sys.time()),": db")
  if(N == 1){
    if(length(sql)!=N) stop("Invalid sql statement length, sql statements length should be equal to 1 (to be recycled) or it should match to connection names length or .") # TODO test
    if(length(name)!=N) stop("Invalid table names length, table names length should be equal to 1 (to be recycled) or it should match to connection names length.") # TODO test
    x = db.one.is.timing(conn.name=conn.name, sql=sql[[1]], name=name, action=action, DT=x, .db.conns=.db.conns, .db.dict=.db.dict, .timing=timing, verbose=verbose-1)
    msg <- paste0(msg,"; processed ",action," in ",conn.name)
    if(is.data.table(x) && !missing(key) && !is.null(key) && is.character(key)){
      setkeyv(x,key)
      msg <- paste0(msg,"; key on: ",paste(key,collapse=", "))
    }
  } # execute one, return DT or 'send' results
  else if(N > 1){
    if((length(sql)!=N && (action %in% c("get","send"))) || (length(name)!=N && (action %in% c("write","read")))){
      msg <- paste0(msg,";recycling")
      if(length(sql)!=N && (action %in% c("get","send"))){
        if(length(sql)==1){
          sql = rep(sql,N)
          msg <- paste0(msg," sql statements")
        }
        else stop("Invalid sql statement length, sql statements length should be equal to 1 (to be recycled) or it should match to connection names length or .") # TODO test
      }
      if(length(name)!=N && (action %in% c("write","read"))){
        if(length(name)==1){
          name = rep(name,N)
          msg <- paste0(msg," table name")
        }
        else stop("Invalid table names length, table names length should be equal to 1 (to be recycled) or it should match to connection names length.") # TODO test
      }
    } # recycling
    x = mapply(FUN=db.one.is.timing, conn.name=conn.name, sql=sql, name=name, MoreArgs=list(action=action, DT=x, .db.conns=.db.conns, .db.dict=.db.dict, .timing=timing, verbose=verbose-1), SIMPLIFY=FALSE)
    if(timing && is.null(getOption("dwtools.timing.conn.name"))) timingv(x)
    msg <- paste0(msg,"; mapply db.one completed")
  } # execute batch, recycle args, return list of DTs or 'send' results
  
  #### Return
  if(verbose > 0) cat(msg,"\n",sep="")
  return(eval(visibility)) # for "get" and "read" it returns including *print* `DT[]`, for "write" and "send" invisibly `invisible(DT)`
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
        x <- db(.(source.table.name), .(source.conn.name)), # x <- db() # required for nrow_in
        .(target.table.name),
        .(target.conn.name)
      ),
      nrow_in = nrowDT(x),
      .timing = .timing,
      verbose = verbose - 1
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
