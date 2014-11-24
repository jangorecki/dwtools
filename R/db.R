
# db dict -----------------------------------------------------------------

#' @title db_dict
#' @description Dictionary about save, load, postprocess, etc. for each db driver.
#' @param drvName character vector of 
#' @keywords internal
db_dict <- function(drvName){
  db_list = list()
  callIfNamespace <- function(package, expr, silent=FALSE, silent.expr=NULL){
    if(requireNamespace("RSQLite", quietly=TRUE)){
      return(eval.parent(expr))
    }
    else if(silent){
      return(eval.parent(silent.expr))
    }
    else if(!silent){
      stop(paste0("Unable to load ",package," namespace. You might have not installed required package. Try: install.packages('",package,"')"))      
    }
  } # not used atm
  db.dict = data.table(
    drvName = c("SQLite","PostgreSQL","ODBC","csv"),
    package = c("RSQLite","RPostgreSQL","RODC","data.table"),
    write = list(
      function(conn, name, value) dbWriteTable(conn=conn, name=name, value=value, row.names=FALSE),
      function(conn, name, value) dbWriteTable(conn=conn, name=name, value=value, row.names=FALSE),
      function(conn, name, value) sqlSave(channel=conn, dat=value, tablename=name, rownames=FALSE),
      function(conn, name, value) write.csv(x=value, file=name, row.names=FALSE)
    ),
    read = list(
      function(conn, name) dbReadTable(conn=conn, name=name),
      function(conn, name) dbReadTable(conn=conn, name=name),
      function(conn, name) sqlQuery(channel=conn, query=paste0("SELECT * FROM ",name)),
      function(conn, name) fread(input=name)
    ),
    get = list(
      function(conn, statement) dbGetQuery(conn=conn, statement=statement),
      function(conn, statement) dbGetQuery(conn=conn, statement=statement),
      function(conn, statement) sqlQuery(channel=conn, query=statement),
      function(conn, statement) stop("not possible to `get` on csv, provide filename to `x` to invoke `read` instead of `get`", call.=FALSE)
    ),
    send = list(
      function(conn, statement) dbSendQuery(conn=conn, statement=statement),
      function(conn, statement) dbSendQuery(conn=conn, statement=statement),
      function(conn, statement) sqlQuery(channel=conn, query=statement),
      function(conn, statement) stop("not possible to `send` to csv", call.=FALSE)
    ),
    tablename = list( # TODO, split "schema.table" to c("schema","table") for postgres and possibily others
      function(x) strsplit(x,".",TRUE)[[1]],
      function(x) x,
      function(x) x,
      function(x) x
    ),
    preprocess = list(
      function(DT) DT[,lapply(l, function(x) if(is.POSIXt(x)) as.integer(x) else if(is.function(x)) x else x)],
      function(DT) DT[,lapply(l, function(x) if(is.function(x)) x else x)],
      function(DT) DT[,lapply(l, function(x) if(is.function(x)) x else x)],
      function(DT) DT
    ),
    postprocess = list(
      function(DT) DT[,lapply(l, function(x) if(int.is.POSIXct(x)) as.POSIXct(x) else if(is.function(x)) x else x)],
      function(DT) DT[,lapply(l, function(x) if(is.function(x)) x else x)],
      function(DT) DT[,lapply(l, function(x) if(is.function(x)) x else x)],
      function(DT) DT
    ),
    key = "drvName"
  )
  return(db.dict)
}

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
#' @description take \code{i} element of the list \code{x} if not present or NULL then return \code{fill}.
#' @keywords internal
list.sub <- function(x, i, fill=NULL){
  if(length(x)<i || is.null(x[[i]])) r = fill
  else r = x[[i]]
  return(r)
}

# db -------------------------------------------------------------------

#' @title Simple database interface
#' @param x data.table (to save in db) or character of table names or character of sql commands.
#' @param \dots if \code{x} is data.table then \dots expects character table names and character connection names else \dots expects only character connection names.
#' @param key character vector to be used to set key, cannot be mixed with multiple connections queries, see examples for chaining in DT syntax.
#' @param timing logical measure timing of queries, read \link{timing}.
#' @param verbose integer, if greater than 0 then print debugging messages.
#' @details Function is designed to be slim and chainable in data.table \code{`[`} operator.
#' \itemize{
#' \item \code{dbWriteTable} - \code{x} is data.table: \code{db(DT,"my_tab")}
#' \item \code{dbReadTable} - \code{x} character table name: \code{db("my_tab")}
#' \item \code{dbGetQuery} - \code{x} character with spaces and starts with \code{"SELECT "}: \code{db("SELECT col1 FROM my_tab1")}
#' \item \code{dbSendQuery} - \code{x} character with spaces and \strong{not} starts with \code{"SELECT "}: \code{db("UPDATE my_tab1 SET col1 = NULL")}
#' }
#' @return In case of \emph{dbWriteTable/dbReadTable/dbGetQuery} the data.table object (possibly with attributes), in case of \emph{dbSendQuery} the send query results,
#' @section Multiple connections:
#' Table names, sql commands, connection names can be character vectors. It allows processing into multiple connections and tables at once. The list of results will be returned, it will be named by the connection names, so if the connecion name was recycled (e.g. \code{db(c("my_tab1","my_tab2"))}) then there will be duplicated names in the resulted list.
#' @section DB interface unifications:
#' TODO: You can provide table name as \code{"my_schema1.my_tab1"} and it will be processed according to target db (e.g. for postgres: \code{c("my_schema1","my_tabl1")}).
#' SQL statements are not unified.
#' @section Limitation:
#' Table names must not contains spaces (which are accepted by some db vendors).
#' SQL send statements should contains spaces. E.g. sqlite \code{.tables} command will need to be written as \code{db("SELECT * FROM sqlite_master WHERE type='table'")}.
#' @section Auto table name:
#' If writing to db and table name is missing or NULL then the \link{auto.table.name} will be used. The table name used in \emph{dbWriteTable} will be provided as \code{"tablename"} attribute of the function result so it can be catched for later use.
#' @section DB migration:
#' There preprocessing and postprocessing functions available so the classes of the data returned by external databases can be also integrated. This gives R ability to act as data hub and gain value as ETL tool.
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
    if(!getOption("dwtools.db.silent.drvName.csv",FALSE)) warning("You must define 'dwtools.db.conns' option to route db requests, `options('dwtools.db.conns'=list(source1=source1,source2=source2))`. ?db")
    .db.conns = list(csv1 = list(drvName = "csv", conn = NULL))
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
    name = list.sub(dots,1,auto.table.name(names(x)))
    conn.name = list.sub(dots,2,names(.db.conns[1]))
    sql = list(NULL)
    visibility = expression(invisible(x[])) # closing function, write returns invisibly
    if(length(conn.name)==1 && length(name)>1) conn.name = rep(conn.name,length(name))
  } # write
  else if(is.character(x) && is.table.name(x)){
    action = "read"
    name = x
    conn.name = list.sub(dots,1,names(.db.conns[1]))
    sql = list(NULL)
    visibility = expression(x[]) # closing function, get returns visibily
    if(length(conn.name)==1 && length(name)>1) conn.name = rep(conn.name,length(name))
  } # read
  else if(is.character(x) && is.sql(x) && is.sql.get(x)){
    action = "get"
    name = list(NULL)
    conn.name = list.sub(dots,1,names(.db.conns[1]))
    sql = x
    visibility = expression(x[]) # closing function, query returns visibily
    if(length(conn.name)==1 && length(sql)>1) conn.name = rep(conn.name,length(sql))
  } # get
  else if(is.character(x) && is.sql(x) && is.sql.send(x)){
    action = "send"
    name = list(NULL)
    conn.name = list.sub(dots,1,names(.db.conns[1]))
    sql = x
    visibility = expression(invisible(x)) # closing function, send returns visibily
    if(length(conn.name)==1 && length(sql)>1) conn.name = rep(conn.name,length(sql))
  } # send
  else{
    stop("Unsupported input, `db` expects `x` as data.table / character sql statement / character table name.")
  } # else error 
  
  #### Define single execution
  # write: 1 DT save to 1 table in 1 conn
  # read: 1 tablename in 1 conn
  # get/send: 1 sql in 1 conn
  db.one <- function(conn.name, sql, name, action, DT, .db.conns, .db.dict, timing, verbose){
    .db.conn = .db.conns[[conn.name]]
    msg <- cat(as.character(Sys.time()),": db.one")
    if(getOption("dwtools.db.one.debug",FALSE)) browser()
    # action
    if(action=="write"){ # write
      if(.db.preprocess){
        DT = tryCatch(
          .db.dict[.(.db.conn$drvName), preprocess[[1]]](DT)
          , error = function(e) stop(paste0("Error in the pre processing function for ",conn.name,", try preprocess function manually on the DT you want to save."), call.=FALSE)
        )
      }
      if(verbose > 0) msg <- paste0(msg,"; write into db table ",paste(name,collapse=".")," in ",conn.name)
      r = .db.dict[.(.db.conn$drvName), write[[1]]](conn = .db.conn$conn, name = name, value = DT)
      if(length(r)) stop(paste0("Writing to db connection '",conn.name,"' results status FALSE."))
      # r = .db.dict[.(.db.conn$drvName), write[[1]](conn = .db.conn$conn, name = schemaname[[1]](name), value = DT)]
      # TO DO: name convert to expected format "asd.asd" or c("asd","asd")
      setattr(DT,"tablename",name)
    } # write
    else if(action=="read"){ # read
      if(verbose > 0) msg <- paste0(msg,"; read from db table ",paste(name,collapse=".")," in ",conn.name)
      DT = .db.dict[.(.db.conn$drvName), read[[1]]](conn = .db.conn$conn, name = name) # TODO name translation to c("schema","tbl")
      setDT(DT)
      if(.db.postprocess){
        DT = tryCatch(
          .db.dict[.(.db.conn$drvName), postprocess[[1]]](DT)
          , error = function(e) stop(paste0("Error in the post processing function for ",conn.name,", use .db.postprocess = FALSE and try postprocess function manually."), call.=FALSE)
        )
      }
    } # read
    else if(action=="get"){ # get
      if(verbose > 0) msg <- paste0(msg,"; get from db statement@",conn.name,": ",sql)
      DT = .db.dict[.(.db.conn$drvName), get[[1]]](conn = .db.conn$conn, statement = sql)
      setDT(DT)
      if(.db.postprocess){
        DT = tryCatch(
          .db.dict[.(.db.conn$drvName), postprocess[[1]]](DT)
          , error = function(e) stop(paste0("Error in the post processing function for ",conn.name,", use .db.postprocess = FALSE and try postprocess function manually."), call.=FALSE)
        )
      }
    } # get
    else if(action=="send"){ # send
      if(verbose > 0) msg <- paste0(msg,"; send to db statement@",conn.name,": ",sql)
      tryCatch(expr = {
        DT = .db.dict[.(.db.conn$drvName), send[[1]]](conn = .db.conn$conn, statement = sql) # here DT is not a DT but send query results
      }, error = function(e) browser())
      } # send
    if(verbose > 0) cat(msg,"; processed ",action," in ",conn.name,"\n",sep="")
    return(DT)
  }
  
  #### Recycle inputs and execute
  # write:
  # 1 DT save to 1 table in 1 conn
  # 1 DT save to 1 table in X conns
  # 1 DT save to X tables in X conns
  # read:
  # 1 table in 1 conn
  # 1 table in X conns
  # X tables in X conns
  # X tables in 1 conn # TODO test
  # get/send:
  # 1 sql in 1 conn
  # 1 sql in X conns
  # X sqls in X conns
  # X tables in 1 conn # TODO test
  
  N = length(conn.name)
  msg <- paste0(as.character(Sys.time()),": db")
  if(N == 1){
    if(length(sql)!=N) stop("Invalid sql statement length, sql statements length should be equal to 1 (to be recycled) or it should match to connection names length or .") # TODO test
    if(length(name)!=N) stop("Invalid table names length, table names length should be equal to 1 (to be recycled) or it should match to connection names length.") # TODO test
    x = db.one(conn.name=conn.name, sql=sql, name=name, action=action, DT=x, .db.conns=.db.conns, .db.dict=.db.dict, timing=timing, verbose=verbose-1)
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
    x = mapply(FUN=db.one, conn.name=conn.name, sql=sql, name=name, MoreArgs=list(action=action, DT=x, .db.conns=.db.conns, .db.dict=.db.dict, timing=timing, verbose=verbose-1), SIMPLIFY=FALSE)
    msg <- paste0(msg,"; mapply db.one completed")
  } # execute batch, recycle args, return list of DTs or 'send' results
  
  #### Return
  if(verbose > 0) cat(msg,"\n",sep="")
  return(eval(visibility)) # for "get" and "read" it returns including *print* `DT[]`, for "write" and "send" invisibly `invisible(DT)`
}

# db migration --------------------------------------------------------

#' @title Copy tables between databases
#' @param source.table.name
#' @export
dbCopy <- function(source.table.name, source.conn.name, target.table.name, target.conn.name, verbose=getOption("dwtools.verbose")){
  stopifnot(length(source.conn.name)==1 && length(target.conn.name)==1)
  stopifnot(length(source.table.name)==length(target.table.name))
  # do one copy
  
  dbCopy.one <- function(source.table.name, source.conn.name, target.table.name, target.conn.name, verbose){
    # TODO pre and post processing
    x = db(
      db(source.table.name, source.conn.name),
      target.table.name,
      target.conn.name
    )
    if(verbose > 0) cat(paste0(as.character(Sys.time()),": dbCopy.one; copy: ",paste(source.table.name,source.conn.name,sep="@")," to ",paste(target.table.name,target.conn.name,sep="@")),"\n",sep="")
    x
  }
  # batch copy
  mx = mapply(dbCopy.one, source.table.name, target.table.name, MoreArgs = list(source.conn.name=source.conn.name, target.conn.name=target.conn.name, verbose=verbose-1), SIMPLIFY = FALSE)
  if(verbose > 0) cat(paste0(as.character(Sys.time()),": dbCopy; copy from ",paste(source.conn.name,target.conn.name,sep=" to ")," completed for ",length(target.table.name)," tables"),"\n",sep="")
  mx
}
