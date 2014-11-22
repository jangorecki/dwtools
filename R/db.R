
# db dict -----------------------------------------------------------------

#' @title db_dict
#' @description Dictionary about save, load, postprocess, etc. for each db driver.
#' @keywords internal
db_dict <- function(){
  db.dict = data.table(
    drvName = c("SQLite","PostgreSQL","RODBC","csv"),
    query = list(
      function(conn, statement) RSQLite::dbGetQuery(conn=conn, statement=statement),
      function(conn, statement) RPostgreSQL::dbGetQuery(conn=conn, statement=statement),
      function(conn, statement) RODBC::sqlQuery(channel=conn, query=statement),
      function(conn, statement) stop("not possible to `query` on csv, use `name` instead of `x` to invoke `read` instead of `query`", call.=FALSE)
    ),
    send = list(
      function(conn, statement) RSQLite::dbSendQuery(conn=conn, statement=statement),
      function(conn, statement) RPostgreSQL::dbSendQuery(conn=conn, statement=statement),
      function(conn, statement) RODBC::sqlQuery(channel=conn, query=statement),
      function(conn, statement) stop("not possible to `send` to csv", call.=FALSE)
    ),
    read = list(
      function(conn, name) RSQLite::dbReadTable(conn=conn, name=name),
      function(conn, name) RPostgreSQL::dbReadTable(conn=conn, name=name),
      function(conn, name) RODBC::sqlQuery(channel=conn, query=paste0("SELECT * FROM ",name)),
      function(conn, name) data.table::fread(input=name)
    ),
    write = list(
      function(conn, name, value) RSQLite::dbWriteTable(conn=conn, name=name, value=value, row.names=FALSE),
      function(conn, name, value) RPostgreSQL::dbWriteTable(conn=conn, name=name, value=value, row.names=FALSE),
      function(conn, name, value) RODBC::sqlSave(channel=conn, dat=value, tablename=name, rownames=FALSE),
      function(conn, name, value) write.csv(x=value, file=name, row.names=FALSE)
    ),
    exists = list(
      DBI::dbExistsTable, 
      DBI::dbExistsTable, 
      RODBC::sqlQuery,
      function(conn, name) file.exists(name)
    ),
    tablename = list(
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

#' @title as.POSIXct
#' @description Setting default for UTC and 1970.
#' @keywords internal
as.POSIXct <- function(x,tz="UTC",origin="1970-01-01"){
  base::as.POSIXct(x,tz=tz,origin=origin)
}

#' @title int.is.POSIXct
#' @description Check if is integer and can be POSIX between 1970 and 2100.
#' @keywords internal
int.is.POSIXct <- function(x, date_from = as.POSIXct("1970-01-01"), date_to = as.POSIXct("2100-01-01")){
  is.integer(x) && all(as.POSIXct(x) %between% c(date_from,date_to))
}

#' @title nrowDT
#' @description Return nrow if DT else NA.
#' @keywords internal
nrowDT <- function(x){
  if(any(c("data.frame","data.table") %in% class(x))) nrow(x) else NA_integer_
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

#' @title Simple database interface.
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
#' Table names, sql commands, connection names can be character vectors. It allows processing into multiple connections and tables at once.
#' @section DB interface unifications:
#' You can provide table name as \code{"my_schema1.my_tab1"} and it will be processed according to target db (e.g. for postgres: \code{c("my_schema1","my_tabl1")}).
#' SQL statements are not unified.
#' @section Limitation:
#' Table names must not contains spaces (which are accepted by some db vendors).
#' SQL send statements should contains spaces. E.g. sqlite \code{.tables} command will need to be written as \code{db("SELECT * FROM sqlite_master WHERE type='table'")}.
#' @section Auto table name:
#' If writing to db and table name is missing or NULL then the \link{auto.table.name} will be used. The table name used in \emph{dbWriteTable} will be provided as \code{"tablename"} attribute of the function result so the \emph{auto.table.name} can be catched for later use.
#' @export
#' @examples
#' \dontrun{
#' NULL
#' }
db <- function(x, ..., key,
               .db.preprocess = getOption("dwtools.db.preprocess"),
               .db.postprocess = getOption("dwtools.db.postprocess"),
               .db.conns = getOption("dwtools.db.conns"),
               .db.dict = getOption("dwtools.db.dict"),
               timing = getOption("dwtools.timing"),
               verbose = getOption("dwtools.verbose")){
  
  #### Validate inputs, catch name and conn.name, set defaults
  # TODO: test warnings
  if(missing(x)){
    stop("x argument must be provided to db function")
  } # stop on missing 'x'
  if(is.null(.db.conns) || length(.db.conns)==0){
    # TO DO TEST
    warning("You must define 'dwtools.db.conns' option to route db requests, `options('dwtools.db.conns'=list(source1=source1,source2=source2))`")
    .db.conns = list(csv1 = list(drvName = "csv", conn = "dwtools_temp.csv"))
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
  } # write
  else if(is.character(x) && is.table.name(x)){
    action = "read"
    name = x
    conn.name = list.sub(dots,1,names(.db.conns[1]))
    sql = list(NULL)
    visibility = expression(x[]) # closing function, get returns visibily
  } # read
  else if(is.character(x) && is.sql(x) && is.sql.get(x)){
    action = "query"
    name = list(NULL)
    conn.name = list.sub(dots,1,names(.db.conns[1]))
    sql = x
    visibility = expression(x[]) # closing function, query returns visibily
  } # query
  else if(is.character(x) && is.sql(x) && is.sql.send(x)){
    action = "send"
    name = list(NULL)
    conn.name = list.sub(dots,1,names(.db.conns[1]))
    sql = x
    visibility = expression(invisible(x)) # closing function, send returns visibily
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
    if(getOption("dwtools.db.one.debug",FALSE)) browser()
    # action
    if(action=="write"){ # write
      if(.db.preprocess){
        DT = tryCatch(
          .db.dict[.(.db.conn$drvName), preprocess[[1]]](DT)
          , error = function(e) stop(paste0("Error in the pre processing function for ",conn.name,", try preprocess function manually on the DT you want to save."), call.=FALSE)
        )
      }
      r = .db.dict[.(.db.conn$drvName), write[[1]]](conn = .db.conn$conn, name = name, value = DT)
      if(!r) stop(paste0("Writing to db connection '",conn.name,"' results status FALSE."))
      # r = .db.dict[.(.db.conn$drvName), write[[1]](conn = .db.conn$conn, name = schemaname[[1]](name), value = DT)]
      # TO DO: name convert to expected format "asd.asd" or c("asd","asd")
      setattr(DT,"tablename",name)
    } # write
    else if(action=="read"){ # read
      DT = .db.dict[.(.db.conn$drvName), read[[1]]](conn = .db.conn$conn, name = name) # TODO name translation to c("schema","tbl")
      setDT(DT)
      if(.db.postprocess){
        DT = tryCatch(
          .db.dict[.(.db.conn$drvName), postprocess[[1]]](DT)
          , error = function(e) stop(paste0("Error in the post processing function for ",conn.name,", use .db.postprocess = FALSE and try postprocess function manually."), call.=FALSE)
        )
      }
    } # read
    else if(action=="query"){ # query
      DT = .db.dict[.(.db.conn$drvName), query[[1]]](conn = .db.conn$conn, statement = sql)
      setDT(DT)
      if(.db.postprocess){
        DT = tryCatch(
          .db.dict[.(.db.conn$drvName), postprocess[[1]]](DT)
          , error = function(e) stop(paste0("Error in the post processing function for ",conn.name,", use .db.postprocess = FALSE and try postprocess function manually."), call.=FALSE)
        )
      }
    } # query
    else if(action=="send"){ # send
      tryCatch(expr = {
        DT = .db.dict[.(.db.conn$drvName), send[[1]]](conn = .db.conn$conn, statement = sql) # here DT is not a DT but send query results
      }, error = function(e) eval.parent(browser()))
      } # send
    if(verbose > 0) cat(as.character(Sys.time()),": db.one: processed ",action," in ",conn.name,"\n",sep="")
    return(DT)
  }
  
  #### Recycle inputs and execute
  # write:
  # 1 DT save to 1 table in 1 conn
  # 1 DT save to 1 table in X conns
  # 1 DT save to X tables in X conns # NOT WORKING TODO
  # read:
  # 1 table in 1 conn
  # 1 table in X conns
  # X table in X conns
  # get/send:
  # 1 sql in 1 conn
  # 1 sql in X conns
  # X sqls in X conns
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
    if((length(sql)!=N && (action %in% c("query","send"))) || (length(name)!=N && (action %in% c("write","read")))){
      msg <- paste0(msg,";recycling")
      if(length(sql)!=N && (action %in% c("query","send"))){
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
  return(eval(visibility))
}

# other db related --------------------------------------------------------

#' @title Copy tables between databases
#' @export
dbCopy <- function(source.name, source.conn.name, target.name, target.conn.name){
  stopifnot(length(source.conn.name)==1 && length(target.conn.name)==1)
  stopifnot(length(source.name)==length(target.name))
  # do one copy
  dbCopy.one <- function(source.name, target.name, source.conn.name, target.conn.name){
    # TODO pre and post processing
    db(x = db(name=source.name, conn.name=source.conn.name),
       name=target.name,
       conn.name=target.conn.name)
  }
  # batch copy
  mapply(dbCopy.one, source.name, target.name, MoreArgs = list(source.conn.name=source.conn.name, target.conn.name=target.conn.name), SIMPLIFY = FALSE)
}
