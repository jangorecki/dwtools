
# db dict -----------------------------------------------------------------

#' @title db_dict
#' @description Dictionary for databases drivers write, read, get, send, schematable, preprocess, postprocess. Extend here for new db/interface (PR welcome). Live extend by option \code{options("dwtools.db.dict"=db_dict())}.
#' @keywords internal
db_dict <- function(){
  db.dict = data.table(
    drvName = c("SQLite","PostgreSQL","MySQL","Oracle","JDBC","ODBC","csv"),
    package = c("RSQLite","RPostgreSQL","RMySQL","ROracle","RJDBC","RODBC","data.table"),
    write = list(
      function(conn, name, value) dbWriteTable(conn=conn, name=name, value=value, row.names=FALSE, append = TRUE),
      function(conn, name, value) dbWriteTable(conn=conn, name=name, value=value, row.names=FALSE, append = TRUE),
      function(conn, name, value) dbWriteTable(conn=conn, name=name, value=value, row.names=FALSE, append = TRUE),
      function(conn, name, value) dbWriteTable(conn=conn, name=name, value=value, row.names=FALSE, append = TRUE),
      function(conn, name, value) dbWriteTable(conn=conn, name=name, value=value, row.names=FALSE, append = TRUE, overwrite = FALSE),
      function(conn, name, value) as.logical(sqlSave(channel=conn, dat=value, tablename=name, rownames=FALSE, append = TRUE)),
      function(conn, name, value) if(is.null(write.table(x=value, file=name, row.names=FALSE, sep=",", dec=".", append=file.exists(name), col.names=!file.exists(name), qmethod="double"))) TRUE else FALSE
    ),
    read = list(
      function(conn, name) dbReadTable(conn=conn, name=name),
      function(conn, name) dbReadTable(conn=conn, name=name),
      function(conn, name) dbReadTable(conn=conn, name=name),
      function(conn, name) dbReadTable(conn=conn, name=name),
      function(conn, name) dbReadTable(conn=conn, name=name),
      function(conn, name) sqlQuery(channel=conn, query=paste0("SELECT * FROM ",name)),
      function(conn, name) fread(input=name)
    ),
    get = list(
      function(conn, statement) dbGetQuery(conn=conn, statement=statement),
      function(conn, statement) dbGetQuery(conn=conn, statement=statement),
      function(conn, statement) dbGetQuery(conn=conn, statement=statement),
      function(conn, statement) dbGetQuery(conn=conn, statement=statement),
      function(conn, statement) dbGetQuery(conn=conn, statement=statement),
      function(conn, statement) sqlQuery(channel=conn, query=statement),
      function(conn, statement) stop("not possible to `get` on csv, provide filename to `x` to invoke `read` instead of `get`", call.=FALSE)
    ),
    send = list(
      function(conn, statement) dbSendQuery(conn=conn, statement=statement),
      function(conn, statement) dbSendQuery(conn=conn, statement=statement),
      function(conn, statement) dbSendQuery(conn=conn, statement=statement),
      function(conn, statement) dbSendQuery(conn=conn, statement=statement),
      function(conn, statement) dbSendUpdate(conn=conn, statement=statement),
      function(conn, statement) sqlQuery(channel=conn, query=statement),
      function(conn, statement) stop("not possible to `send` to csv", call.=FALSE)
    ),
    tablename = list(
      function(x){ r = strsplit(x,".",TRUE)[[1]]; if(length(r) > 2) stop("Table name should contain at most one dot for schema.table mapping, in case of SQLite schema will be removed") else r[length(r)] },
      function(x){ r = strsplit(x,".",TRUE)[[1]]; if(length(r) > 2) stop("Table name should contain at most one dot for schema.table mapping") else r },
      function(x) x,
      function(x) x,
      function(x) x,
      function(x) x,
      function(x) paste(x,"csv",sep=".")
    ),
    preprocess = list(
      function(DT) DT,
      function(DT) DT,
      function(DT) DT,
      function(DT) DT,
      function(DT) DT,
      function(DT) DT,
      function(DT) DT
    ),
    postprocess = list(
      function(DT) DT,
      function(DT) DT,
      function(DT) DT,
      function(DT) DT,
      function(DT) DT,
      function(DT) DT,
      function(DT) DT
    ),
    key = "drvName"
  )
  return(db.dict)
}
