
# technical ---------------------------------------------------------------

# col names to db table names
gsub.table.name <- function(x, ncol=getOption("dwtools.auto.table.name.ncol"), nchar=getOption("dwtools.auto.table.name.nchar")){
  nm = gsub(",","_",gsub(" ","_",
                         tolower(substring(na.omit(x[1:ncol]), 1, nchar))
  ))
  paste(paste(xgsub(nm),collapse="_"),as.character(Sys.time(),"%Y%m%d%H%M%S"),sep="_")
}

as.POSIXct <- function(x,tz="UTC",origin="1970-01-01") base::as.POSIXct(x,tz=tz,origin=origin)
int.is.POSIXct <- function(x, date_from = as.POSIXct("1970-01-01"), date_to = as.POSIXct("2100-01-01")){
  is.integer(x) && (as.POSIXct(x) %between% c(date_from,date_to))
}

# timing
timing <- function(expr){
  subx = substitute(expr)
  l = system.time(eval.parent(expr))
  setDT(as.list(l))[,list(timestamp = Sys.time(),
                          dwtools_session = getOption("dwtools.session"),
                          expr = subx,
                          expr_crc32 = digest(subx,algo="crc32"),
                          user_self = user.self,
                          sys_self = sys.self,
                          elapsed = elapsed,
                          user_child = user.child,
                          sys_child = sys.child)]
}

# db sub processes --------------------------------------------------------

# read write
read <- function(){
  DT = .db.dict[i = .conns[[conn.name]]$drvName,
                j = query[[1]]
                ](conn = .conns[[conn.name]]$conn, name = name)
  setDT(DT)
  if(.postprocess){
    DT = tryCatch(
      .db.dict[i = .conns[[conn.name]]$drvName, j = postprocess[[1]](DT)]
      , error = function(e) stop(paste0("Error in the post processing function for ",conn.name,", use .postprocess = FALSE and try postprocess function manually."))
    )
  }
  return(DT)
}
write <- function(){
  if(.preprocess){
    DT = tryCatch(
      .db.dict[i = .conns[[conn.name]]$drvName, j = preprocess[[1]](DT)]
      , error = function(e) stop(paste0("Error in the pre processing function for ",conn.name,", try preprocess function manually on the DT you want to save."))
    )
  }
  stopifnot(xdbWriteTable())
  return(DT)
}
# define single function
single_conn <- function(x){
  if(is.character(x)) read()
  else if(is.data.table(x)) write()
  
  #if(.timing) timing(read())
  #if(.timing) timing(write())
}

# db -------------------------------------------------------------------

#' @param x data.table to save or character vector of sql command.
#' @param name under which save/load the data, can be vector corresponding to conn.name to handle save to multiple connections at once.
#' @param conn.name character vector connection names to use, should match to names of defined 'dwtools.conns' option.
#' @param verbose integer, if greater than 0 then print debugging messages.
#' @details x
#' @section Multiple connections:
#' Function is capable to read and write into multiple connections and tables at once, just provide vectors of characters to \code{name} (or sql queries in \code{x}) and \code{conn.name} to query list of tables from corresponding connections.
#' @section Timing:
#' Use option \code{options("dwtools.timing"=TRUE)} to turn on timing measurment, log timing to connection by \code{options("dwtools.timing.conn.name"="sqlite1")} and target table \code{options("dwtools.timing.name"="mylogtable")}.
#' @export
db <- function(x, name, conn.name,
               .preprocess = getOption("dwtools.preprocess"),
               .postprocess = getOption("dwtools.postprocess"),
               .conns = getOption("dwtools.conns"),
               .db.dict = getOption("dwtools.db.dict"),
               .timing = getOption("dwtools.timing"),
               verbose = getOption("dwtools.verbose")){
  if(missing(x) && !missing(name)){
    x = name
  }
  
  # auto.conns
  if(is.null(.conns) || length(.conns)==0) stop("you must define 'dwtools.conns' option to route db requests, if you define one it will be used by default")
  if(length(.conns)==1){
    if(missing(conn.name) || is.null(conn.name)) conn.name = names(.conns[1])
  }
  else if(length(.conns)>1){
    if(length(conn.name)>1){
      # save / load from multiple connections
      
    }
  }
  
  # auto.table.name
  if(missing(name)) name = gsub.table.name(names(x))
  else if(!is.character(name)){
    warning("will use auto.table.name due to incorrect `name` class, should be character")
    name = gsub.table.name(names(x))
  }
  for(i in length(conn.name)){
    single_conn(x)
  }
  
  return(x)
}
