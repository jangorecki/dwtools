.onLoad <- function(libname, pkgname){
  
  # dependency
  
  # due to big int timestamp - not yet used
  #options("scipen"=100)
  #trunc(as.numeric(Sys.time())*1e3)
  
  # dwtools
  
  # to be used in the batch processes if you want to group/join datasets from the corresponding batches. It is integer POSIX of R session start. To get new session value R session needs to be restarted.
  #options("dwtools.session" = as.integer(Sys.time() - proc.time()[['elapsed']])) # R session time
  options("dwtools.session" = as.integer(Sys.time())) # dwtools session time
  # usage
  #DT[, dwtools_session := getOption("dwtools.session")]
  
  options("dwtools.verbose" = 0)
  
  options("dwtools.timing" = FALSE)
  #options("dwtools.timing.conn.name" = "sqlite1")
  #options("dwtools.timing.name "= "mylogtable")
  
  # db
  
  options("dwtools.db.preprocess"=FALSE) # db
  options("dwtools.db.postprocess"=FALSE) # db
  
  options("dwtools.db.auto.table.name.ncol"=4) # auto.table.name
  options("dwtools.db.auto.table.name.nchar"=4) # auto.table.name
  
  options("dwtools.db.dict" = db_dict())
  #options("dwtools.db.conns" = list(sqlite1=sqlite1, sqlite2=sqlite2, postgres1=postgres1, mssql1=mssql1, csv1=csv1))
}

.onAttach <- function(libname, pkgname){
  packageStartupMessage("dwtools (pre 1.0.0) not yet functional")
}
