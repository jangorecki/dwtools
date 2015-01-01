.onLoad <- function(libname, pkgname){
  
  # dwtools
  
  options("dwtools.session"=as.integer(Sys.time())) # dwtools session time
  options("dwtools.verbose"=0L)
  
  # timing
  
  options("dwtools.timing"=FALSE)
  options("dwtools.timing.verbose"=0L)
  options("dwtools.timing.conn.name"=NULL)
  options("dwtools.timing.name"="dwtools_timing")
  options("dwtools.tag.sep"=";")
  
  # db
  
  options("dwtools.db.preprocess"=FALSE)
  options("dwtools.db.postprocess"=FALSE)
  options("dwtools.db.auto.table.name.ncol"=4L)
  options("dwtools.db.auto.table.name.nchar"=4L)
  options("dwtools.db.dict"=db_dict())
  
  # vwap
  
  options("dwtools.time.dict"=time_dict())
  
}

.onAttach <- function(libname, pkgname){
  packageStartupMessage("dwtools 0.8.1 dev version")
}
