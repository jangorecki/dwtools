.onLoad <- function(libname, pkgname){
  
  # dwtools
  
  options("dwtools.session"=as.integer(Sys.time())) # dwtools session time
  options("dwtools.verbose"=0)
  
  # timing
  
  options("dwtools.timing"=FALSE)
  options("dwtools.timing.conn.name"=NULL)
  options("dwtools.timing.name"="dwtools_timing")
  
  # db
  
  options("dwtools.db.preprocess"=FALSE)
  options("dwtools.db.postprocess"=FALSE)
  options("dwtools.db.auto.table.name.ncol"=4)
  options("dwtools.db.auto.table.name.nchar"=4)
  options("dwtools.db.dict"=db_dict())
  
}

.onAttach <- function(libname, pkgname){
  packageStartupMessage("dwtools (pre 1.0.0)")
}
