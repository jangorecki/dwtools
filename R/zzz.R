.onLoad <- function(libname, pkgname){
  
  # to be used in the batch processes if you want to group/join datasets from the corresponding batches. It is integer POSIX of R session start. To get new session value R session needs to be restarted.
  options("dwtools.session" = as.integer(Sys.time() - proc.time()[['elapsed']]))
  # usage
  #DT[, dwtools_session := getOption("dwtools.session")]
  
  options("dwtools.preprocess"=FALSE) # db
  options("dwtools.postprocess"=FALSE) # db
  
  options("dwtools.auto.table.name.ncol"=4) # gsub.table.name
  options("dwtools.auto.table.name.nchar"=4) # gsub.table.name
  
  options("dwtools.timing" = FALSE)
  options("dwtools.timing.conn.name" = "sqlite1")
  options("dwtools.timing.name "= "mylogtable")
  
  options("dwtools.verbose" = 0)
  
  options("dwtools.db.dict" = db.dict)
   
  if(connection_setup_example <- FALSE){
    sqlite1 = list(drvName = "SQLite",
                   dbname = "sqlite1.db")
    sqlite1$conn = RSQLite::SQLite()
    sqlite2 = list(drvName = "SQLite",
                   dbname = "sqlite2.db")
    postgres1 = list(drvName = "PostgreSQL", 
                     dbname = "sqlite2.db",
                     user = "",
                     pass = "",
                     conn = expression(PostgreSQL()))
    mssql1 = list(drvName = "RODBC",
                  dbname = "",
                  user = "",
                  pass = "",
                  conn = "")
    csv1 = list(drvName = "csv",
                file = "")
    options("dwtools.conns" = list(sqlite1=sqlite1, sqlite2=sqlite2, postgres1=postgres1, mssql1=mssql1, csv1=csv1))
  }
}

.onAttach <- function(libname, pkgname){
  packageStartupMessage("dwtools not yet functional")
}
