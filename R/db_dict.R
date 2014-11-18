db.dict = data.table(
  drvName = c("SQLite","PostgreSQL","RODBC"), 
  query = list(
    DBI::dbGetQuery, 
    DBI::dbGetQuery, 
    RODBC::sqlQuery
    ),
  insert = list(
    DBI::dbWriteTable,
    DBI::dbWriteTable,
    RODBC::sqlSave
    ),
  exists = list(
    DBI::dbExistsTable, 
    DBI::dbExistsTable, 
    RODBC::sqlQuery
    ),
  preprocess = list(
    function(DT) DT[,lapply(l, function(x) if(is.POSIXt(x)) as.integer(x) else if(is.function(x)) x else x)],
    function(DT) DT[,lapply(l, function(x) if(is.function(x)) x else x)],
    function(DT) DT[,lapply(l, function(x) if(is.function(x)) x else x)]
  ),
  postprocess = list(
    function(DT) DT[,lapply(l, function(x) if(int.is.POSIXct(x)) as.POSIXct(x) else if(is.function(x)) x else x)],
    function(DT) DT[,lapply(l, function(x) if(is.function(x)) x else x)],
    function(DT) DT[,lapply(l, function(x) if(is.function(x)) x else x)]
  ),
  key = "drvName"
)
