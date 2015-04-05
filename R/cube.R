validNames <- function(x){
  if(length(x) > 0L){
    if(is.null(nm <- names(x))){
      return(FALSE)
    } else {
      return(length(x) == uniqueN(nm))
    }
  } else {
    return(TRUE)
  }
}

#' @title Cube class
#' @docType class
#' @format An R6 class object.
#' @name CUBE
#' @export
#' @example tests/example-cube.R
CUBE <- R6Class(
  classname = "CUBE",
  public = list(
    DB = new.env(),
    factList = list(),
    dimList = list(),
    refList = list(),
    initialize = function(fact=list(), dim=list(), ref=list()){
      stopifnot(validNames(fact), validNames(dim), validNames(ref))
      lapply(names(fact), self$addFact, fact)
      lapply(names(dim), self$addDim, dim)
      lapply(names(ref), self$addRef, ref)
      self
    },
    MDX = function(cols = list(), rows = list(), from, where = list(), FUN = sum, ..., columns = list()){
      if(length(cols)==0L & length(columns)>0L) cols <- columns
      # check relation
      joins <- rbindlist(self$refList)[fact==eval(from) & dim %in% eval(names(c(rows,cols,where)))]
      # all tables
      all_tbls <- ls(self$DB)
      # define measures
      if(!(from %chin% names(cols))){
        value.var <- names(self$factList[[from]][unlist(self$factList[[from]]) %chin% "numeric"])
      } else{
        value.var <- cols[[from]]
        cols[from] <- NULL
      }
      # drop redundant keys and sub-aggregate
      fact <- self$DB[[from]
                      ][,j = lapply(.SD, FUN, ...),
                        by = c(joins$fact_col),
                        .SDcols = value.var]
      # join dims
      tbls <- all_tbls[all_tbls %chin% unique(joins$dim)]
      # row.sebset based on where arg
      row.subset <- setNames(as.list(rep(TRUE,length(tbls))),tbls)
      if(length(where) > 0L) row.subset[tbls] <- where[tbls]
      dt <- joinbyv(master = fact,
              join = lapply(tbls, function(tbl) self$DB[[tbl]]),
              row.subset = row.subset
      )[,j = lapply(.SD, FUN, ...),
        by = c(unlist(c(rows,cols))),
        .SDcols = value.var]
      if(length(cols) > 0L){
        ftxt <- paste(paste(unlist(rows),collapse="+"),"~",paste(unlist(cols),collapse="+"))
        return(dcast(dt, formula = as.formula(ftxt), value.var = value.var))
      }
      return(dt)
    },
    addFact = function(name, fact){
      assign(name, fact[[name]], envir=self$DB)
      self$factList <- c(self$factList, setNames(list(lapply(fact[[name]],class)),name))
      # cat("Object",name,"processed\n")
      invisible(self)
    },
    addDim = function(name, dim){
      assign(name, dim[[name]], envir=self$DB)
      self$dimList <- c(self$dimList, setNames(list(lapply(dim[[name]],class)),name))
      # cat("Object",name,"processed\n")
      invisible(self)
    },
    addRef = function(name, ref){
      refTbls <- strsplit(name,"-",fixed=TRUE)[[1L]]
      refCols <- strsplit(ref[[name]],"-",fixed=TRUE)[[1L]]
      if(length(refCols)==1L) refCols <- rep(refCols,2) # column name match
      # check if tbls, cols exists and if classes match
      
      # build a list of ref metadata
      self$refList <- c(self$refList, list(list(fact=refTbls[1], dim=refTbls[2], fact_col=refCols[1], dim_col=refCols[2])))
      # cat("Object",name,"processed\n")
      invisible(self)
    },
    delFact = function(name, fact) NULL,
    delDim = function(name, dim) NULL,
    delRef = function(name, ref) NULL,
    plot = function() NULL,
    print = function() cat("CUBE object:\n DB:\n  Facts: ",paste(names(self$factList),collapse=", "),"\n  Dimensions: ",paste(names(self$dimList),collapse=", "),"\n References: ",paste(rbindlist(self$refList)[,paste(fact,dim,sep="-")],collapse=", "),"\n",sep="")
  )
)
