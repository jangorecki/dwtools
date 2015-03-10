
# EAV -------------------------------------------------------------

#' @title Entity-Attribute-Value data evaluate
#' @description Evaluate expression on data stored in EAV model as you would using regular wide table.
#' @param x data.table data in EAV model.
#' @param j quoted expression to evaluate, the same way as using on wide format data.table.
#' @param id.vars character vector of columns name which defines an \emph{Entity}.
#' @param variable.name character column name which defines \emph{Attribute}.
#' @param measure.vars character column name which defines \emph{Value}.
#' @param fun.aggregate function which will be applied on duplicates in Entity and Attribute and used when \emph{dcasting}.
#' @param shift.on character column name of field which should excluded from the grouping variables to use \code{shift} over that field. Should be only used when \emph{j} expression is going to use \code{shift}. See examples.
#' @param wide logical default \emph{FALSE} will return EAV data after evaluation of \emph{j} expression, when \emph{TRUE} it will return wide format table.
#' @details The easiest way to use is to setkey on your entity and attribute columns, then only \emph{x} and \emph{j} needs to be passed to function. See examples.
#' @export
#' @example tests/example-eav.R
eav <- function(x, j, id.vars = key(x)[-length(key(x))], variable.name = key(x)[length(key(x))], measure.vars = names(x)[!(names(x) %in% key(x))], fun.aggregate = sum, shift.on = character(), wide=FALSE){
  stopifnot(is.data.table(x))
  r <- x[,lapply(.SD,fun.aggregate),c(id.vars,variable.name),.SDcols=measure.vars
    ][,dcast(.SD,formula=as.formula(paste(paste(id.vars,collapse=' + '),paste(variable.name,collapse=' + '),sep=' ~ ')),fun.aggregate=fun.aggregate,value.var=measure.vars)
      ][,eval(j), by = eval(id.vars[!(id.vars %in% shift.on)])
        ]
  if(wide) r[] else melt(r,id.vars=id.vars, variable.name=variable.name, value.name=measure.vars)[,.SD,keyby=c(id.vars,variable.name)]
}