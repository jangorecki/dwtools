suppressPackageStartupMessages(library(dwtools))
library(shinyTree)

if(!exists("x")) x <- dw.populate(N=1e5, scenario="denormalize")
if(!exists("dw")) dw <- build_hierarchy(x,factname="fact_sales")

stopifnot(is.data.table(x), is.list(dw))
numcols <- sapply(x,class)=="numeric"

label <- function(x) gsub("_"," ",x)
unlabel <- function(x) gsub(" ","_",x)
labelnames <- function(x) label(names(x))

dims <- names(dw$tables)[-length(dw$tables)]
fact <- names(dw$tables)[length(dw$tables)]

textInputRow <- function (inputId, label, value = ""){
  div(style="display:inline-block", tags$label(label, `for` = inputId), tags$input(id = inputId, type = "text", value = value,class="input-small"))
}

## dev
# [ ] 
# [ ] query on shinyBI on normalized and denormalized data, control type by checkbox, query interface in shinyTree the same for both
# [ ] display timing of processing
# [ ] fix count and count distint to work on dimension attributes instead of measures
# [ ] add download to csv/excel/ods
# [ ] refresh tree selection on inputs change in Aggregates tab
