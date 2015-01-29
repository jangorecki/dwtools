shinyUI(
  navbarPage(
    "shinyBI",
    tabPanel("Hierarchy",
             fluidPage(
               sidebarLayout(
                 sidebarPanel(width = 3,
                              shinyTree("tree",checkbox=TRUE,search=TRUE,dragAndDrop=FALSE)),
                 mainPanel(width = 9,
                           dataTableOutput("dt"))
               )
             )),
    tabPanel("Aggregates",
             fluidPage(
               sidebarLayout(
                 sidebarPanel(width = 3,
                              #textInput("filter_input", label="filter", value=""),
                              selectInput("measure_input", label="measures", choices=c(names(numcols)[numcols],names(numcols)[!numcols]), selected=NULL, multiple=TRUE),
                              selectInput("by_input", label="groups", choices=c(names(numcols)[!numcols],names(numcols)[numcols]), selected=NULL, multiple=TRUE),
                              selectInput("aggr_fun_input", label="aggregate function", choices=c("sum","avg","median","min","max","count","count distinct","count NA","pct NA"), selected="sum", multiple=FALSE)),
                 mainPanel(width = 9,
                           dataTableOutput("aggr_dt"))
               )
             ))
  )
)