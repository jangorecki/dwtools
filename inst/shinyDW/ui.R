shinyUI(
  navbarPage(
    "shinyDW",
    tabPanel("Hierarchy browser",
             fluidPage(
               sidebarLayout(
                 sidebarPanel(width = 3,
                              shinyTree("tree",checkbox=TRUE,search=TRUE,dragAndDrop=FALSE)),
                 mainPanel(width = 9,
                           dataTableOutput("aggr_dt"))
               )
               #, wellPanel(verbatimTextOutput("tree_str"))
             )),
    tabPanel("Denormalize dataset",
             fluidPage(
               sidebarLayout(
                 sidebarPanel(width = 3,
                              selectInput("numcols_input", label="measures", choices=names(numcols)[numcols], selected=names(numcols)[numcols], multiple=TRUE),
                              selectInput("by_input", label="groups", choices=names(numcols)[!numcols], selected=NULL, multiple=TRUE),
                              selectInput("aggr_fun_input", label="aggregate function", choices=c("sum","mean","median","min","max","length"), selected="sum", multiple=FALSE)),
                 mainPanel(width = 9,
                           dataTableOutput("dt"))
               )
             ))
  )
)