shinyUI(
  navbarPage(
    "shinyDW",
    tabPanel("DW explore",
             fluidPage(
               sidebarLayout(
                 sidebarPanel(width = 3,
                              shinyTree("aggr_tree")),
                 mainPanel(width = 9,
                           #verbatimTextOutput("tree_str"),
                           dataTableOutput("aggr_dt"))
               ),
               wellPanel(dataTableOutput("aggr_idx_dt"))
             )),
    tabPanel("Full dataset",
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