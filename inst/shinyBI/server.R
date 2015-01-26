limitlen <- function(x, len=getOption("dwtools.limitlen",25L)){
  if(length(x) > len) x[1:len]
  else x
}

count <- length
`count distinct` <- uniqueN
avg <- mean

shinyServer(function(input, output, session){
  
  output$dt <- renderDataTable(query_dt(), options = list(pageLength = 5, lengthMenu = c(5,10,15,100))) # full raw dataset for manual manipulation in shiny
  
  query_dt <- reactive({
    by_input <- input$by_input
    aggr_fun_input <- input$aggr_fun_input
    numcols_input <- input$numcols_input
    isolate({
      eval(bquote(x[,lapply(.SD,.(aggr_fun_input)),by=.(by_input),.SDcols=.(numcols_input)]))
    })
  })
  
  query_star <- reactive({
    NULL
  })

  output$tree <- renderTree({
    lapply(dw$tables, function(x) setNames(as.list(rep("",length(x))),names(x)))
  })
  output$tree_str <- renderPrint({
    str(input$tree)
  })
  observe({
    tree <- input$tree
    isolate({
      if(is.null(tree)) return(invisible(NULL))
      sel <- get_selected(tree, format = "slices")
      if(!length(sel)) return(invisible(NULL))
      updateSelectInput(session, "by_input", selected = sapply(sel, function(sel) lapply(sel, names)))
    })
  })
  output$aggr_dt <- renderDataTable(eval(query_dt()), options = list(pageLength = 5, lengthMenu = c(5,10,15,50,100)))
})