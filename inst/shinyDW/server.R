limitlen <- function(x, len=25L){
  if(length(x) > len) x[1:len]
  else x
}

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

  output$aggr_tree <- renderTree({
    l <- split(r[,.SD,.SDcols=c(names(r)[!(names(r) %in% c("byi","bycols"))])], seq.int(nrow(r)))
    setattr(l,"names",r[,byichar])
    lapply(limitlen(l),function(x) as.list(x))
  })
  output$tree_str <- renderPrint({
    str(input$aggr_tree)
  })
  observe({
    tree <- input$aggr_tree
    isolate({
      if(length(byichar_input <- unlist(get_selected(tree)))){
        updateSelectInput(session, "by_input", selected = r[byichar==byichar_input,bycols[[1]]])
      }
    })

  })
  output$aggr_idx_dt <- renderDataTable(r[,.SD,.SDcols=c(names(r)[!(names(r) %in% c("byi","bycols"))])], options = list(pageLength = 5, lengthMenu = c(5,10,15,100)))
  output$aggr_dt <- renderDataTable(eval(query_dt()), options = list(pageLength = 5, lengthMenu = c(5,10,15,100)))
})