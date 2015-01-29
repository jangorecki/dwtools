# SQL translation
count <- length
`count distinct` <- uniqueN
`count NA` <- function(x) sum(is.na(x))
`pct NA` <- function(x) sum(is.na(x))/length(x)
avg <- mean

shinyServer(function(input, output, session){
  
  output$dt <- renderDataTable(DT(), options = list(pageLength = 5, lengthMenu = c(10,25,50,100)))
  
  DTquery <- reactive({
    by_input <- input$by_input
    aggr_fun_input <- input$aggr_fun_input
    measure_input <- input$measure_input
    filter_input <- input$filter_input
    isolate({
      if(length(measure_input)==0) return(invisible(NULL))
      if(is.null(filter_input)) filter_input <- ""
      if(nchar(filter_input)==0L) filter_input <- quote(TRUE) else filter_input <- parse(text = filter_input)
      eval(bquote(x[.(filter_input),lapply(.SD,.(aggr_fun_input)),by=.(by_input),.SDcols=.(measure_input)]))
    })
  })
  DT <- reactive({
    by_input <- input$by_input
    measure_input <- input$measure_input
    filter_input <- input$filter_input
    isolate({
      allcols <- c(by_input,measure_input)
      if(length(allcols)==0) return(data.table())
      if(is.null(filter_input)) filter_input <- ""
      if(nchar(filter_input)==0L) filter_input <- quote(TRUE) else filter_input <- parse(text = filter_input)
      eval(bquote(x[.(filter_input),.SD,.SDcols=.(allcols)]))
    })
  })

  output$tree <- renderTree({
    N <- length(dw$tables)
    tree <- list()
    for(tbl in seq_len(N)){
      n <- ncol(dw$tables[[tbl]])
      tblkey <- key(dw$tables[[tbl]])
      for(col in seq_len(n)){
        colname <- names(dw$tables[[tbl]][,col,with=FALSE])
        leaf <- copy(0L)
        colclass <- class(dw$tables[[tbl]][[col]])
        if("numeric" %in% colclass) setattr(leaf,"sticon","calculator")
        if(colname %in% tblkey) setattr(leaf,"sticon","key")
        tree[[labelnames(dw$tables[tbl])]][labelnames(dw$tables[[tbl]][,col,with=FALSE])] <- list(leaf)
      }
    }
    setattr(tree[[N]],"stopened",TRUE) # default open fact table
    tree
  })
  output$tree_str <- renderPrint({
    str(input$tree)
  })
  sel_ismeasure <- reactive({
    tree <- input$tree
    isolate({
      if(is.null(tree)) return(invisible(NULL))
      sel <- get_selected(tree, format = "slices")
      if(!length(sel)) return(invisible(NULL))
      colsel <- sapply(sel,function(x) sapply(x, names))
      numcols[unlabel(colsel)]
    })
  })
  observe({
    sel <- sel_ismeasure()
    isolate({
      if(is.null(sel)) return(invisible(NULL))
      updateSelectInput(session, "by_input", selected = names(sel[!sel]))
    })
  }) # update by_input
  observe({
    sel <- sel_ismeasure()
    isolate({
      if(is.null(sel)) return(invisible(NULL))
      updateSelectInput(session, "measure_input", selected = names(sel[sel]))
    })
  }) # update measure_input
  
  output$aggr_dt <- renderDataTable(DTquery(), options = list(pageLength = 5, lengthMenu = c(10,25,50,100)))
})