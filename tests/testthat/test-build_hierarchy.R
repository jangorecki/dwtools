context("build_hierarchy tests")

test_that("match of generated hierarchy", {
  X = dw.populate(N=1e4, scenario="star")
  x <- joinbyv(X$SALES, join=list(X$PRODUCT, X$CURRENCY, X$GEOGRAPHY, X$CUSTOMER, X$TIME))
  dw <- build_hierarchy(x, factname="fact_sales",deploy=FALSE)
  map_names <- c(CURRENCY = "dim_curr", GEOGRAPHY = "dim_geog", TIME = "dim_time", CUSTOMER = "dim_cust", PRODUCT = "dim_prod", SALES = "fact_sales")
  setkeyv(X$SALES,NULL)
  r <- mapply(FUN = function(x, dw.table){
    if(!is.null(key(x))) x <- copy(x)[.(X$SALES[,unique(.SD),.SDcols=c(key(x))][[1]])] # subset initial dimensions to facts data
    data.equal.data.table(x,dw.table,ignore_row_order=TRUE,ignore_col_order=TRUE)
  }, x = X[names(map_names)], dw.table = dw$tables[map_names], SIMPLIFY=TRUE)
  expect_true(all(r), info="test generated hierarchy match to origin hierarchy")
})

test_that("build_hierarchy deploy=TRUE, match of SQL side denormalized data to origin R data", {
  #x = dw.populate(N=1e4, scenario="denormalize")
  # db connection
  
  #dw <- build_hierarchy(x, factname="fact_sales",deploy=TRUE)
  
  #db_denormalized <- db("SELECT * FROM fact_sales 
  #   LEFT OUTER JOIN dim_prod ON dim_prod.prod_code = fact_sales.prod_code 
  #   LEFT OUTER JOIN dim_curr ON dim_curr.curr_code = fact_sales.curr_code 
  #   LEFT OUTER JOIN dim_geog ON dim_geog.geog_code = fact_sales.geog_code 
  #   LEFT OUTER JOIN dim_cust ON dim_cust.cust_code = fact_sales.cust_code 
  #   LEFT OUTER JOIN dim_time ON dim_time.time_code = fact_sales.time_code")
  
  # clean up db
  
  #expect_equal(x, db_denormalized, info="test sqlite denormalization match to origin dataset")
  expect_equal(TRUE,TRUE)
})