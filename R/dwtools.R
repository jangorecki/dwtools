#' @title Data Warehouse tools
#' @description Extension for \link{data.table} package for Data Warehouse related functionalities.
#' @docType package
#' @import data.table digest
#' @name dwtools
NULL

# dw.populate -------------------------------------------------------------

#' @title Populate data
#' @description sample DW data. TODO better example data, k argument for groups size like in DT benchmark
#' @param n integer facts volume
#' @param k groups
#' @param seed integer used in \code{set.seed}
#' @param scenario character in \code{c("fact","star schema","star schema denormalized")}
#' @param verbose integer print sub statuses
#' @export
dw.populate <- function(n = 1e3, k = 10, seed = 1, scenario = c("fact","star schema","star schema denormalized"), verbose = getOption("dwtools.verbose")){
  product <- 
    data.table(prod_code = 1:16,
               prod_name = paste("prod",letters[1:16]),
               prod_group_code = rep(1:4,4), 
               prod_group_name = paste("prod group",rep(letters[1:4],4)),
               prod_family_code = rep(1:2,8), 
               prod_family_name = paste("prod family",rep(letters[1:2],8)))
  customer <- 
    data.table(cust_code = 1:24, 
               cust_name = paste("cust",letters[1:24]),
               cust_mail = paste0("cust",1:24,"@mail.com"),
               cust_hq_code = rep(1:3,8),
               cust_hq_name = paste("cust hq",rep(letters[1:3],8)))
  geography <- 
    data.table(state_code = state.abb,
               state_name = state.name,
               division_code = as.integer(state.division),
               division_name = as.character(state.division),
               region_code = as.integer(state.region),
               region_name = as.character(state.region))
  time <- 
    data.table(date_code = seq(as.Date(paste0(year(Sys.Date())-1,"-01-01")),
                               as.Date(paste0(year(Sys.Date()),"-12-31")),
                               by = "1 day"))
  time[,`:=`(month_code = month(date_code), 
             month_name = months(date_code),
             quarter_code = as.POSIXlt(date_code)$mon %/% 3L + 1L,
             year_code = year(date_code))]
  set.seed(seed)
  sales <- 
    data.table(prod_code = sample(product[,prod_code], n, TRUE),
               cust_code = sample(customer[,cust_code], n, TRUE),
               state_code = sample(geography[,state_code], n, TRUE),
               date_code = sample(time[,date_code], n, TRUE),
               quantity = rnorm(n, 500, 200),
               value = rnorm(n, n, 2000))
  
  TODO_to_use <- function(){
    DT <- data.table(
      id1 = sample(sprintf("id%03d",1:K), N, TRUE),      # large groups (char)
      id2 = sample(sprintf("id%03d",1:K), N, TRUE),      # large groups (char)
      id3 = sample(sprintf("id%010d",1:(N/K)), N, TRUE), # small groups (char)
      id4 = sample(K, N, TRUE),                          # large groups (int)
      id5 = sample(K, N, TRUE),                          # large groups (int)
      id6 = sample(N/K, N, TRUE),                        # small groups (int)
      v1 =  sample(5, N, TRUE),                          # int in range [1,5]
      v2 =  sample(5, N, TRUE),                          # int in range [1,5]
      v3 =  sample(round(runif(100,max=100),4), N, TRUE) # numeric e.g. 23.5749
    )
  } # TODO k arg
  
  DT = switch(scenario,
              "fact" = sales,
              "star schema" = list(sales=sales,product=product,customer=customer,geography=geography,time=time),
              "denormalized star schema" = joinbyv(master=sales, join=list(product=product,customer=customer,geography=geography,time=time), by=list("prod_code","cust_code","state_code","date_code")))
  if(verbose > 0) cat(as.character(Sys.time()),": dw.populate: processed scenario '",scenario,"' volume n: ",n,", groups k:",k,"\n",sep="")
  return(DT)
}

# timing ------------------------------------------------------------------

#' @title Measure timing
#' @description Collect timings and nrows when possible.
#' @details Use option \code{options("dwtools.timing"=TRUE)} to turn on timing measurment in functions which supports timing measurement. To log timing to db connection, setup \code{options("dwtools.db.conns"}, provide connectio name to \code{options("dwtools.timing.conn.name"="sqlite1")} and target table \code{options("dwtools.timing.name"="mylogtable")} otherwise timing will be returned as \code{"timing"} attribute to the expression result.
#' @export
timing <- function(expr, nrow_in = NA_integer_,
                   .timing = getOption("dwtools.timing"),
                   .timing.name = getOption("dwtools.timing.name"),
                   .timing.conn.name = getOption("dwtools.timing.conn.name")){
  if(!.timing) return(expr)
  subx = substitute(expr)
  l = system.time(r <- eval.parent(expr))
  x = setDT(as.list(l))[,list(timestamp = Sys.time(),
                              dwtools_session = getOption("dwtools.session"),
                              expr = subx,
                              expr_crc32 = digest(subx,algo="crc32"),
                              nrow_in = nrow_in,
                              nrow_out = nrowDT(r),
                              user_self = user.self,
                              sys_self = sys.self,
                              elapsed = elapsed,
                              user_child = user.child,
                              sys_child = sys.child)]
  if(!is.null(.timing.name) && !is.null(.timing.conn.name)) db(x, name=.timing.name, conn.name=.timing.conn.name, .timing=FALSE)
  else eval.parent(setattr(r, "timing", x))
  return(x)
}

# technical ---------------------------------------------------------------

#' @title as.POSIXct
#' @description Setting default for UTC and 1970.
#' @keywords internal
as.POSIXct <- function(x,tz="UTC",origin="1970-01-01"){
  base::as.POSIXct(x,tz=tz,origin=origin)
}

#' @title int.is.POSIXct
#' @description Check if is integer and can be POSIX between 1970 and 2100.
#' @keywords internal
int.is.POSIXct <- function(x, date_from = as.POSIXct("1970-01-01"), date_to = as.POSIXct("2100-01-01")){
  is.integer(x) && all(as.POSIXct(x) %between% c(date_from,date_to))
}

#' @title nrowDT
#' @description Return nrow if DT else NA.
#' @keywords internal
nrowDT <- function(x){
  if(any(c("data.frame","data.table") %in% class(x))) nrow(x) else NA_integer_
}