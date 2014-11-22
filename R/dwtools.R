#' @title dwtools
#' @description Extension for \link{data.table} package for Data Warehouse related functionalities.
#' @references \url{https://www.github.com/jangorecki/dwtools}
#' @docType package
#' @name dwtools
#' @aliases dwtools-package
#' @import data.table
NULL

# dw.populate -------------------------------------------------------------

#' @title Populate data
#' @description sample DW data
#' @param n integer facts volume
#' @param seed integer used in \code{set.seed}
#' @param scenario character in \code{c("fact","star schema","star schema denormalized")}
#' @param verbose integer print sub statuses
#' @export
dw.populate <- function(n = 1e3, seed = 1, scenario = c("fact","star schema","star schema denormalized"), verbose = getOption("dwtools.verbose")){
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
  DT = switch(scenario,
              "fact" = sales,
              "star schema" = list(sales=sales,product=product,customer=customer,geography=geography,time=time),
              "denormalized star schema" = joinbyv(master=sales, join=list(product=product,customer=customer,geography=geography,time=time), by=list("prod_code","cust_code","state_code","date_code")))
  if(verbose > 0) cat(as.character(Sys.time()),": dw.populate: processed scenario '",scenario,"'\n",sep="")
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
