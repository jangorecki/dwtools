#' @title Data Warehouse tools
#' @description Extension for \link{data.table} package for Data Warehouse related functionalities.
#' @details The core functions includes:
#' \itemize{
#' \item \link{db} as Extracting and Loading tool in ETL terms.
#' \item \link{joinbyv} a denormalization of star schema and snowflake schema to flat table.
#' \item \link{dw.populate} populate star schema data.
#' \item \link{timing} measure timing and rows in-out.
#' \item \link{CJI} custom indices for in-memory processing.
#' }
#' @note All dot prefixed arguments are designed to be taken from the options, use them only in special cases, they may be removed from functions input args in future.
#' @docType package
#' @import data.table digest
#' @name dwtools
NULL

# dw.populate -------------------------------------------------------------

#' @title Populate DW data
#' @description Populate sample DW data. By default \emph{star schema}, see \code{scenario} arg for others.
#' @param N integer facts volume
#' @param K groups size
#' @param S integer used in \link{set.seed}
#' @param scenario character in \code{c("fact","star schema","star schema denormalized")}
#' @param setkey logical used only for scenario \code{"star schema"}, default TRUE makes dimensions populated with keys already. For non-key benchmarks use FALSE, also remember about \code{getOption("datatable.auto.index"=FALSE)},
#' @param verbose integer print sub statuses
#' @details The following case \code{scenario="star schema normalized"} will invoke lookup for full columns set in all the dimensions. On the real data it is advised to use \code{scenario="star schema"} and later denormalize using \link{joinbyv} function where you can provide subsets of columns on each lookup.
#' @export
#' @example tests/dw_populate_examples.R
dw.populate <- function(N = 1e5, K = 1e2, S = 1, scenario = "star schema", setkey = TRUE, verbose = getOption("dwtools.verbose")){
  set.seed(S)
  # N, K  taken from: https://github.com/Rdatatable/data.table/wiki/Benchmarks-%3A-Grouping#code-to-reproduce-the-timings-above-
  CUSTOMER <- # grp size: K
    data.table(cust_code = sprintf("id%03d",1:K), 
               cust_name = paste0(sample(letters,K,TRUE),sample(letters,K,TRUE),sample(letters,K,TRUE)," ",sample(letters,K,TRUE),sample(letters,K,TRUE),sample(letters,K,TRUE)),
               cust_mail = paste0("cust",1:K,"@mail.com"),
               cust_active = sample(c(TRUE,FALSE),K,TRUE))
  PRODUCT <- # grp size: 1:(N/K)
    data.table(prod_code = 1:(N/K),
               prod_name = paste0("prod ",1:(N/K),sample(letters,N/K,TRUE)),
               prod_group_code = 1:((N/K)/5), 
               prod_group_name = paste0("prod group ",1:((N/K)/5),sample(letters,(N/K)/5,TRUE)),
               prod_family_code = 1:((N/K)/10), 
               prod_family_name = paste0("prod family ",1:((N/K)/10),sample(letters,(N/K)/10,TRUE)))
  GEOGRAPHY <- # grp size fixed to 50
    data.table(geog_code = state.abb,
               geog_name = state.name,
               geog_division_code = as.integer(state.division),
               geog_division_name = as.character(state.division),
               geog_region_code = as.integer(state.region),
               geog_region_name = as.character(state.region))
  TIME <- # grp size fixed to 1826
    data.table(time_code = seq(as.Date("2010-01-01"),
                               as.Date("2014-12-31"),
                               by = "1 day"))
  TIME[,`:=`(time_month_code = month(time_code), 
             time_month_name = months(time_code),
             time_quarter_code = as.POSIXlt(time_code)$mon %/% 3L + 1L,
             time_year_code = year(time_code))]
  # currency taken from: https://github.com/jangorecki/Rbitcoin/blob/master/R/dictionaries.R#L139
  ct.dict = list(
    crypto = c('BTC','LTC','NMC','FTC','NVC','PPC','TRC','XPM','XDG','XRP','XVN'),
    fiat = c('USD','EUR','GBP','KRW','PLN','RUR','JPY','CHF','CAD','AUD','NZD','CNY','INR',
             'TRY','SYP','GEL','AZN','IRR','KZT','NOK','SEK','ISK','MYR','DKK','BGN','HRK',
             'CZK','HUF','LTL','RON','UAH','IDR','IQD','MNT','BRL','ARS','VEF','MXN')
  )
  CURRENCY <- # grp size fixed to 49
    rbindlist(lapply(1:length(ct.dict), function(i) data.table(curr_code = ct.dict[[i]], currency_type = names(ct.dict[i]))))

  SALES <- data.table(
    cust_code = sample(CUSTOMER$cust_code, N, TRUE),
    prod_code = sample(PRODUCT$prod_code, N, TRUE),
    geog_code = sample(GEOGRAPHY$geog_code, N, TRUE),
    time_code = sample(TIME$time_code, N, TRUE),
    curr_code = sample(CURRENCY$curr_code, N, TRUE),
    amount =  round(runif(N,max=1e3),4),
    value =  round(runif(N,max=1e6),8)
  )
  if(scenario %in% c("star schema") && setkey){
    invisible(mapply(FUN = function(join, by) setkeyv(join,by),
                     join = list(CUSTOMER=CUSTOMER,PRODUCT=PRODUCT,GEOGRAPHY=GEOGRAPHY,TIME=TIME,CURRENCY=CURRENCY), 
                     by = list("cust_code","prod_code","geog_code","time_code","curr_code"), SIMPLIFY=FALSE))
  }
  DT = switch(scenario,
              "fact" = SALES,
              "star schema" = list(SALES=SALES,CUSTOMER=CUSTOMER,PRODUCT=PRODUCT,GEOGRAPHY=GEOGRAPHY,TIME=TIME,CURRENCY=CURRENCY),
              "denormalized star schema" = joinbyv(master=SALES, join=list(CUSTOMER=CUSTOMER,PRODUCT=PRODUCT,GEOGRAPHY=GEOGRAPHY,TIME=TIME,CURRENCY=CURRENCY), by=list("cust_code","prod_code","geog_code","time_code","curr_code")))
  if(verbose > 0) cat(as.character(Sys.time()),": dw.populate: processed scenario '",scenario,"' volume N: ",N,", groups K: ",K,"\n",sep="")
  return(DT)
}

# timing ------------------------------------------------------------------

#' @title Measure timing
#' @description Collect timings and nrows when possible.
#' @param expr expression
#' @param nrow_in integer manually provided input object nrow
#' @param .timing logical
#' @param .timing.name character
#' @param .timing.conn.name character
#' @details Use option \code{options("dwtools.timing"=TRUE)} to turn on timing measurment in functions which supports timing measurement. To log timing to db connection, setup \code{options("dwtools.db.conns"}, provide connectio name to \code{options("dwtools.timing.conn.name"="sqlite1")} and target table \code{options("dwtools.timing.name"="mylogtable")} otherwise timing will be returned as \code{"timing"} attribute to the expression result.
#' @export
timing <- function(expr, nrow_in = NA_integer_,
                   .timing = getOption("dwtools.timing"),
                   .timing.name = getOption("dwtools.timing.name"),
                   .timing.conn.name = getOption("dwtools.timing.conn.name")){
  if(!.timing) return(eval.parent(expr))
  subx = substitute(expr)
  l = system.time(r <- eval.parent(expr))
  x = setDT(as.list(l))[,list(timestamp = Sys.time(),
                              dwtools_session = getOption("dwtools.session"),
                              expr = paste(deparse(subx, width.cutoff=500L),collapse="\n"),
                              expr_crc32 = digest(subx,algo="crc32"),
                              nrow_in = nrow_in,
                              nrow_out = nrowDT(r),
                              user_self = user.self,
                              sys_self = sys.self,
                              elapsed = elapsed,
                              user_child = user.child,
                              sys_child = sys.child)]
  if(!is.null(.timing.name) && !is.null(.timing.conn.name)) db(x, .timing.name, .timing.conn.name, timing=FALSE)
  else setattr(r, "timing", x)
  return(r)
}

# technical ---------------------------------------------------------------

#' @title as.POSIXct
#' @description Setting default for UTC and 1970.
#' @keywords internal
as.POSIXct <- function(x,tz="UTC",origin="1970-01-01"){
  base::as.POSIXct(x,tz=tz,origin=origin)
}

#' @title is.int.POSIXct
#' @description Check if is integer and could be POSIX between 1970 and 2100.
#' @keywords internal
is.int.POSIXct <- function(x, date_from = as.POSIXct("1970-01-01"), date_to = as.POSIXct("2100-01-01")){
  is.integer(x) && all(as.POSIXct(x) %between% c(date_from,date_to))
}

#' @title nrowDT
#' @description Return nrow if DT else NA.
#' @keywords internal
nrowDT <- function(x){
  if(any(c("data.frame","data.table") %in% class(x))) nrow(x) else NA_integer_
}