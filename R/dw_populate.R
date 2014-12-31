
# dw.populate -------------------------------------------------------------

#' @title Populate DW data
#' @description Populate sample DW data. By default \emph{star}, see \code{scenario} arg for others.
#' @param N integer facts volume
#' @param K groups size
#' @param S integer used in \link{set.seed}
#' @param scenario character in \code{c("fact","star","denormalized")}
#' @param setkey logical used only for scenario \code{"star"}, default TRUE makes dimensions populated with keys already. For non-key benchmarks use FALSE, also remember about \code{getOption("datatable.auto.index"=FALSE)},
#' @param verbose integer print sub statuses
#' @details The following case \code{scenario="denormalized"} will invoke lookup for full columns set in all the dimensions. On the real data it is advised to use \code{scenario="star"} and later denormalize using \link{joinbyv} function where you can provide subsets of columns on each lookup.
#' @seealso \link{joinbyv}
#' @export
#' @example tests/dw_populate_examples.R
dw.populate <- function(N = 1e5, K = 1e2, S = 1, scenario = "star", setkey = TRUE, verbose = getOption("dwtools.verbose")){
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
    rbindlist(lapply(1:length(ct.dict), function(i) data.table(curr_code = ct.dict[[i]], curr_type = names(ct.dict[i]))))
  
  SALES <- data.table(
    cust_code = sample(CUSTOMER$cust_code, N, TRUE),
    prod_code = sample(PRODUCT$prod_code, N, TRUE),
    geog_code = sample(GEOGRAPHY$geog_code, N, TRUE),
    time_code = sample(TIME$time_code, N, TRUE),
    curr_code = sample(CURRENCY$curr_code, N, TRUE),
    amount = round(runif(N,max=1e3),4),
    value = round(runif(N,max=1e6),8)
  )
  if(scenario %in% c("star") && setkey){
    invisible(mapply(FUN = function(join, by) setkeyv(join,by),
                     join = list(CUSTOMER=CUSTOMER,PRODUCT=PRODUCT,GEOGRAPHY=GEOGRAPHY,TIME=TIME,CURRENCY=CURRENCY), 
                     by = list("cust_code","prod_code","geog_code","time_code","curr_code"), SIMPLIFY=FALSE))
  }
  DT <- switch(scenario,
               "fact" = SALES,
               "star" = list(SALES=SALES,CUSTOMER=CUSTOMER,PRODUCT=PRODUCT,GEOGRAPHY=GEOGRAPHY,TIME=TIME,CURRENCY=CURRENCY),
               "denormalize" = joinbyv(master=SALES, join=list(CUSTOMER=CUSTOMER,PRODUCT=PRODUCT,GEOGRAPHY=GEOGRAPHY,TIME=TIME,CURRENCY=CURRENCY), by=list("cust_code","prod_code","geog_code","time_code","curr_code")),
               stop("non supported scenario"))
  if(verbose > 0) cat(as.character(Sys.time()),": dw.populate: processed scenario '",scenario,"' volume N: ",N,", groups K: ",K,"\n",sep="")
  return(DT)
}
