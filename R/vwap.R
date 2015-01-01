
# technical ---------------------------------------------------------------

#' @title time_dict
#' @description Time units dictionary
#' @keywords internal
time_dict <- function(){
  data.table(label = c("mins","5 mins","10 mins","15 mins","30 mins","hours","3 hours","6 hours","12 hours","days"),
             unit = c("mins","mins","mins","mins","mins","hours","hours","hours","hours","days"),
             amount = c(1,5,10,15,30,1,3,6,12,1),
             seconds = c(60,60*5,60*10,60*15,60*30,60*60,60*60*3,60*60*6,60*60*12,60*60*24),
             key = "label")
}

#' @title round.POSIXct
#' @description Trunc or ceiling of POSIXct to particular time units
#' @keywords internal
round.POSIXct <- function(x, units, trunc_ceiling = "trunc", tz = "UTC"){
  H <- as.integer(format(x, "%H", tz = tz))
  M <- as.integer(format(x, "%M", tz = tz))
  S <- as.integer(format(x, "%S", tz = tz))
  secs <- 3600*H + 60*M + S
  origin <- format(x, "%Y-%m-%d", tz = tz)
  r <- getOption("dwtools.time.dict")[.(units),seconds]
  offset <- switch(trunc_ceiling, "ceiling" = 1, "trunc" = 0)
  as.POSIXct((trunc(secs/r)+offset)*r, origin = origin, tz = tz)
}

# vwap --------------------------------------------------------------------

#' @title OHLC and VWAP
#' @description Perform OHLC and VWAP on tick trades.
#' @param x data.table of tick trades, columns: \emph{date, price, amount, tid, type}.
#' @param units time units period to aggregate.
#' @param trunc_ceiling character, default \emph{trunc}, also possible \emph{ceiling}.
#' @details Rows where \code{amount==0} (normally should not happened) should be filtered before passing the tick data to the function.
#' @return data.table aggregated to specified time units, \emph{price} column is VWAP.
#' @export
#' @example tests/example-vwap.R
vwap <- function(x, units, trunc_ceiling="trunc"){
  stopifnot(nrow(x) > 0)
  x[, list(date, price, amount, tid, type,
           period_time = round.POSIXct(x=date, units=units, trunc_ceiling=trunc_ceiling)), # key
    ][, list(period_time = head(period_time,1),
             tid_open = head(tid,1), tid_close = tail(tid,1),
             timestamp_open = head(date,1), timestamp_close = tail(date,1),
             n = .N, bids = sum(type=="bid"), asks = sum(type=="ask"),
             open = head(price,1), high = max(price,na.rm=TRUE), low = min(price,na.rm=TRUE), close = tail(price,1),
             price = sum(price*amount,na.rm=TRUE)/sum(amount,na.rm=TRUE), volume = sum(amount,na.rm=TRUE)),
      keyby = list(period_int = as.integer(period_time))
      ]
}