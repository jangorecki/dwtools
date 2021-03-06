% Generated by roxygen2 (4.1.0): do not edit by hand
% Please edit documentation in R/vwap.R
\name{vwap}
\alias{vwap}
\title{OHLC and VWAP}
\usage{
vwap(x, units, trunc_ceiling = "trunc")
}
\arguments{
\item{x}{data.table of tick trades, columns: \emph{date, price, amount, tid, type}.}

\item{units}{time units period to aggregate.}

\item{trunc_ceiling}{character, default \emph{trunc}, also possible \emph{ceiling}.}
}
\value{
data.table aggregated to specified time units, \emph{price} column is VWAP.
}
\description{
Perform OHLC and VWAP on tick trades.
}
\details{
Rows where \code{amount==0} (normally should not happened) should be filtered before passing the tick data to the function.
}
\examples{
suppressPackageStartupMessages(library(dwtools))

# populate tick data
suppressPackageStartupMessages(library(Rbitcoin))
tick = market.api.process("hitbtc",c("BTC","USD"),"trades")[["trades"]]
print(tick)

# ohlc + vwap
DT = vwap(tick, "30 mins")
print(DT)

# same with timing
DT = timing(vwap(tick, "30 mins"), nrow(tick))
get.timing()

# ohlc + vwap, aggregate using ceiling
DT = vwap(tick, "30 mins", "ceiling")
print(DT)
}

