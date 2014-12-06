suppressPackageStartupMessages(library(data.table))
library(dwtools)
options("dwtools.verbose"=3)  # turn on status messages printed to console

# populate tick data
suppressPackageStartupMessages(library(Rbitcoin))
tick = market.api.process("hitbtc",c("BTC","USD"),"trades")[["trades"]]
print(tick)

# ohlc + vwap
DT = vwap(tick, "30 mins")
print(DT)

# same with timing
DT = timing(vwap(tick, "30 mins"), nrow(tick))
attr(DT,"timing")

# ohlc + vwap, aggregate using ceiling
DT = vwap(tick, "30 mins", "ceiling")
print(DT)
