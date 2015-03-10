suppressPackageStartupMessages(library(dwtools))

# basic product EAV
dt <- data.table(product=c(1,1,2,2,3,3),
                 attribute=rep(c('amount_in_pack','price'),3),
                 value=c(24,115,5,200,8,20.5),
                 key=c('product','attribute'))
dt
eav(dt, quote(price_of_pack := price * amount_in_pack))

# sources of income EAV, variable number of source
dt <- data.table(customer=c(1,2,2,2,3,3),
                 attribute=c('salary','salary','benefits','gambling','fraud','salary'),
                 value=c(560,490,120,85,200,380),
                 key=c('customer','attribute'))
dt
eav(dt, quote(total_income := rowSums(.SD,na.rm=TRUE)))

# sales of products over time
dt <- dw.populate(scenario='fact')[,.(prod_code,time_code,amount,value)][,melt(.SD, id=1:2,variable.name='measure',value.name='value')] # prepare EAV
setkey(dt,prod_code,time_code,measure)
dt
system.time(
  r <- eav(dt, quote(avg_price:=value/amount))
) # great timing even on big sets thanks to data.table!
r

# shift.on usage - calc price change over time
eav(r, quote(price_change := avg_price - shift(avg_price, 1L, NA, "lag")[[1L]]), shift.on='time_code')

# leave in wide format
eav(r, quote(price_change := avg_price - shift(avg_price, 1L, NA, "lag")[[1L]]), shift.on='time_code', wide=TRUE)
