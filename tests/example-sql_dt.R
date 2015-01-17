suppressPackageStartupMessages(library(dwtools))

dt <- dw.populate(scenario="fact")
print(dt)

sql.dt(select = geog_code, from = dt, where = value %between% c(100,180))
sql.dt(geog_code, dt, value %between% c(100,180))

sql.dt(select = list(geog_code, time_code, value), from = dt, where = value < 100)
sql.dt(list(geog_code, time_code, value), dt, value < 100)

sql.dt(, dt, value < 100) # select * / .SD

sql.dt(
  select = list(sum_value=sum(value)),
  from = dt,
  group = list(geog_code),
  order = order(-sum_value)
)
sql.dt(list(sum_value=sum(value)), dt, , list(geog_code), order(-sum_value)) # the same

# update by reference
dt <- dw.populate(scenario="fact")
print(dt)
sql.dt(
  select = c("cust_curr_amount","cust_curr_value") := lapply(.SD,sum),
  from = dt,
  group = list(cust_code, curr_code),
  .SDcols=c("amount","value")
)

# the same but non update
dt <- dw.populate(scenario="fact")
sql.dt(
  select = c("cust_curr_amount","cust_curr_value") := lapply(list(amount,value),sum),
  from = copy(dt),
  group = list(cust_code, curr_code)
)
