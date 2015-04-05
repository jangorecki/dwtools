suppressPackageStartupMessages(library(dwtools))

# populate csv
star <- dw.populate(1e5, scenario="star")
invisible(lapply(names(star), function(tbl) write.table(star[[tbl]], file = paste(tolower(tbl),"csv",sep="."), sep=";", row.names = FALSE, col.names = TRUE, append = FALSE)))

# define source
fact <- c(fact = "sales.csv")
dims <- c(time = "time.csv", geography = "geography.csv", currency = "currency.csv", customer = "customer.csv", product = "product.csv")

# build CUBE
cube <- CUBE$new(
  fact = list(sales = fread(fact)),
  dim = lapply(dims, function(x){ dt <- fread(x); setkeyv(dt,names(dt)[1]) }),
  ref = list("sales-time"="time_code", "sales-currency"="curr_code", "sales-geography"="geog_code", "sales-customer"="cust_code", "sales-product"="prod_code")
)

# remove csv files
invisible(file.remove(c(fact, dims)))

## preview CUBE object

print(cube)
eapply(cube$DB, nrow) # nrow each
cube$factList # fact data types
cube$dimList[["geography"]] # geography dim data types
lapply(cube$dimList, names) # column names
rbindlist(cube$refList) # relations

## MDX like queries

# South and West region 2014's sales (default all num cols in fact) by geog_name and time_quarter_code
cube$MDX(
  rows = list(geography = "geog_name"),
  cols = list(time = "time_quarter_code"),
  from = "sales",
  where = list(
    time = quote(time_year_code == 2014), 
    geography = quote(geog_region_name %in% c("South","West"))
  )
)
# same but only single measure
cube$MDX(
  rows = list(geography = "geog_name"),
  cols = list(time = "time_quarter_code", sales = "value"),
  from = "sales",
  where = list(
    time = quote(time_year_code == 2014), 
    geography = quote(geog_region_name %in% c("South","West"))
  )
)

# sales value over time (year, month) and regions, filter for fiat currencies
cube$MDX(
  rows = list(time = c("time_year_code","time_month_code","time_month_name")),
  cols = list(sales = "value", geography = "geog_region_name"),
  from = "sales",
  where = list(currency = quote(curr_type == "fiat"))
)
# same but changed filter to grouping
cube$MDX(
  rows = list(time = c("time_year_code","time_month_code","time_month_name")),
  cols = list(sales = "value", geography = "geog_region_name", currency = "curr_type"),
  from = "sales"
)

# use time dimension on both rows and cols
cube$MDX(
  rows = list(time = "time_year_code"),
  cols = list(time = c("time_month_code","time_month_name"), sales = "value"),
  from = "sales"
)
