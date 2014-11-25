dwtools
=======

**Current version: pre 1.0.0**

Data Warehouse tools. Extensions to `data.table` functionalities. See below for core functions in the package. Report any bug as issues on github.

Core functions
--------------

``` {.r}
library(dwtools)
#> Loading required package: data.table
#> dwtools (pre 1.0.0)
```

### dw.populate

Not core but it will populate data for the next examples

``` {.r}
SALES = dw.populate(scenario="fact")
head(SALES)
#>    cust_code prod_code state_code  date_code curr_code   amount
#> 1:     id088       649         UT 2010-01-02       CZK 883.8430
#> 2:     id097       139         NM 2011-09-08       IDR 507.5106
#> 3:     id087       527         NY 2010-01-10       IDR 423.3552
#> 4:     id044       702         OR 2013-02-21       FTC 304.5715
#> 5:     id020        78         NJ 2013-10-25       XDG 973.9260
#> 6:     id009       339         WI 2011-04-19       XVN 817.0486
#>           value
#> 1:     92.28056
#> 2: 191600.92622
#> 3: 803561.87327
#> 4: 435609.55580
#> 5: 998722.79516
#> 6: 850158.35636
```

### db

Function provides simple database interface. It handles DBI drivers, was tested with Postgres and SQLite, also RODBC (any odbc connection) might be supported already but it was not tested. NoSQL support in dev. In ETL terms where `data.table` serves as `transformation` layer, the dwtools `db` function serves `extraction` and `loading` layers.

``` {.r}
# setup db connection
library(RSQLite) # install.packages("RSQLite")
#> Loading required package: DBI
sqlite1 = list(drvName="SQLite",dbname="sqlite1.db",conn=dbConnect(SQLite(), dbname="sqlite1.db"))
options("dwtools.db.conns" = list(sqlite1=sqlite1, csv1=list(drvName="csv")))

# write to db
db(SALES,"sales_table")
# read from from db
db("sales_table")[0:6]
#>    cust_code prod_code state_code date_code curr_code   amount
#> 1:     id088       649         UT     14611       CZK 883.8430
#> 2:     id097       139         NM     15225       IDR 507.5106
#> 3:     id087       527         NY     14619       IDR 423.3552
#> 4:     id044       702         OR     15757       FTC 304.5715
#> 5:     id020        78         NJ     16003       XDG 973.9260
#> 6:     id009       339         WI     15083       XVN 817.0486
#>           value
#> 1:     92.28056
#> 2: 191600.92622
#> 3: 803561.87327
#> 4: 435609.55580
#> 5: 998722.79516
#> 6: 850158.35636
# query from db # not supported for csv driver
db("SELECT * FROM sales_table")[0:6]
#>    cust_code prod_code state_code date_code curr_code   amount
#> 1:     id088       649         UT     14611       CZK 883.8430
#> 2:     id097       139         NM     15225       IDR 507.5106
#> 3:     id087       527         NY     14619       IDR 423.3552
#> 4:     id044       702         OR     15757       FTC 304.5715
#> 5:     id020        78         NJ     16003       XDG 973.9260
#> 6:     id009       339         WI     15083       XVN 817.0486
#>           value
#> 1:     92.28056
#> 2: 191600.92622
#> 3: 803561.87327
#> 4: 435609.55580
#> 5: 998722.79516
#> 6: 850158.35636
# send to db # not supported for csv driver
db("DROP TABLE sales_table")

# populate dimension and save to db
GEOGRAPHY = dw.populate(scenario="star schema")$GEOGRAPHY
db(GEOGRAPHY,"geography")
rm(GEOGRAPHY)
# lookup from db and setkey
db("geography",key="state_code")[0:6]
#>    state_code state_name division_code      division_name region_code
#> 1:         AK     Alaska             9            Pacific           4
#> 2:         AL    Alabama             4 East South Central           2
#> 3:         AR   Arkansas             5 West South Central           2
#> 4:         AZ    Arizona             8           Mountain           4
#> 5:         CA California             9            Pacific           4
#> 6:         CO   Colorado             8           Mountain           4
#>    region_name
#> 1:        West
#> 2:       South
#> 3:       South
#> 4:        West
#> 5:        West
#> 6:        West
# all the above can be chained, including keys
setkeyv(SALES,"state_code")[,db("geography",key="state_code")[.SD]][0:6] # [...][...]
#>    state_code state_name division_code division_name region_code
#> 1:         AK     Alaska             9       Pacific           4
#> 2:         AK     Alaska             9       Pacific           4
#> 3:         AK     Alaska             9       Pacific           4
#> 4:         AK     Alaska             9       Pacific           4
#> 5:         AK     Alaska             9       Pacific           4
#> 6:         AK     Alaska             9       Pacific           4
#>    region_name cust_code prod_code  date_code curr_code   amount    value
#> 1:        West     id085       456 2012-09-28       MYR 202.9677 591036.2
#> 2:        West     id042       149 2010-11-09       AUD 195.9950 437804.4
#> 3:        West     id078       328 2014-02-11       KRW 443.5554 905961.1
#> 4:        West     id045       694 2013-07-18       ISK 456.1536 588006.9
#> 5:        West     id033       698 2014-06-17       SYP 667.7494 672761.0
#> 6:        West     id076       857 2014-07-11       EUR 986.6756 626315.0

# help: ?db
```

`db` accepts vector of sql statements / table names to allow batch processing. In case of tables migration see `?dbCopy`.

### joinbyv

Denormalization of star schema and snowflake schema to flat fact table.

``` {.r}
# populate star schema
X = dw.populate()
lapply(X, head)
#> $SALES
#>    cust_code prod_code state_code  date_code curr_code   amount
#> 1:     id088       649         UT 2010-01-02       CZK 883.8430
#> 2:     id097       139         NM 2011-09-08       IDR 507.5106
#> 3:     id087       527         NY 2010-01-10       IDR 423.3552
#> 4:     id044       702         OR 2013-02-21       FTC 304.5715
#> 5:     id020        78         NJ 2013-10-25       XDG 973.9260
#> 6:     id009       339         WI 2011-04-19       XVN 817.0486
#>           value
#> 1:     92.28056
#> 2: 191600.92622
#> 3: 803561.87327
#> 4: 435609.55580
#> 5: 998722.79516
#> 6: 850158.35636
#> 
#> $CUSTOMER
#>    cust_code cust_name      cust_mail cust_active
#> 1:     id001   grg rro cust1@mail.com       FALSE
#> 2:     id002   jjf cer cust2@mail.com       FALSE
#> 3:     id003   ohn myr cust3@mail.com        TRUE
#> 4:     id004   xzg mxr cust4@mail.com       FALSE
#> 5:     id005   fqe jym cust5@mail.com       FALSE
#> 6:     id006   xfn zsz cust6@mail.com       FALSE
#> 
#> $PRODUCT
#>    prod_code prod_name prod_group_code prod_group_name prod_family_code
#> 1:         1   prod 1y               1   prod group 1r                1
#> 2:         2   prod 2x               2   prod group 2r                2
#> 3:         3   prod 3t               3   prod group 3t                3
#> 4:         4   prod 4r               4   prod group 4d                4
#> 5:         5   prod 5q               5   prod group 5d                5
#> 6:         6   prod 6b               6   prod group 6e                6
#>    prod_family_name
#> 1:   prod family 1w
#> 2:   prod family 2b
#> 3:   prod family 3l
#> 4:   prod family 4x
#> 5:   prod family 5s
#> 6:   prod family 6h
#> 
#> $GEOGRAPHY
#>    state_code state_name division_code      division_name region_code
#> 1:         AK     Alaska             9            Pacific           4
#> 2:         AL    Alabama             4 East South Central           2
#> 3:         AR   Arkansas             5 West South Central           2
#> 4:         AZ    Arizona             8           Mountain           4
#> 5:         CA California             9            Pacific           4
#> 6:         CO   Colorado             8           Mountain           4
#>    region_name
#> 1:        West
#> 2:       South
#> 3:       South
#> 4:        West
#> 5:        West
#> 6:        West
#> 
#> $TIME
#>     date_code month_code month_name quarter_code year_code
#> 1: 2010-01-01          1    January            1      2010
#> 2: 2010-01-02          1    January            1      2010
#> 3: 2010-01-03          1    January            1      2010
#> 4: 2010-01-04          1    January            1      2010
#> 5: 2010-01-05          1    January            1      2010
#> 6: 2010-01-06          1    January            1      2010
#> 
#> $CURRENCY
#>    curr_code currency_type
#> 1:       ARS          fiat
#> 2:       AUD          fiat
#> 3:       AZN          fiat
#> 4:       BGN          fiat
#> 5:       BRL          fiat
#> 6:       BTC        crypto
# denormalize 
DT = joinbyv(master=X$SALES,
        join=list(CUSTOMER=X$CUSTOMER,PRODUCT=X$PRODUCT,GEOGRAPHY=X$GEOGRAPHY,TIME=X$TIME,CURRENCY=X$CURRENCY),
        col.subset=list(c("cust_active"),
                        c("prod_group_name","prod_family_name"),
                        c("region_name"),
                        c("month_name"),
                        NULL))[0:6]
print(DT)
#>    curr_code currency_type month_name   region_name prod_group_name
#> 1:       ARS          fiat    January         South  prod group 30u
#> 2:       ARS          fiat    January     Northeast  prod group 43q
#> 3:       ARS          fiat    January North Central  prod group 11e
#> 4:       ARS          fiat    January         South prod group 154w
#> 5:       ARS          fiat    January          West prod group 181m
#> 6:       ARS          fiat    January         South prod group 179e
#>    prod_family_name cust_active cust_code prod_code state_code  date_code
#> 1:  prod family 30v        TRUE     id095        30         OK 2010-01-01
#> 2:  prod family 43v        TRUE     id056       443         MA 2010-01-03
#> 3:  prod family 11o       FALSE     id021       811         ND 2010-01-03
#> 4:  prod family 54m        TRUE     id003       954         TX 2010-01-04
#> 5:  prod family 81i        TRUE     id068       581         WY 2010-01-04
#> 6:  prod family 79t        TRUE     id054       779         WV 2010-01-06
#>      amount     value
#> 1: 529.5890  29848.39
#> 2: 587.5603 270383.21
#> 3: 207.0269 925775.90
#> 4: 896.9127 153436.35
#> 5: 844.4536 419426.22
#> 6: 933.0257 752510.56

# help: ?joinbyv
```

### CJI

Also known as *Nth setkey*.
Creates custom indices for a data.table object. May require lot of memory.

``` {.r}
# not yet ready
# CJI()
```

Installation
------------

``` {.r}
# install dwtools
devtools::install_github("jangorecki/dwtools")
```

License
-------

GPL-3.
Donations are welcome and will be partially forwarded to dependencies of dwtools. `19JRajumtMNU9h9Wvdpsnq13SRdZjfbLeN`.
