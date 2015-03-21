# dwtools

**Current version:** [0.8.3.8](NEWS.md)  

Data Warehouse and data integration helpers. Unifies database connectors to DBI, JDBC, ODBC, CSV. Easy multiple simultaneous databases connections managing. Data modelling helpers, denormalization of star schema and snowflake schema, basic normalization. And few more.

## Installation

```r
library(devtools)
install_github("jangorecki/dwtools")
```

## Features

For basic examples of each function see [introduction vignette](https://cdn.rawgit.com/jangorecki/b02bdfb7a2bdb010f6cc/raw/392f1e32e23443699d2481ca6d7aefab3fa15499/dwtools.html).

- [db](tests/example-db.R): simple database interface accross multiple various connections.
- [timing](tests/example-timing.R): measure function timing and process metadata, for extended logging see [logR](https://github.com/jangorecki/logR) package.
- [pkgsVersion](tests/example-pkgs_version.R): batch package version compare between libraries.
- [joinbyv](tests/example-joinbyv.R): batch join tables, denormalization of star schema or snowflake schema modeled data.
- [build_hierarchy](example-build_hierarchy.R): basic hierarchy detection.
- [eav](example-eav.R): [Entity-Attribute-Value](https://en.wikipedia.org/wiki/Entity%E2%80%93attribute%E2%80%93value_model) data manipulation.
- [shinyBI](https://jangorecki.shinyapps.io/shinyBI/): shinyApp for making aggregates on star schema data.
- [idxv](example-idxv.R): DT binary search on multiple keys, also known as *Nth setkey*.
- more in [introduction vignette](https://cdn.rawgit.com/jangorecki/b02bdfb7a2bdb010f6cc/raw/392f1e32e23443699d2481ca6d7aefab3fa15499/dwtools.html).

## License

GPL-3.  
Donations are welcome and will be partially forwarded to dependencies of dwtools: [19JRajumtMNU9h9Wvdpsnq13SRdZjfbLeN](https://blockchain.info/address/19JRajumtMNU9h9Wvdpsnq13SRdZjfbLeN)

## Contact

`J.Gorecki@wit.edu.pl`
