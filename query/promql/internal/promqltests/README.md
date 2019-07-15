# PromQL End-to-end Tests 

The code under this directory is organized in the go submodule `github.com/influxdata/promqltests`.
This was made as a go submodule to avoid bringing unnecessary dependencies into InfluxDB.
This module is _not_ intended to be exported and used by other modules.

This module contains end-to-end tests for PromQL.
Those tests compare the results of a PromQL query against a Prometheus database to the results of a Flux query (transpiled from the PromQL one) against InfluxDB.

This submodule depends on InfluxDB, but it replaces the dependency with the local InfluxDB on filesystem.
This prevents developers to deal with updating the dependencies and makes CI detect the failures once one breaks something in InfluxDB.