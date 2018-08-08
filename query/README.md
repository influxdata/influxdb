# Flux - Influx data language

`fluxd` is an HTTP server for running **Flux** queries to one or more InfluxDB
servers.

`fluxd` runs on port `8093` by default

## Specification

A complete specification can be found in [SPEC.md](./docs/SPEC.md).

## Installation

### Generic Installation

1. Use InfluxDB nightly builds, which can be found here: https://portal.influxdata.com/downloads. Write data to the instance by using telegraf or some other data source.

**Note:** InfluxDB uses port `8082` for handling Flux queries. Ensure this port is accessible. _This port has no authentication._

2. Download `fluxd` from nightly builds: https://portal.influxdata.com/downloads .

3. Start `fluxd`. It will connect to the default host and port which is `localhost:8082`.
To run in federated mode, add the `--storage-hosts` option with each host separated by a comma.

```sh
fluxd --storage-hosts localhost:8082
```

4. To run a query, POST a **Flux** query string to `/query` as the `query` parameter:

```sh
curl -XPOST --data-urlencode \
'query=from(db:"telegraf")
    |> filter(fn: (r) => r["_measurement"] == "cpu" AND r["_field"] == "usage_user")
    |> range(start:-170h)
    |> sum()' \
http://localhost:8093/query?organization=my-org
```

Any value can be used for the `organization` parameter. It does not apply to running flux queries against the InfluxDB 1.x nightlies but is required.

### Docker Installation

There are now images for Flux and InfluxDB nightlies. If you have docker installed on your machine, this can be an easier method of trying out Flux on your local computer.

1. Create a docker network.

```sh
docker network create influxdb
```

2. Start the InfluxDB nightly. You can use either the `nightly` or `nightly-alpine` tag.

```sh
docker volume create influxdb
docker run -d --name=influxdb --net=influxdb -p 8086:8086 -v influxdb:/var/lib/influxdb quay.io/influxdb/influxdb:nightly
```

**Note:** If you run `influxd` in a container and `fluxd` outside of a container, you must add `-p 8082:8082` to expose the flux port.

3. Start fluxd using the nightly image. There is no `alpine` image for this yet.

```sh
docker run -d --name=flux --net=influxdb -p 8093:8093 quay.io/influxdb/flux:nightly
```

4. Follow the instructions from the General Installation section for how to query the server.

5. When updating, ensure that you pull both nightlies and restart them at the same time. We are making changes often and using the nightlies from the same night will result in fewer problems than only updating one of them.

### Prometheus metrics

Metrics are exposed on `/metrics`.
`fluxd` records the number of queries and the number of different functions within **Flux** queries

### Federated Mode

By passing multiple hosts to the `--storage-hosts` option, `fluxd` will query multiple InfluxDB servers.

For example:

```sh
fluxd --storage-hosts influxdb1:8082,influxdb2:8082
```

The results from multiple InfluxDB are merged together as if there was one server.

## Getting Started

Flux runs a query by reading a data source into a collection of tables and then passing each table through transformation steps to describe the desired query operations. Each table is composed of zero or more rows. Transformations are represented as functions which take a table of data as an input argument and return a new table that has been transformed. There are also special functions that combine and separate existing tables into new tables with a different grouping.

### Basic Syntax

All queries begin with the function `from`. It is a source function that does not accept any input, but produces a stream of tables as output. All queries must be followed by a `range` function which will limit the time range that data is selected from. Even if you want all data, you must specify a time range. This is so it is explicit that a user wants to query the entire database instead of the much more common range of time data.

```
from(db: "telegraf") |> range(start: -5m)
```

Function parameters are all keyword arguments. There are no positional arguments. The `from` function takes a single parameter: `db`. This specifies the InfluxDB database it should read from. At the moment, it is not possible to read from anything other than the default retention policy for a database. The `from` function will organize the data so that each series is its own table. That means that all transformations will happen _per series_ unless this is changed by using `group` or `window` (explained below). If you are familiar with InfluxDB 1.x, this is the opposite of InfluxQL's default behavior. InfluxQL would automatically group all series into the same table.

Functions are separated by the `|>` operator. This operator will take the stream of tables from one function and it will send it as input to another function that takes a stream of tables as the input. In this case, the `from` function outputs a stream of tables and sends that output to the `range` function which filters out any rows from each table that are not within the specified time range. The `range` function takes two parameters: `start` and `stop`. The default `stop` time is the current time and a duration, like `-5m`, can be used to specify a relative time from the current time. That means the above query is asking for all data within the last 5 minutes.

The `from` function creates a table where each row has the following attributes:

* `_measurement` - the measurement of the series
* `_field` - the field of the series
* `_value` - the output value
* `_start` - the start time of the table (equal to the range start)
* `_stop` - the stop time of the table (equal to the range stop)
* `_time` - the time for each row

The `|>` operator is used extensively in flux so you will see it in all of the query examples. Each transformation can be chained to another transformation so, while the examples below will be simple, they can be combined to yield the desired query and table structure.

### Filter rows with an anonymous function

The rows can be filtered by using the `filter` function. When communicating with `influxd`, the measurement name is put into the `_measurement` tag and the field name is put into the `_field` tag. If you wanted to filter by a specific measurement or field, you could do that by using filter like this:

```
from(db: "telegraf") |> range(start: -5m)
    |> filter(fn: (r) => r._measurement == "cpu" and r._field == "usage_user")
```

The `filter` function takes a function returning a boolean as its only parameter. The function parameter name is `fn`. An anonymous function is defined by using parenthesis to specify the function arguments, using the `=>` operator, and then either followed by a single line with the expression. See the User Defined Functions section for more information about defining functions and how to create your own.

When accessing data in a table, dot syntax or indexing syntax can be used. In the above, we used dot syntax. You can also use the indexing syntax like this: `r["_measurement"]`. This document will use the dot syntax, but either one can be used anywhere.

It is also common in flux to break up longer lines by including a newline between the different function calls. It is convention to have a newline followed by a tab and the pipe operator before writing the next function.

### Limit the number of rows

The `limit` function can be used to limit the number of points in each table. It takes a single parameter which is `n`.

```
from(db: "telegraf") |> range(start: -5m) |> limit(n: 1)
```

### Aggregates and Selectors

Aggregates and selectors will execute the aggregation/selection function on each table. The output is defined for each function, but most aggregates will output a single row for each table and most selectors will select a single row for each table.

To find the mean of each table, you could use the `mean()` function.

```
from(db: "telegraf") |> range(start: -5m) |> mean()
```

If you wanted to find the maximum value in each table, you could use `max()`.

```
from(db: "telegraf") |> range(start: -5m) |> max()
```

The full list of aggregation and selection functions is located in the spec.

### Grouping and Windowing

Since flux will group each series into its own table, we sometimes need to modify this grouping if we wanted to combine different series. As an example, what if we wanted to know the average user cpu usage for each AWS region? A common schema would be to have two tags: `region` and `host`. We would write that query like this:

```
from(db: "telegraf") |> range(start: -5m)
    |> filter(fn: (r) => r._measurement == "cpu" and r._field == "usage_user")
    |> group(by: ["region"])
    |> mean()
```

The `group` function would take every row that had the same `region` value and put it into a single table. If we had servers in two different regions, it would result in us having two different tables.

Similarly, if we wanted to group points into buckets of time, the `window` function can do that. We can modify the above function to give us the mean for every minute pretty easily.

```
from(db: "telegraf") |> range(start: -5m)
    |> filter(fn: (r) => r._measurement == "cpu" and r._field == "usage_user")
    |> group(by: ["region"])
    |> window(every: 1m)
    |> mean()
```

### Map

It is also possible to perform math and rename the columns by using the `map` function. The `map` function takes a function and will execute that function on each row to output a new row for the output table.

```
from(db: "telegraf") |> range(start: -5m)
    |> filter(fn: (r) => r._measurement == "cpu" and r._field == "usage_user")
    |> map(fn: (r) => {_value: r._value / 100})
```

The function passed into map must return an object. An object is a key/value structure. By default, the `map` function will merge any columns within the grouping key into the new row so you do not have to specify all of the existing columns that you do not want to modify. If you do not want to automtaically merge those columns, you can use `mergeKey: false` as a parameter to `map`.

**Note:** Math support is limited right now and the filter is required because the query engine will throw an error if the value is of different types with different series. So you must filter the results so only fields with a single type are selected at the moment.

## User Defined Functions

A user can define their own function which can then be reused. To do this, we use the function syntax and assign it to a variable.

```
add = (table=<-, n) => map(table: table, fn: (r) => {_value: r._value + n})
from(db: "telegraf") |> range(start: -5m)
    |> filter(fn: (r) => r._measurement == "cpu" and r._field == "usage_user")
    |> add(n: 5)
```

When defining a function, default arguments can be specified by using an equals sign. In addition, a table processing function can be specified by including one parameter that takes `<-` as an input. The typical parameter name for these is `table`, but it can be any name since the pipe operator does not use a specific name. In the above example, we build a new function around the existing `map` function by passing the table to the `map` function as a parameter instead of with the pipe. If you wanted to use the pipe operator instead, the following is also valid:

```
add = (table=<-, n) => table |> map(fn: (r) => {_value: r._value + n})
from(db: "telegraf") |> range(start: -5m)
    |> filter(fn: (r) => r._measurement == "cpu" and r._field == "usage_user")
    |> add(n: 5)
```
