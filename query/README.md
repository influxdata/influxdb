# IFQL (Influx Query Language)

`ifqld` is an HTTP server for running **IFQL** queries to one or more InfluxDB
servers.

`ifqld` runs on port `8093` by default

### Specification
Here is the rough design specification for details until we get documentation up: http://bit.ly/platform/query-spec

### INSTALLATION
1. Upgrade to InfluxDB >= 1.4.1
https://portal.influxdata.com/downloads


2. Update the InfluxDB configuration file to enable **IFQL** processing; restart
the InfluxDB server. InfluxDB will open port `8082` to accept **IFQL** queries.

> **This port has no authentication.**

```
[ifql]
  enabled = true
  log-enabled = true
  bind-address = ":8082"
```

3. Download `ifqld` and install from https://github.com/influxdata/platform/query/releases

4. Start `ifqld` with the InfluxDB host and port of `8082`. To run in federated
mode (see below), add the `--host` option for each InfluxDB host.

```sh
ifqld --verbose --host localhost:8082
```

5. To run a query POST an **IFQL** query string to `/query` as the `q` parameter:
```sh
curl -XPOST --data-urlencode \
'q=from(db:"telegraf")
    |> filter(fn: (r) => r["_measurement"] == "cpu" AND r["_field"] == "usage_user")
    |> range(start:-170h)
    |> sum()' \
http://localhost:8093/query
```

#### docker compose

To spin up a testing environment you can run:

```
docker-compose up
```

Inside the `root` directory. It will spin up an `influxdb` and `ifqld` daemon
ready to be used. `influxd` is exposed on port `8086` and port `8082`.


### Prometheus metrics
Metrics are exposed on `/metrics`.
`ifqld` records the number of queries and the number of different functions within **IFQL** queries

### Federated Mode
By passing the `--host` option multiple times `ifqld` will query multiple
InfluxDB servers.

For example:

```sh
ifqld --host influxdb1:8082 --host influxdb2:8082
```

The results from multiple InfluxDB are merged together as if there was
one server.

### Basic Syntax

IFQL constructs a query by starting with a table of data and passing the table through transformations steps to describe the desired query operations.
Transformations are represented as functions which take a table of data as an input argument and return a new table that has been transformed.
There is a special function `from` which is a source function, meaning it does not accept a table as input, but rather produces a table.
All other transformation functions accept at least one table and return a table as a result.

For example to get the last point for each series in a database you start by creating a table using `from` and then pass that table into the `limit` function.

```
// Select the last point per series in the telegraf database.
limit(table:from(db:"telegraf"), n:1)
```

Since it is common to chain long lists of transformations together the pipe forward operator `|>` can be used to make reading the code easier.
These two expressions are equivalent:

```
// Select the last point per series in the telegraf database.
limit(table:from(db:"telegraf"), n:1)


// Same as above, but uses the pipe forward operator to indicate the flow of data.
from(db:"telegraf") |> limit(n:1)
```


Long list of functions can thus be chained together:

```
// Get the first point per host from the last minute of data.
from(db:"telegraf") |> range(start:-1m) |> group(by:["host"]) |> first()
```



### Supported Functions

Below is a list of supported functions.

#### from

Starting point for all queires. Get data from the specified database.

Example: `from(db:"telegraf")`

##### options
* `db` string
    `from(db:"telegraf")`

* `hosts` array of strings
    `from(db:"telegraf", hosts:["host1", "host2"])`

#### count

Counts the number of results

##### options

*  `useStartTime` boolean
Use the start time as the timestamp of the resulting aggregate.

Example: `from(db:"telegraf") |> count()`

#### filter

Filters the results using an expression

Example:
```
from(db:"foo")
    |> filter(fn: (r) => r["_measurement"]=="cpu" AND
                r["_field"] == "usage_system" AND
                r["service"] == "app-server")
    |> range(start:-12h)
    |> max()
```

##### options

* `fn` function(record) bool

Function to when filtering the records.
The function must accept a single parameter which will be the records and return a boolean value.
Records which evaluate to true, will be included in the results.


#### first

Returns the first result of the query

##### options

*  `useStartTime` boolean
Use the start time as the timestamp of the resulting aggregate.

Example: `from(db:"telegraf") |> first()`

#### group
Groups results by a user-specified set of tags

##### options

*  `by` array of strings
Group by these specific tag names
Cannot be used with `except` option

Example: `from(db: "telegraf") |> range(start: -30m) |> group(by: ["tag_a", "tag_b"])`

*  `keep` array of strings
Keep specific tag keys that were not in `by` in the results

Example: `from(db: "telegraf") |> range(start: -30m) |> group(by: ["tag_a", "tag_b"], keep:["tag_c"])`
*  `except` array of strings
Group by all but these tag keys
Cannot be used with `by` option

Example: `from(db: "telegraf") |> range(start: -30m) |> group(except: ["tag_a"], keep:["tag_b", "tag_c"])`

#### join

Join two time series together on time and the list of `on` keys.

Example:

```
cpu = from(db: "telegraf") |> filter(fn: (r) => r["_measurement"] == "cpu" and r["_field"] == "usage_user") |> range(start: -30m)
mem = from(db: "telegraf") |> filter(fn: (r) => r["_measurement"] == "mem" and r["_field"] == "used_percent") |> range(start: -30m)
join(tables:{cpu:cpu, mem:mem}, on:["host"], fn: (tables) => tables.cpu["_value"] + tables.mem["_value"])
```

##### options

* `tables` map of tables
Map of tables to join. Currently only two tables are allowed.

* `on` array of strings
List of tag keys that when equal produces a result set.

* `fn`

Defines the function that merges the values of the tables.
The function must defined to accept a single parameter.
The parameter is a map, which uses the same keys found in the `tables` map.
The function is called for each joined set of records from the tables.

#### last

Returns the last result of the query

##### options

*  `useStartTime` boolean
Use the start time as the timestamp of the resulting aggregate.

Example: `from(db: "telegraf") |> last()`

#### limit
Restricts the number of rows returned in the results.

Example: `from(db: "telegraf") |> limit(n: 10)`

#### map

Applies a function to each row of the table.

##### options

* `fn` function

Function to apply to each row. The return value of the function may be a single value or an object.

Example:
```
from(db:"foo")
    |> filter(fn: (r) => r["_measurement"]=="cpu" AND
                r["_field"] == "usage_system" AND
                r["service"] == "app-server")
    |> range(start:-12h)
    // Square the value
    |> map(fn: (r) => r._value * r._value)
```

Example:
```
from(db:"foo")
    |> filter(fn: (r) => r["_measurement"]=="cpu" AND
                r["_field"] == "usage_system" AND
                r["service"] == "app-server")
    |> range(start:-12h)
    // Square the value and keep the original value
    |> map(fn: (r) => ({value: r._value, value2:r._value * r._value}))
```

#### max

Returns the max value within the results

##### options

*  `useStartTime` boolean
Use the start time as the timestamp of the resulting aggregate.

Example:
```
from(db:"foo")
    |> filter(fn: (r) => r["_measurement"]=="cpu" AND
                r["_field"] == "usage_system" AND
                r["service"] == "app-server")
    |> range(start:-12h)
    |> window(every:10m)
    |> max()
```

#### mean

Returns the mean of the values within the results

##### options

*  `useStartTime` boolean
Use the start time as the timestamp of the resulting aggregate.

Example:
```
from(db:"foo")
    |> filter(fn: (r) => r["_measurement"] == "mem" AND
            r["_field"] == "used_percent")
    |> range(start:-12h)
    |> window(every:10m)
    |> mean()
```

#### min

Returns the min value within the results

##### options

*  `useStartTime` boolean
Use the start time as the timestamp of the resulting aggregate.

Example:
```
from(db:"foo")
    |> filter(fn: (r) => r[ "_measurement"] == "cpu" AND
               r["_field" ]== "usage_system")
    |> range(start:-12h)
    |> window(every:10m, period: 5m)
    |> min()
```


#### range
Filters the results by time boundaries

Example:
```
from(db:"foo")
    |> filter(fn: (r) => r["_measurement"] == "cpu" AND
               r["_field"] == "usage_system")
    |> range(start:-12h, stop: -15m)
```

##### options
* start duration
Specifies the oldest time to be included in the results

* stop duration or timestamp
Specifies exclusive upper time bound
Defaults to "now"

#### sample

Sample values from a table.

##### options

*  `useStartTime` boolean
Use the start time as the timestamp of the resulting aggregate.
* `n`
Sample every Nth element
* `pos`
Position offset from start of results to begin sampling
`pos` must be less than `n`
If `pos` less than 0, a random offset is used.
Default is -1 (random offset)

Example to sample every fifth point starting from the second element:
```
from(db:"foo")
    |> filter(fn: (r) => r["_measurement"] == "cpu" AND
               r["_field"] == "usage_system")
    |> range(start:-1d)
    |> sample(n: 5, pos: 1)
```

#### set
Add tag of key and value to set
Example: `from(db: "telegraf") |> set(key: "mykey", value: "myvalue")`
##### options
* `key` string
* `value` string

#### skew

Skew of the results

##### options

*  `useStartTime` boolean
Use the start time as the timestamp of the resulting aggregate.

Example: `from(db: "telegraf") |> range(start: -30m, stop: -15m) |> skew()`

#### sort
Sorts the results by the specified columns
Default sort is ascending

Example:
```
from(db:"telegraf")
    |> filter(fn: (r) => r["_measurement"] == "system" AND
               r["_field"] == "uptime")
    |> range(start:-12h)
    |> sort(cols:["region", "host", "value"])
```

##### options
* `cols` array of strings
List of columns used to sort; precedence from left to right.
Default is `["value"]`

For example, this sorts by uptime descending to find the longest
running instances.

```
from(db:"telegraf")
    |> filter(fn: (r) => r["_measurement"] == "system" AND
               r["_field"] == "uptime")
    |> range(start:-12h)
    |> sort(desc: true)
```

* `desc` bool
Sort results descending

#### spread

Difference between min and max values

##### options

*  `useStartTime` boolean
Use the start time as the timestamp of the resulting aggregate.

Example: `from(db: "telegraf") |> range(start: -30m) |> spread()`

#### stddev

Standard Deviation of the results

##### options

*  `useStartTime` boolean
Use the start time as the timestamp of the resulting aggregate.

Example: `from(db: "telegraf") |> range(start: -30m, stop: -15m) |> stddev()`

#### sum

Sum of the results

##### options

*  `useStartTime` boolean
Use the start time as the timestamp of the resulting aggregate.

Example: `from(db: "telegraf") |> range(start: -30m, stop: -15m) |> sum()`

### toBool

Convert a value to a bool.

Example: `from(db: "telegraf") |> filter(fn:(r) => r._measurement == "mem" and r._field == "used") |> toBool()`

The function `toBool` is defined as `toBool = (table=<-) => table |> map(fn:(r) => bool(v:r._value))`.
If you need to convert other columns use the `map` function directly with the `bool` function.

### toInt

Convert a value to a int.

Example: `from(db: "telegraf") |> filter(fn:(r) => r._measurement == "mem" and r._field == "used") |> toInt()`

The function `toInt` is defined as `toInt = (table=<-) => table |> map(fn:(r) => int(v:r._value))`.
If you need to convert other columns use the `map` function directly with the `int` function.

### toFloat

Convert a value to a float.

Example: `from(db: "telegraf") |> filter(fn:(r) => r._measurement == "mem" and r._field == "used") |> toFloat()`

The function `toFloat` is defined as `toFloat = (table=<-) => table |> map(fn:(r) => float(v:r._value))`.
If you need to convert other columns use the `map` function directly with the `float` function.

### toDuration

Convert a value to a duration.

Example: `from(db: "telegraf") |> filter(fn:(r) => r._measurement == "mem" and r._field == "used") |> toDuration()`

The function `toDuration` is defined as `toDuration = (table=<-) => table |> map(fn:(r) => duration(v:r._value))`.
If you need to convert other columns use the `map` function directly with the `duration` function.

### toString

Convert a value to a string.

Example: `from(db: "telegraf") |> filter(fn:(r) => r._measurement == "mem" and r._field == "used") |> toString()`

The function `toString` is defined as `toString = (table=<-) => table |> map(fn:(r) => string(v:r._value))`.
If you need to convert other columns use the `map` function directly with the `string` function.

### toTime

Convert a value to a time.

Example: `from(db: "telegraf") |> filter(fn:(r) => r._measurement == "mem" and r._field == "used") |> toTime()`

The function `toTime` is defined as `toTime = (table=<-) => table |> map(fn:(r) => time(v:r._value))`.
If you need to convert other columns use the `map` function directly with the `time` function.

### toUInt

Convert a value to a uint.

Example: `from(db: "telegraf") |> filter(fn:(r) => r._measurement == "mem" and r._field == "used") |> toUInt()`

The function `toUInt` is defined as `toUInt = (table=<-) => table |> map(fn:(r) => uint(v:r._value))`.
If you need to convert other columns use the `map` function directly with the `uint` function.


#### window
Partitions the results by a given time range

##### options
* `every` duration
Duration of time between windows

Defaults to `period`'s value
```
from(db:"foo")
    |> range(start:-12h)
    |> window(every:10m)
    |> max()
```

* `period` duration
Duration of the windowed parition
```
from(db:"foo")
    |> range(start:-12h)
    |> window(every:10m)
    |> max()
```

Default to `every`'s value
* `start` time
The time of the initial window parition.

* `round` duration
Rounds a window's bounds to the nearest duration

Example:
```
from(db:"foo")
    |> range(start:-12h)
    |> window(every:10m)
    |> max()
```

### Custom Functions

IFQL also allows the user to define their own functions.
The function syntax is:

```
(parameter list) => <function body>
```

The list of parameters is simply a list of identifiers with optional default values.
The function body is either a single expression which is returned or a block of statements.
Functions may be assigned to identifiers to given them a name.

Examples:

```
// Define a simple addition function
add = (a,b) => a + b

// Define a helper function to get data from a telegraf measurement.
// By default the database is expected to be named "telegraf".
telegrafM = (measurement, db="telegraf") =>
    from(db:db)
         |> filter(fn: (r) => r._measurement == measurement)

// Define a helper function for a common join operation
// Use block syntax since we have more than a single expression
abJoin = (measurementA, measurementB, on) => {
    a = telegrafM(measurement:measurementA)
    b = telegrafM(measurement:measurementB)
    return join(
        tables:{a:a, b:b},
        on:on,
        // Return a map from the join fn,
        // this creates a table with a column for each key in the map.
        // Note the () around the map to indicate a single map expression instead of function block.
        fn: (t) => ({
            a: t.a._value,
            b: t.b._value,
        }),
    )
}
```

#### Pipe Arguments

Functions may also declare that an argument can be piped into from an pipe forward operator by specifing a special default value:

```
// Define add function which accepts `a` as the piped argument.
add = (a=<-, b) => a + b

// Call add using the pipe forward syntax.
1 |> add(b:3) // 4

// Define measurement function which accepts table as the piped argument.
measurement = (m, table=<-) => table |> filter(fn: (r) => r._measurement == m)

// Define field function which accepts table as the piped argument
field = (field, table=<-) => table |> filter(fn: (r) => r._field == field)

// Query usage_idle from the cpu measurement and the telegraf database.
// Using the measurement and field functions.
from(db:"telegraf")
    |> measurement(m:"cpu")
    |> field(field:"usage_idle")
```

