# InfluxQL & Kapacitor 2.0
This document puts forth some ideas for the new data model of InfluxDB and a query language that unites Influx and Kapacitor into one. The goal is to have a query language that is easy to use and fits with the model of working with time series data. It should be extensible and easy for users to add new functions and build on top of some key primitives.

The rollout of this should be as an additional endpoint in InfluxDB and Kapacitor that can work with the existing data model or work with the new data model.

## Proposed Data Model & Terms

The new data model's goal is to simplify things for the users, while keeping the ability to represent other data models like InfluxDB 1.x, OpenTSDB, and Prometheus. The primary goal is to remove questions from the user about how to organize things and also let the data structures improve the clarity of the query language. The new model consists of databases, which have a retention period (how long they keep data around) and time series are identified by tag key/value pairs (note that measurements and fields are no longer a concept that exists). A series is a time ordered collection of time/value pairs. Values in a series must all be of the same type, either bool, int64, uint64, float64, or string. Queries can span multiple databases and can write into multiple databases so removing the separate retention policy heirarchy won't limit functionality.

### Definitions

The following definitions encompass the entire vocabulary of the data model. Where appropriate, JSON examples are given, but the protocol will also support a line or protobuf based structure.

* **tag** - a key/value pair of strings. (e.g. "region":"host" or "building":"2"). Tags MUST NOT start with `#`. Tag keys starting with `_` have special meaning and should not be used by the user unless they conform to the predefined set of keys that use `_`.
* **system tag** - A tag with a key that starts with `_` followed by letters and/or numbers. These are indexed just like tags, but are used by the system for compatibility layers with other systems.
* **meta** - A JSON style object associated with a series. This data is not indexed and can only be included/joined in a query. Its purpose is to add more context to series for user interfaces and additional calculations.
* **tagset** - an object of 1 or more tag key/value string pairs. Example:

```json
{
	"region":"host",
	"building":"2"
}
```

* **value** - a single value of type null, bool, int64, uint64, float64 (must support NaN), time, or string
* **epoch** - distance in time from UTC 00:00:00 01/01/1970. Step size for distance is determined by precision
* **precision** - the precision at which an epoch should be interpreted. Acceptable values:
  * s - seconds
  * ms - milliseconds
  * us - microseconds
  * ns - nanoseconds
* **time** - an RFC3339 or RFC3339Nano time string
* **point** - an object with value, epoch, time, and tagset. value is required. One and only one of either epoch or time MUST be present. Tagset is optional. Example:

```json
{
	"value": 23.1,
	"epoch": 1491499253,
	"tagset": {"host":"A"}
}
```

* **vector** - a time/epoch ordered collection of points
* **series** - an object with a tagset, metadata, and an array of points. All values in the vector must be of the same data type. Points in the vector will have tagsets that are the union of the series tagset and their own, if it is specified. Keys in the point tagset will override any keys from the series tagset. (e.g. a point in series {"host":"A","region":"west"} with a point tagset of {"host":"B"} will have the resulting tagset of {"host":"B","region":"west"}). Note that series could map to raw underlying database series or could be a combination of series or composed series from transformations. Example:

```json
{
	"id": 24,
	"meta": {
		"dataType": "float64",
		"metricType": "gauge"
	},
	"tagset": {
		"host": "A",
		"region": "B"
	},
	"points": [
		{"value":23.2, "epoch":1491499253},
		{"value":78.1, "epoch":1491499263, "tagset":{"host":"B"}}
	]
}
```

**Note** All values in a vector or series must be of the same type. There are methods to do conversion at query time.

* **matrix** - a collection of series. Example:

```json
[
	{
		"tagset": {
			"host": "A",
		},
		"points": [{"value":23.1, "epoch":1491499253}, {"value":56.2, "epoch":1491499263}]
	},
	{
		"tagset": {
			"host": "B"
		},
		"points": [{"value":23.1, "epoch":1491499253}, {"value":56.2, "epoch":1491499263}]
	}
]
```

* **column vector** - a vector that is the column in a matrix, so each point is from a different series.
* **raw series** - a series that maps to an underlying database series. Note that the **series** object can be a combination of various **raw series** or transformations of those. A raw series would take the same form as a series with the restrictions that all points in the vector have identical tagsets and that no points have the same epoch/time. Raw series also have a unique `uint64` id.
* **raw point** - a point that maps to an individual time/value pair in a raw series. The **point** object from above could be an aggregate computed on the fly or a mathematic transformation with other points. Raw points exist in the underlying database and have their parent series' tagset and the point's time as a composite unique identifier.
* **database** - an object with a name and a retention [duration string](https://golang.org/pkg/time/#ParseDuration). The duration determines how long data is kept in the database. It is a container of raw series. Names must be a string and can contain `_`, `-`, `0-9`, `a-z`, `A-Z`. Database names MUST not start with `_`. Note that retention policies are no longer a part of the model. Example:

```json
{
	"name": "foo",
	"retention": "0"
}
```

### Proposed Write Protocol

The primary write protocol is a line protocol. In the context of a write, only **raw points** can be written. The line protocol contains some shortcuts to avoid the duplication of shared information like tags and time, but ultimately those will be converted into raw points, which are written into raw series (meaning that points in a series MUST have a unique time or they will overwrite previously written points).

The protocol can also include comments, which are lines that start with `#` followed by a space. Lines that start with `##` specify a block of meta data for the line immediately following.

The most basic style of the protocol has the tag key/value pairs separated by `:` with a trailing space, a value, and a trailing epoch or time. The structure looks like this:

```
<tagset> <value> <time>
```

All three sections are required. Here are the rules for each section.

#### Tagset

The tagset is the key/value pairs that identify the series. A line MUST have at least one tag key/value pair. They should be separated by `:`. Spaces, commas, colons, and backslashes in either a key or value MUST be escaped by a backslash.

#### Value

The value block can be any of the value types: boolean, float64, int64, uint64, time, string, or bytes. Null values should never be specified as they aren't stored in the database. Boolean is represented with `t` or `f`. Int64, UInt64 must have a trailing `i` or `u`, while all numbers without the trailing character are parsed as float64. Times must be in RFC3339Nano format. Strings should be wrapped in double quotes, which means any double quote within a string must be escaped with a backslash. Bytes look like an array of numbers: `[102, 111, 111]`

#### Time

The time block can be specified as either an epoch or an RFC3339Nano string. If an epoch is used it should be followed by its precision. However, if no precision is specified, the system will guess the precision by whatever is closest to the current time on the server receiving the write.

#### Meta data

*TODO:* give this more definition.

Meta data can be written with a line starting with `##`. The meta data will be associated with the series on the next line. Meta data is a JSON object. Keys in the object starting with `_` are reserved for the system. The data in meta is not indexed. The only validation is that it is a parseable JSON object.

#### Examples

```text
system:cpu,region:cpu,host:a,metric:user_idle 23.2 1491675816
system:cpu,region:cpu,host:a,metric:user_idle 23.2 1491675816s
system:cpu,region:cpu,host:a,metric:user_idle 23.2 1491675816002ms
system:cpu,region:cpu,host:a,metric:user_idle 23.2 2017-04-08T14:23:54Z
etype:error,app:paul's\ app t 1491675816000000
some\:key:foo 2i 1491675816
service:apache,host:serverA,region:west "127.0.0.1 - frank [10/Oct/2000:13:55:36 -0700] \"GET /apache_pb.gif HTTP/1.0\" 200 2326" 2017-04-18T14:58:00Z
```

#### gRPC

Writes can come in through gRPC. Here's the `proto` definition for the schema and service. (TODO)

### Representing the InfluxDB 1.x Data Model

The InfluxDB 1.x data model can be represented by using special `_measurement` and `_field` tag keys to split measurement and fields up. Here's an example:

```text
# InfluxDB 1.x
cpu,host=A,region=west user=16.4,system=23.2 1491675816

# Influx 2.0 example
_measurement:cpu,host:serverA,region:west,_field:user 16.4 1491675816
_measurement:cpu,host:serverA,region:west,_field:system 23.2 1491675816
```

### Representing the Prometheus/OpenTSDB Data Model

The Prometheus data model can be represented by using the special `_metric` tag key. Here's an example:

```text
# Prometheus
node_cpu{cpu="cpu0",mode="user"} 182168.46875

# Influx 2.0
_metric:node_cpu,cpu:cpu0,mode:user 182168.46875 1491675816
```

### Pre-defined tag keys

There are a set of tag keys used by the system that start with `_` that are well known. Some of them are for conversions from Influx 1.x or Prometheus while others are for information useful in graphing.

* **_units** - the units of the measurement - 
* **_type** - the type of the series - [int64, uint64, float64, bool, string]
* **_samplingRate** - duration string for how frequently this series is expected to be sampled. `0` indicates that it is an event stream (irregular series)
* **_metric** - for Prometheus mapping, the metric name
* **_measurement** - for InfluxDB 1.x mapping, the measurement name
* **_field** - for InfluxDB 1.x mapping, the field name
* **_description** - a human readable description of the series. Like Prometheus' HELP.

## Proposed Query Language

The new query language is functional in style, but I would call it a streaming query language because the engine should process parts of function inputs as they come in rather than getting the entire input at once. From the user's perspective it should look purely functional, but the engine should optimize queries to be streamed. Functions take named arguments with the argument name and value separated by `:` and arguments separated by `,`. String arguments are wrapped in double quotes or backticks. If using double quotes, any double quotes in the string must be escaped by backslash. If using backticks, double quotes do not need escaping. Function and argument names should use camel casing like in Javascript.

A **query** is made up of a number of **statements**, which can include variable assignments. The result set sent back is the last set of statements separated by commas. In the case of multiple statements returning values, they will be streamed back to the client in order of the statements from left to right. Queries can also include comments, which are started with `//` and continue until a newline.

Without using real function or argument names, here's an example of what the query language looks like:

```javascript
// as one line
funcA(argOne: "some thing", argTwo: ['foo', 'bar']).funcB(arg: /some regex/).funcC(arg: `here "is a" string`)
```

```javascript
// or separating for readability with newlines
funcA(argOne: "some thing", argTwo: ['foo', 'bar'])
    .funcB(arg: /some regex/)
    .funcC(arg: `here "is a" string`)
```

```javascript
// or even more newline separation
funcA(
    argOne: "some thing",
    argTwo: ['foo', 'bar'])
.funcB(
    arg: /some regex/)
.funcC(
    arg: `here "is a" string`)
```

```javascript
// variable assignment and multiple returns
var foo = funcA()

foo.transformOne(), foo.transformTwo()
```

The query language doesn't aim to be the most terse representation possible. Rather, it is meant to be readable and, more importantly, extensible. New functions should be easy to define and add to the language. Existing functions should be able to get new optional arguments without breaking existing clients.

This documentation lays out the functions in the query language. For each function we define the following:

* **accepts** what data types (or function returns) a function can be chained off. For example `range` accepts the result of a select: `select(...).range(...)`
* **arguments** named arguments for a function. Required arguments will be marked and the first listed

The passed in value for a function argument can be either the direct type (like a string), a variable holding the proper type, or another function in the language that returns the correct type. For example, a function argument that takes an array of strings could take `f(arg:['foo','bar'])` or it could take `f(arg:someFuncThatReturnsStringArray())`.

### The Data Model

We can represent data in the database as a matrix that can be manipulated. It's helpful to think of the data this way when working with the query language since it is a series of functions performing transormations on the matrix of time series. You can also think of them as data frames. For example, here is a basic matrix of series data:

|                           Series | 10  | 20  | 30  | 40
|                              --- |:---:|:---:|:---:|:---:
|`{"host":"a","metric":"cpu_user"}`|23.1 |25.3 |28.9 |35.2
|`{"host":"b","metric":"cpu_user"}`|76.2 |50.1 |56.3 |65.0

An entire database is really just a very large matrix like the one shown above. In the matrix, each row is a series. The first column identifies the series key. Each column after is a time and the columns will always be in time order (ascending by default, but reversible). The series column contains the tags that identify a given series. Each value cell will contain a value, and optionally, tag data specific to that point. As JSON, this matrix would look like this:

```json
{
	"matrix":[
		{
			"series": {
				"host": "a",
				"metric": "cpu_user"
			},
			"points": [
				{"epoch":10,"value":23.1},
				{"epoch":20,"value":25.3},
				{"epoch":30,"value":28.9},
				{"epoch":40,"value":35.2}
			]
		},
		{
			"series": {
				"host": "b",
				"metric": "cpu_user"
			},
			"points": [
				{"epoch":10,"value":76.2},
				{"epoch":20,"value":50.1},
				{"epoch":30,"value":56.3},
				{"epoch":40,"value":65.0}
			]
		}
	]
}
```

#### Functions on series in a matrix

Functions can either operate on the values in a series, or on the series column themselves. For example, say we have this matrix `foo`:

|                           Series | 10  | 20  
|                              --- |:---:|:---:
|`{"host":"a","metric":"cpu_user"}`| 12  | 13
|`{"host":"b","metric":"cpu_user"}`| 5   | 6

If we called `foo.sum()` we would get the following matrix:

|                           Series | 10
|                              --- |:---:
|`{"host":"a","metric":"cpu_user"}`| 25
|`{"host":"b","metric":"cpu_user"}`| 11

We can see that sum was applied across each row individually. That is, sum all the value columns across a row.

#### Column oriented functions

Most functions in the language operate on the individual series. However, there are some functions that operate on the columns in the matrix. Specifically, the functions `join` and `filter`. The `sort` function is a special case that can combine both row and column operations.

### Query Language Data Types

Functions and their arguments take and return defined data types. Here's the list of types

* int (ex: `var foo = 2`)
* float (ex: `var foo = 2.2`)
* duration (ex: `10m` or `-1h`)
* string (ex: `var foo = "hello"`)
* key identifier - used in expressions (ex: `'some_key'`)
* time - can be a duration, which will be calculated from now, a second epoch, a second epoch with a `.` followed by either milliseconds, microseconds, or nanoseconds, or an RFC3339Nano string. If a query has any relative times (durations), all should be calculated from the same `now` timestamp.
* matrix
* array
* map

### Operators

Operators are used in expressions (the `where` argument in `select` or `exp` in `join` and `filter`)

* `<` less than
* `<=` less than or equal to
* `>` greater than
* `>=` greater than or equal to
* `=` equal to
* `!=` not equal
* `=~` regex match (right hand side must be regex)
* `!~` regex doesn't match (right hand side must be regex)
* `or` logical OR
* `and` logical AND
* `in` the tag value for the tag key on the left is one of the members in the string literal array on the right
* `not` negates the next operator like `in`
* `empty` used against tags or strings to match against either an empty string or not present value
* `startsWith` the tag value for the tag key on the left starts with the string literal on the right

### Functions

#### Databases
`databases` returns an array of database objects.

```javascript
// Accepts: start
// Arguments: none
// Returns: an array of database objects
databases()
```

#### Create Database
`createDatabase` will create a database with the given retention period.

```javascript
// Accepts: start
// Arguments:
//   name - a required string that must start with [0-9]|[a-z]|[A-Z]. It can contain [0-9]|[a-z]|[A-Z]|-|_
//   retention - a duration for how long to keep data. Optional, 0 (keep forever) is the default
// Returns: error string, empty if none
createDatabase(name:"foo")
```

#### Drop Database
`dropDatabase` will drop the specified database.

```javascript
// Accepts: start
// Arguments:
//   name - required string
// Returns: error string, empty if none
dropDatabase(name:"foo")
```

#### Select
`select` will return a matrix of series with empty vectors based on selection criteria. By default, select will limit the time range of the index it checks to the most recent data (depending on configuration this could be 24h-7d). For exact time matching use select to filter down the number of series and then perform a query that returns the count of results in that time window, then filter out all zero counts. To get any time series data back from the database, the `range` function must be chained off the select. For limiting the number of series that get returned you can use the `limit` function. You can also order the series using `sort`.

**NOTE:** A major difference between 2.0 and 1.x is that by default, series will not be merged together. That is, all `select` statements will yield a matrix with each individual series. This is what `group by *` would do in InfluxQL 1.x. With InfluxQL 2.0 it is now the default behavior.

```javascript
// Accepts: start, matrix
// Arguments:
//   database - a string, required if not chained off a matrix
//   where - an expression to match against tags
// Returns: a matrix with only the series column (empty points arrays)
select(database:"foo") // get all series for database foo
```

##### The `where` expression

The `where` argument takes an expression that matches predicates against tag information. The where argument is wrapped in curley braces `{` `}`. Tag keys must be wrapped in single quotes and tag values must be wrapped in double quotes. Here are some examples:

```javascript
select(database:"foo", where:{'metric'="cpu"})
select(database:"foo", where:{'metric'="cpu" OR 'metric'="mem"})
select(database:"foo", where:{('metric'="cpu" OR 'metric'="mem") AND 'host'="serverA"})
select(database:"foo", where:{'host' in ["a","b"]})
select(database:"foo", where:{'host' NOT EMPTY})

// variables can be referenced in the where expression
var hostName = "serverA"
select(database:"foo", where:{'host' = hostName})
```

#### Range
`range` will get results by the time range specified. If chained against `select` it will pull the data from the database. If chained against a matrix, the points in the matrix will be filtered by the range. The range function could be updated with new arguments later to do things like filter out or keep periodic intervals (e.g. range for 4 weeks of data, but exclude Saturdays and Sundays).

```javascript
// Accepts: select or matrix
// Arguments:
//   start - a time. Could be relative e.g. `-4h` or an epoch, or a time string
//   end - a time
//   count - an integer. Get this number of values either from the passed 
//           in start or end time. If neither time is passed, count will return 
//           the most recent points. If both start and end are specified, it will
//           return the count number of points from start time to end or cut off before
//           count is hit if the end time comes first
// Returns: a matrix
select(database:"foo").range(start:-4h)
```

#### Clear
`clear` will remove series from a matrix that have no points so they are not returned in the response.

```javascript
select(database:"foo", where:{'metric'="cpu"})
  .range(start:-1h)
  .clear()
```

#### Casting types
The value types in a matrix can be cast using the type casting functions. When converting from string to int64, float64 or others they will be parsed. Errors can either be ignored or returned.

##### Float
`float` will convert a single value or all values in a matrix to `float64` type values.

```javascript
// Accepts: matrix, int64, uint64, bool, string
// Arguments:
//   fixed - an integer specifying how many decimal places to round to
//   error - boolean indicating if it should fail on casting error. default is false
// Returns: matrix of float64 values or other value converted to a float64
```

##### Int
`int` will convert a single value or all values in a matrix to `int64` type values.

```javascript
// Accepts: matrix, float64, uint64, bool, string
// Arguments:
//   error - boolean indicating if it should fail on casting error. default is false
// Returns: matrix of int64 values or other value converted to a int64
```

##### UInt
`uint` will convert a single value or all values in a matrix to `uint64` type values.

```javascript
// Accepts: matrix, float64, int64, bool, string
// Arguments:
//   error - boolean indicating if it should fail on casting error. default is false
// Returns: matrix of uint64 values or other value converted to a uint64
```

##### String
`string` will convert a single value or all values in a matrix to `string` type values.

```javascript
// Accepts: matrix, float64, int64, uint64, bool
// Arguments:
//   error - boolean indicating if it should fail on casting error. default is false
// Returns: matrix of string values or other value converted to a string
```

##### Bool
`bool` will convert a single value or all values in a matrix to `bool` type values.

```javascript
// Accepts: matrix, float64, int64, uint64, string
// Arguments:
//   error - boolean indicating if it should fail on casting error. default is false
// Returns: matrix of bool values or other value converted to a bool
```

#### Window
`window` will group points is each series in a matrix based on the windowing criteria. These windows can then have functions applied to them to produce aggregates or summaries of each.

```javascript
// Accepts: a matrix
// Arguments:
//   every - start a window after this duration. Must be less than or equal to period.
//   period - windows are for this duration. If only every or period is specified,
//            the other is set to the same value by default. If the every duration
//            is less than the period, windows will overlap.
//   count - number of windows (points) desired. Will adjust bucket durations
//           based on this count and the start and end time. Either count or
//           every & period can be specified, but not both.
//   start - optional, time to start the windows. If not specified, the time to start
//           will be the time of the first value in each series
//   round - optional, duration to round the start time to
// Returns: matrix
```

For example, we start with this matrix `foo`:

|                           Series | 10  | 20  | 30  | 40
|                              --- |:---:|:---:|:---:|:---:
|`{"host":"a","metric":"cpu_user"}`| 12  | 13  | 18  | 14
|`{"host":"b","metric":"cpu_user"}`| 6   | 5   | 3   | 2

And we call `foo.window(every:20s)` we are conceptually splitting the series into windows demarcated every 20 seconds. For the aid of this visualization we have empty columns to indicate the start and end of a window:

|                           Series |1.start|  10 |  20 |1.end|2.start| 30  | 40  | 2.end
|                              --- |-------|:---:|:---:|-----|-------|:---:|:---:|---
|`{"host":"a","metric":"cpu_user"}`|       | 12  | 13  |     |       | 18  | 14  |
|`{"host":"b","metric":"cpu_user"}`|       | 6   | 5   |     |       | 3   | 2   |

We see that there are now columns that mark the start and end of each window. This is meant as a temporary stage before calling some sort of aggregate or selector function to be applied to each cell. For example if we had called `foo.window(every:20s).sum()` we would have received the following result:

Series | 10  | 30
   --- | --- | ---
`{"host":"a","metric":"cpu_user"}`| 48.4 | 64.1
`{"host":"b","metric":"cpu_user"}`| 126.2 | 121.3

If we had called sum twice like this `foo.window(every:20s).sum().sum()`, it would collapse into a single column (which is the same as what `foo.sum()` would produce. Note that not all aggregates behave like this. For example `foo.mean()` would be different than `foo.window(every:20s).mean().mean()`):

Series | 10
   --- | ---
`{"host":"a","metric":"cpu_user"}`| 112.5
`{"host":"b","metric":"cpu_user"}`| 247.5

##### Overlapping Windows
The windows can also be overlapping. For example, if we specified a period of `20s` but created a window every `10s` we would get overlapping windows. So if we call `foo.window(period:20s, every:10s)` we would get:

Series | 1.start | 10  | 2.start | 20  | 1.end | 3.start | 30  | 2.end | 40  | 3.end
   --- | ------- | --- | ------- | --- | ----- | ------- | --- | ----- | --- | ---
`{"host":"a","metric":"cpu_user"}`| |23.1| | 25.3| | | 28.9| | 35.2 |
`{"host":"b","metric":"cpu_user"}`| |76.2| | 50  | | | 56.3| | 65.0 |

And if we created a sum of the data: `foo.window(period:20s, every:10s).sum()` we would get:

Series | 10  | 20  | 30
   --- | --- | --- | ---
`{"host":"a","metric":"cpu_user"}`| 48.4 | 54.2 | 64.1
`{"host":"b","metric":"cpu_user"}`| 126.2 | 106.3 | 121.3

##### Starting all windows at the same time
If we specify a `start` argument, this will be the time that all series are started from. This makes it easy to ensure that all series in a matrix will have buckets on the same boundaries. For example, if we have:

```javascript
// example using windows from a given start time
select(database:"foo",where:{'metric' = "cpu"})
  .range(start:-1h)
  .window(period:10m,start:-1h)
// if executed at 13:03:59, windows would occur at 12:03:59, 12:13:59, 12:23:59, etc.

select(database:"foo",where:{'metric' = "cpu"})
  .range(start:-1h)
  .window(period:10m,start:-1h,round:10m)
// if executed at 13:03:59, windows would occur at 12:00:00, 12:10:0, 12:20:0, etc.
```

#### Merge
`merge` will merge series in a matrix together based on the grouping criteria. Note that all values in a series must be of the same type. If a type difference is encountered, merge will cast based on the type of the first series in the matrix. You can use the type casting functions to force a consistent type before a call to merge. (should we have merge throw an error instead and direct the user to typecast?)

```javascript
// Accepts: matrix
// Arguments:
//   keys: an array of strings. This acts like a group by clause in SQL
//   keep: an array of strings. These are tag key data that should be kept in the individual points
//   error: boolean to return an error on type casting issue. Defaults to false
// Returns: matrix
select(database:"foo",where:{'metric'="network_in"})
  .merge(keys:["region"])
```

We'll use tables as an example to show how it works. For example, if we have the following matrix `foo`:

| Series                                      |  10 |  20 |
|---------------------------------------------|:---:|:---:|
|`{"host":"a","metric":"network","dc":"west"}`|  1  |  1  |
|`{"host":"b","metric":"network","dc":"east"}`|  2  |  2  |
|`{"host":"c","metric":"network","dc":"east"}`|  3  |  3  |
|`{"host":"d","metric":"network","dc":"west"}`|  4  |  4  |

If we called `foo.merge(keys:["metric"])` we would get a matrix that looks something like this:

| Series               |10 `host:a,dc:west`|10 `host:b,dc:east`|10 `host:c,dc:east`|10 `host:d,dc:west`|20 `host:a,dc:west`|20 `host:b,dc:east`|20 `host:c,dc:east`|20 `host:d,dc:west`|
|----------------------|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|
|`{"metric":"network"}`|  1  |  2  |  3  |  4  |  1  |  2  |  3  |  4  |

Note that in results that get returned to the client, only the series information would be retained, unless we pass in an argument to keep certain keys as tagsets on the individual points.

We can merge based on multiple tag keys. For example if we called `foo.merge(keys:["metric", "dc"]) we would get a matrix that looks like this:

| Series                           |  10 `host:a`| 10 `host:b`| 10 `host:c`| 10 `host:d`| 20 `host:a`| 20 `host:b`| 20 `host:c`| 20 `host:d`|
|----------------------------------|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|
|`{"metric":"network","dc":"west"}`|  1  |     |     |  4  |  1  |     |     |  4  |
|`{"metric":"network","dc":"east"}`|     |  2  |  3  |     |     |  2  |  3  |     |

#### Keys
`keys` will return an array of tag keys that occur in the matrix. The output of keys function can be used as the predecessor argument to `empty` or `not empty` in a `where` expression. For pagination through keys, `limit` can be chained off it.

```javascript
// Accepts: matrix
// Arguments: none
// Returns: []string
select(database:"foo").keys()
```

#### Values
`values` returns a string array of values in sorted order for a given key and the given criteria. For pagination through values, `limit` can be chained off of it.

```javascript
// Accepts: matrix
// Arguments:
//   key - required argument, a string of what tag key to return values for
// Returns: []string
select(database:"foo").values(key:"metric")
```

#### Cardinality
`cardinality` returns a map of the count of unique tag values for each key in the matrix.

```javascript
// Accepts: matrix
// Arguments:
//   keys - optional string array. only return the cardinality for the passed keys
// Returns: matrix
select(database:"foo", where:{'region'="west"})
  .cardinality()
```

#### Limit
`limit` accepts either a matrix or an array and will limit the number of results. In the case of a matrix, it limits the number of series (or rows) that are returned. In the case of an array it is used to take a slice. It's a good practice to always apply a limit of some kind.

```javascript
// Accepts: matrix, array
// Arguments:
//   n - the number of results to limit to, required argument.
//   offset - the offset (inclusive) to start with. defaults to 0.
// Returns: a matrix if passed a matrix, an array if passed an array
//
select(database:"foo").limit(n:10,offset:20) // get the third page of results
```

#### Time Shift
`timeShift` will shift all returned timestamps in the matrix by the given duration.

```javascript
// Accepts: a matrix
// Arguments:
//   duration - a duration
// Returns: matrix
select(database:"foo").range(start:"2017-01-01")
  .timeShift(duration:-168h) // shift everything to a week before
```

#### Interpolate
`interpolate` will normalize the time series in a matrix based on different criteria. It could work across columns or on the series themselves.

```javascript
// Accepts: matrix
// Arguments:
//   start - time, ensure data exists starting at this time
//   every - duration, ensure that a value exists at every duration period from start
//   value - integer, float, string, bool, or expression to fill with. Expressions can
//           use functions calculated from the series like mean($)
// Returns: a matrix
select(database:"foo",where:{'metric'="requests"})
  .range(start:-4h)
  .clear()
  .window(period:10m)
  .sum()
  .interpolate(start:-4h,every:10m,value:{mean($)})
```

#### Join
`join` operates on the columns in a matrix to combine series together. The `interpolate` function can be used to ensure that timestamps for the series line up and can be matched together.

```javascript
// Accepts: matrix
// Arguments:
//   keys - the tag keys to group the join by
//   exp - the expression to join series on
// Returns: matrix
select(database:"foo",where:{'metric'="errors" OR 'metric'="requests"})
  .range(start:-30m)
  .window(period:10m)
  .sum()
  .interpolate(start:-30m,value:0)
  .join(keys:["host"],exp:
    {('metric'."errors".float()/'metric'."requests" * 100).float(fixed:1) AS 'metric'."error_rate"})
```

We'll use the query from above to show an example of how join works. Say we start with this data after the the `interpolate` function (the input to `join`). Assuming run at `2017-07-06 06:00`:

| Series                           |2017-07-06 05:30|2017-07-06 05:40|2017-07-06 05:50|
|----------------------------------|:--------------:|:--------------:|:--------------:|
|`{"host":"a","metric":"errors"}`  |       0        |       3        |       2        |
|`{"host":"b","metric":"errors"}`  |       1        |       0        |       0        |
|`{"host":"a","metric":"requests"}`|       50       |       60       |       50       |
|`{"host":"b","metric":"requests"}`|       37       |       29       |       0        |

The call to `join` would result in a matrix that looks like this:

| Series                             |2017-07-06 05:30|2017-07-06 05:40|2017-07-06 05:50|
|------------------------------------|:--------------:|:--------------:|:--------------:|
|`{"host":"a","metric":"error_rate"}`|       0.0      |       5.0      |        4.0     |
|`{"host":"b","metric":"error_rate"}`|       2.7      |       10.3     |        0.0     |

##### Join Expression
The join expression can do things like calculate based on transformations. By default, the output value will be of the same type as the two input values. For instance `int64 / int64` would yield an `int64` value. In the case of mixed types, `float64` will be chosen if present, or `int64` if present. It is possible to force an output type by wrapping the expression like `float(int64 / int64)`.

The join expression can also pull in values from another matrix. In that case the series that gets joined will be based on the `keys` passed in. For example:

```javascript
var e = select(database:"foo",where:{'metric'="errors"})
	      .range(start:-24h).window(period:10m).sum()
var r = select(database:"foo",where:{'metric'="requests"})
          .range(start:-24h).window(period:10m).sum()

e.join(group:["host"],exp:{
	('metric'."errors".float()/r.'metric'."requests").float(fixed:2) AS "error_rate"})
```

In the above example we see `r.'metric'."requests"`. When join goes through and joins series in `e` it will match against the series in `r` that have matching `host` tags.

#### Union
`union` will append all the series in one matrix to another.

```javascript
// Accepts: matrix
// Arguments:
//   m - required matrix to union
// Returns: matrix
// for example, merge events stream from two databases
select(database:"foo",where:{'metric' = "events"})
  .range(start:-1h)
  .union(m:select(database:"bar",where:{'metric' = "events"})
    .range(start:-1h))
```

#### Filter
`filter` will filter points out of a matrix based on an expression. It can be used to filter points from one series based on evaluations from another series.

```javascript
// Accepts: matrix
// Arguments:
//   exp - a required expression string
//   group - a string array of keys to group series by
// Returns: matrix
select(database:"foo",where:{'metric'="cpu_user"})
  .range(start:-1h)
  .filter(exp:{$ < 50}) // filter out any values less than 50

// or to filter series in a matrix based on others
select(database:"foo",where:{
    'metric'="response_time" OR 'metric'="log_lines"})
  .filter(group: ["host"], exp:{'metric'."response_time" > 300})
```

#### Sort
`sort` will sort the series in a matrix in ascending order by default. They can either be sorted by their tags or by some expression.

```javascript
// Accepts: matrix
// Arguments:
//   exp - an expression to specify what to sort by. Should return a value that can be sorted
//   keys - a string array of tag keys to specify the sort order, required if exp not given
//   order - string of either asc or desc, asc is default
// Returns: the original matrix, modified in place with the sort order
select(database:"foo",where:{'metric'="cpu_system"})
  .range(start:-1h)
  .sort(exp:{mean($)})
  .limit(n:10)
```

#### Rate
`rate` calculates the average rate of increase between each point for each series in a matrix.

```javascript
// Accepts: a matrix
// Arguments:
//   precision - a duration for what the rate should be. per second, per minute, etc.
// Returns: a matrix
selelct(database:"foo",where:{'metric' = "network_in" AND 'host' = "A")
  .range(start:-1h).rate(precision:1s)
```

#### Aggregate Functions
Aggregate functions will process each series in a matrix. It will be applied to each window in a series, or the entire series if no windows exist. They combine points in the window into a single value. The resulting timestamp of the aggregate will be the same as the time of the first value by default. This can be changed to use the last value's timestamp or the `interpolate` function can be used to round the resulting timestamps to a bucketed duration.

##### Count

##### Mean

##### Percentile

##### Stddev

#### Selector Functions
Selector functions will process each series in a matrix. It will be applied to each window in a series, or the entire series if no windows exist. Selector functions differ from aggregates in that they don't combine data points, but select specific ones from a window or series. The default behavior for selector will leave the timestamps unchanged for the points that have been selected. `interpolate` can be used to round those timestamps to a set interval.

##### Min

##### Max

##### Top

#### Transformation Functions
Transformation functions will process each series in a matrix.

##### Difference

### Examples

Here are a bunch of query examples for different scenarios. The imaginary data isn't necessarily consistent from query to query. The examples are here to illustrate what the language looks like for some real world query scenarios depending on what type of schema is set up.

```javascript
// get all cpu measurements for a specific host for the last hour
select(database:"foo", where:{'host'="a" AND 'system'="cpu"})
  .range(start:-1h)

// get the 90th percentile, max, and mean of all cpu measurements
// for a specific host in 5m windows over last 4h
var cpu = select(database:"foo", where:{'host'="A" AND 'system'="cpu"})
            .range(start:-4h)
            .window(every:5m)

cpu.percentile(n:90), cpu.max(), cpu.mean()

// get the last value recorded for all sensors in a specific building
select(database:"foo",where:{'building' = "23" AND 'type' = "sensor"})
  .last()

// what keys are in the db
select(database:"foo").keys()
// ["tag1","tag2","tag3", ...]

// what keys and how many values does each have in the db (last 24h). This query
// would be a very likely starting point for an exploration UI
select(database:"foo")
  .cardinality()
// {"tag1":23, "tag2":76, ...}

// what keys and how many values does each have for the sensor types in the west
select(database:"foo",where:{"type" = 'sensor' AND "region" = 'west'})
  .cardinality()
// {"tag1":23, "tag2":76, ...}

// get the number of hosts that have reported data in the west data center in the last 24h
select(database:"foo",where:{"region" = 'west'})
  .cardinality(keys:["host"])
// {"host":234}

// get unique hosts in west region, but limit so you can paginate through
select(database:"foo",where:{"region" = 'west'})
  .values(key:"host")
  .limit(n:50,offset:100)

// get the metrics that each host has reported data for in the last 24h
// source data:
/*
[
  {
    "series": {"host":"A","metric":"cpu"}
    "values": []
  },
  {"host":"B","metric":"cpu"},
  {"host":"A","metric":"mem"},
  {"host":"B","metric":"mem"}
]
*/
select(database:"foo", where: {'metric' NOT EMPTY})
  .group(keys:["host"])
  .merge(keys:["metric"], op:"append")
  .rename(from:"metric",to:"metrics")
// returns matrix with empty values and series objects like:
/*
[
  {
    "series": {"host":"A","metrics":["cpu","mem"]},
    "values": []
  },
  {
  	"series": {"host":"B","metrics":["cpu","mem"]},
  	"values": []
  }
]
*/

// get the number of hosts in each data center that have reported
// data in the last 24h
select(database:"foo")
  .group(keys:["data_center"])
  .merge(keys:["host"],op:"count")
  .rename(from:"host",to:"hostCount")
// returns matrix with empty values and series objects like:
// {"data_center":"west","hostCount":23}

// get the cpu usage_user of hosts that have mysql running
select(database:"foo",where:{
      'system' = "cpu" AND 'metric' = "usage_user" AND 'host' IN
        select(database:"foo",where:{'service'="mysql"}).values(key:"host")
    })
  .range(start:-4h)

// get the count in 10m periods in the last 24h from an event stream and
// filter that to only include those periods that were 2 sigma above the average
var m = select(database:"foo",where:{'event' = "pageview"})
		  .range(start:-24h)
		  .merge()
		  .window(every:10m)
		  .count()

// this is shorthand for m.stddev.join(exp:{$ * 2})
var sigma = m.stddev() * 2

// return only the counts 2 sigma above
// shorthand for m.filter(exp:{$ > sigma})
m > sigma
```
