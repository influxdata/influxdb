# InfluxQL & Kapacitor 2.0

This document puts forth some ideas for the new data model of InfluxDB and a query language that unites Influx and Kapacitor into one.
The goal is to have a query language that is easy to use and fits with the model of working with time series data.
It should be extensible and easy for users to add new functions and build on top of some key primitives.

The rollout of this should be as an additional endpoint in InfluxDB and Kapacitor that can work with the existing data model or work with the new data model.

## Proposed Data Model & Terms

The new data model's goal is to simplify things for the users, while keeping the ability to represent other data models like InfluxDB 1.x, OpenTSDB, and Prometheus.
The primary goal is to remove questions from the user about how to organize things and also let the data structures improve the clarity of the query language.
The new model consists of databases, which have a retention period (how long they keep data around) and time series are identified by tag key/value pairs (note that measurements and fields are no longer a concept that exists).
A series is a time ordered collection of time/value pairs.
Values in a series must all be of the same type, either bool, int64, uint64, float64, or string.
Queries can span multiple databases and can write into multiple databases so removing the separate retention policy heirarchy won't limit functionality.

### Definitions

The following definitions encompass the entire vocabulary of the data model.
Where appropriate, JSON examples are given, but the protocol will also support a line or protobuf based structure.

* **tag** - a key/value pair of strings. (e.g. "region":"host" or "building":"2").
    Tags MUST NOT start with `#`.
    Tag keys starting with `_` have special meaning and should not be used by the user unless they conform to the predefined set of keys that use `_`.
* **system tag** - A tag with a key that starts with `_` followed by letters and/or numbers.
    These are indexed just like tags, but are used by the system for compatibility layers with other systems.
* **meta** - A JSON style object associated with a series.
    This data is not indexed and can only be included/joined in a query.
    Its purpose is to add more context to series for user interfaces and additional calculations.
* **tagset** - an set of one or more tag key/value string pairs.
    Example:
    ```json
    {
        "region":"host",
        "building":"2"
    }
    ```
* **value** - a single value of type null, bool, int64, uint64, float64 (must support NaN), time, or string
* **epoch** - distance in time from UTC 00:00:00 01/01/1970. 
    Step size for distance is determined by precision
* **precision** - the precision at which an epoch should be interpreted.
    Acceptable values:
      * s - seconds
      * ms - milliseconds
      * us - microseconds
      * ns - nanoseconds
* **time** - an RFC3339 or RFC3339Nano time string
* **point** - an object with value, epoch, time, and tagset.
    Value is required.
    One and only one of either epoch or time MUST be present.
    Tagset is optional.
    Example:
    ```json
    {
        "value": 23.1,
        "epoch": 1491499253,
        "tagset": {"host":"A"}
    }
    ```
* **vector** - a time/epoch ordered collection of points
* **series** - an object with a tagset, metadata, and an array of points.
    All values in the vector must be of the same data type.
    Points in the vector will have tagsets that are the union of the series tagset and their own, if it is specified.
    Keys in the point tagset will override any keys from the series tagset. (e.g. a point in series {"host":"A","region":"west"} with a point tagset of {"host":"B"} will have the resulting tagset of {"host":"B","region":"west"}).
    Note that series could map to raw underlying database series or could be a combination of series or composed series from transformations.
    Example:
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
* **table** - a collection of series.
    Columns are time, value and any tags keys on the series.
    Example:
    ```json
    {
        "columns": [
            "time",
            "host",
            "value"
        ],
        "points" :[
            [ 1491499253, "A", 23.1],
            [ 1491499253, "B", 23.1],
            [ 1491499263, "A", 56.2],
            [ 1491499263, "B", 56.2]
        ]
    }
    ```
    Tables can be sorted by their tags or time columns.
    Tables can represent unbounded data and so may be logically infinite.
* **block** - a subset of a table.
    Blocks are used break up tables into logically smaller bounded data sets.
    Block define bounds around which transformations should operate.
* **database** - an object with a name and a retention [duration string](https://golang.org/pkg/time/#ParseDuration).
    The duration determines how long data is kept in the database.
    It is a container of raw series.
    Names must be a string and can contain `_`, `-`, `0-9`, `a-z`, `A-Z`.
    Database names MUST not start with `_`.
    **Note** Retention policies are no longer a part of the model.
    Example:
    ```json
    {
        "name": "foo",
        "retention": "0"
    }
    ```

### Proposed Write Protocol

The primary write protocol is a line protocol.
The line protocol contains some shortcuts to avoid the duplication of shared information like tags and time, but ultimately those will be converted into points, which are written into series (meaning that points in a series MUST have a unique time or they will overwrite previously written points).

The protocol can also include comments, which are lines that start with `#` followed by a space.
Lines that start with `##` specify a block of meta data for the line immediately following.

The most basic style of the protocol has the tag key/value pairs separated by `:` with a trailing space, a value, and a trailing epoch or time.
The structure looks like this:

```
<tagset> <value> <time>
```

All three sections are required.
Here are the rules for each section.

#### Tagset

The tagset is the key/value pairs that identify the series.
A line MUST have at least one tag key/value pair.
They should be separated by `:`.
Spaces, commas, colons, and backslashes in either a key or value MUST be escaped by a backslash.

#### Value

The value block can be any of the value types: boolean, float64, int64, uint64, time, string, or bytes.
Null values should never be specified as they aren't stored in the database.
Boolean is represented with `t` or `f`.
Int64, UInt64 must have a trailing `i` or `u`, while all numbers without the trailing character are parsed as float64.
Times must be in RFC3339Nano format.
Strings should be wrapped in double quotes, which means any double quote within a string must be escaped with a backslash.
Bytes look like an array of numbers: `[102, 111, 111]`.

#### Time

The time block can be specified as either an epoch or an RFC3339Nano string.
If an epoch is used it should be followed by its precision.
However, if no precision is specified, the system will guess the precision by whatever is closest to the current time on the server receiving the write.

#### Meta data

*TODO:* give this more definition.

Meta data can be written with a line starting with `##`.
The meta data will be associated with the series on the next line.
Meta data is a JSON object.
Keys in the object starting with `_` are reserved for the system.
The data in meta is not indexed.
The only validation is that it is a parseable JSON object.

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

Writes can come in through gRPC.
Here's the `proto` definition for the schema and service. (TODO)

### Representing the InfluxDB 1.x Data Model

The InfluxDB 1.x data model can be represented by using special `_measurement` and `_field` tag keys to split measurement and fields up.
Here's an example:

```text
# InfluxDB 1.x
cpu,host=A,region=west user=16.4,system=23.2 1491675816

# Influx 2.0 example
_measurement:cpu,host:serverA,region:west,_field:user 16.4 1491675816
_measurement:cpu,host:serverA,region:west,_field:system 23.2 1491675816
```

### Representing the Prometheus/OpenTSDB Data Model

The Prometheus data model can be represented by using the special `_metric` tag key.
Here's an example:

```text
# Prometheus
node_cpu{cpu="cpu0",mode="user"} 182168.46875

# Influx 2.0
_metric:node_cpu,cpu:cpu0,mode:user 182168.46875 1491675816
```

### Pre-defined tag keys

There are a set of tag keys used by the system that start with `_` that are well known.
Some of them are for conversions from Influx 1.x or Prometheus while others are for information useful in graphing.

* **_units** - the units of the measurement - 
* **_type** - the type of the series - [int64, uint64, float64, bool, string]
* **_samplingRate** - duration string for how frequently this series is expected to be sampled. `0` indicates that it is an event stream (irregular series)
* **_metric** - for Prometheus mapping, the metric name
* **_measurement** - for InfluxDB 1.x mapping, the measurement name
* **_field** - for InfluxDB 1.x mapping, the field name
* **_description** - a human readable description of the series. Like Prometheus' HELP.

## Proposed Query Language

The new query language is functional in style, but I would call it a streaming query language because the engine should process parts of function inputs as they come in rather than getting the entire input at once.
From the user's perspective it should look purely functional, but the engine should optimize queries to be streamed.
Functions take named arguments with the argument name and value separated by `:` and arguments separated by `,`.
String arguments are wrapped in double quotes.
When using double quotes, any double quotes in the string must be escaped by backslash.
Function and argument names should use camel casing like in Javascript.

A **query** is made up of a number of **statements**, which can include variable assignments.
The result set sent back are any terminal steps in the query(i.e. leaf nodes).
Queries can also include comments, which are started with `//` and continue until a newline.

Without using real function or argument names, here's an example of what the query language looks like:

```javascript
// as one line
funcA(argOne: "some thing", argTwo: ['foo', 'bar']).funcB(arg: /some regex/).funcC(arg: "here \"is a\" string")
```

```javascript
// or separating for readability with newlines
funcA(argOne: "some thing", argTwo: ['foo', 'bar'])
    .funcB(arg: /some regex/)
    .funcC(arg: "here \"is a\" stringr)
```

```javascript
// or even more newline separation
funcA(
    argOne: "some thing",
    argTwo: ['foo', 'bar'])
.funcB(
    arg: /some regex/)
.funcC(
    arg: "here \"is a\" string")
```

```javascript
// variable assignment and multiple returns
var foo = funcA()

foo.transformOne()
foo.transformTwo()
```

The query language doesn't aim to be the most terse representation possible.
Rather, it is meant to be readable and, more importantly, extensible.
New functions should be easy to define and add to the language.
Existing functions should be able to get new optional arguments without breaking existing clients.

This documentation lays out the functions in the query language.
For each function we define the following:

* **accepts** what data types (or function returns) a function can be chained off.
    For example `range` accepts the result of a select: `select(...).range(...)`
* **arguments** named arguments for a function.
    Required arguments will be marked and the first listed

The passed in value for a function argument can be either the direct type (like a string), a variable holding the proper type, or another function in the language that returns the correct type.
For example, a function argument that takes an array of strings could take `f(arg:['foo','bar'])` or it could take `f(arg:someFuncThatReturnsStringArray())`.

### The Data Model

We can represent data in the database as a table that can be manipulated.
It's helpful to think of the data this way when working with the query language since it is a series of functions performing transformations on the table of time series.
You can also think of them as data frames.
For example, here is a basic table of series data:


| Time | host | metric   | Value |
| ---- | ---- | ------   | ----- |
| 10   | a    | cpu_user | 23.1  |
| 10   | b    | cpu_user | 76.1  |
| 20   | a    | cpu_user | 25.3  |
| 20   | b    | cpu_user | 50.1  |
| 30   | a    | cpu_user | 28.9  |
| 30   | b    | cpu_user | 56.3  |
| 40   | a    | cpu_user | 35.2  |
| 40   | b    | cpu_user | 65.0  |


An entire database is really just a very large table like the one shown above.
In the table, each row represents a point.
The columns identify the time, tags, and value.
As JSON, this table would look like this:

```json
{
    "table":{
        "columns": [
            "time",
            "host",
            "metric",
            "value"
        ],
        "points": [
            [ 10, "a", "cpu_user", 23.1],
            [ 10, "b", "cpu_user", 76.1],
            [ 20, "a", "cpu_user", 25.3],
            [ 20, "b", "cpu_user", 50.1],
            [ 30, "a", "cpu_user", 28.9],
            [ 30, "b", "cpu_user", 56.3],
            [ 40, "a", "cpu_user", 35.2],
            [ 40, "b", "cpu_user", 65.0]
        ]
    }
}
```

#### Table Blocks

A table represents all data for all time and all series that were selected.
Since this data is potentially unbounded the table may have to be infinitely long.
In order to deal with unbounded data we introduce the concept of a `block`.
A `block` is a bounded subset records in a table.

A block's bounds can be defined along two general dimensions:

* Time - Windowing separates records by time.
    A simple fixed window of one hour will create 24 blocks of data for a day.
* Cardinality - Grouping separates records based on their tags.
    For example records can be grouped by host to place all records for host=A into one block and records for host=B into another block.

A block is defined by its window time bounds and the tags for the grouping.

By default data queried from the database has each record in its own block.
By applying `window` and `merge` transforms the records can be organized into different blocks.

#### Transformations

Transformations take a table as input and produce another table as output.
Transformations consume records from tables in blocks and place the resulting records into the blocks of the output table.

As an example, let's say we want to perform an aggregate transformation on each series.
Aggregate transformations by definition produce a single record as output for each block in the input table.

Given this table `foo` grouped by `host,metric` and windowed with a single global window:

| Time | host | metric   | Value |
| ---- | ---- | ------   | ----- |
| 10   | a    | cpu_user | 23.1  |
| 20   | a    | cpu_user | 25.3  |
| 30   | a    | cpu_user | 28.9  |
| 40   | a    | cpu_user | 35.2  |
| ---- | ---- | ------   | ----- |
| 10   | b    | cpu_user | 76.1  |
| 20   | b    | cpu_user | 50.1  |
| 30   | b    | cpu_user | 56.3  |
| 40   | b    | cpu_user | 65.0  |

Notice that there are two blocks of records within the table `foo`.

If we called `foo.sum()` we would get the following matrix:

| Time | host | metric   | Value |
| ---- | ---- | ------   | ----- |
| 40   | a    | cpu_user | 112.5 |
| 40   | b    | cpu_user | 247.5 |

We can see that sum was applied for each group individually and produced a single result for each block.

####  Windowing

Windowing data is a transformation in and of itself.
The window transformation consumes input blocks and places the records in the output blocks according to their time.

For example given the same table `foo` with two blocks:

| Time | host | metric   | Value |
| ---- | ---- | ------   | ----- |
| 10   | a    | cpu_user | 23.1  |
| 20   | a    | cpu_user | 25.3  |
| 30   | a    | cpu_user | 28.9  |
| 40   | a    | cpu_user | 35.2  |
| ---- | ---- | ------   | ----- |
| 10   | b    | cpu_user | 76.1  |
| 20   | b    | cpu_user | 50.1  |
| 30   | b    | cpu_user | 56.3  |
| 40   | b    | cpu_user | 65.0  |

The transformation `foo.window(every:20s)` would produce this output table with four blocks.

| Time | host | metric   | Value |
| ---- | ---- | ------   | ----- |
| 10   | a    | cpu_user | 23.1  |
| 20   | a    | cpu_user | 25.3  |
| ---- | ---- | ------   | ----- |
| 30   | a    | cpu_user | 28.9  |
| 40   | a    | cpu_user | 35.2  |
| ---- | ---- | ------   | ----- |
| 10   | b    | cpu_user | 76.1  |
| 20   | b    | cpu_user | 50.1  |
| ---- | ---- | ------   | ----- |
| 30   | b    | cpu_user | 56.3  |
| 40   | b    | cpu_user | 65.0  |

The combined transformation `foo.window(every:20s).sum()` would produce this output table with four blocks each with a single record.

| Time | host | metric   | Value |
| ---- | ---- | ------   | ----- |
| 20   | a    | cpu_user | 48.4  |
| ---- | ---- | ------   | ----- |
| 40   | a    | cpu_user | 64.1  |
| ---- | ---- | ------   | ----- |
| 20   | b    | cpu_user | 126.2 |
| ---- | ---- | ------   | ----- |
| 40   | b    | cpu_user | 121.3 |

**NOTE** Windowing may place an input record into multiple output blocks.
    Overlapping windows is a simple example of this case.
    More an that below.

#### Merging

Merging is a transformation that groups records into blocks by their tags.

For example given this table `bar` which represents raw data from a select (since by default each record is in its own block):

| Time | host | metric   | Value |
| ---- | ---- | ------   | ----- |
| 10   | a    | cpu_user | 23.1  |
| ---- | ---- | ------   | ----- |
| 20   | a    | cpu_user | 25.3  |
| ---- | ---- | ------   | ----- |
| 30   | a    | cpu_user | 28.9  |
| ---- | ---- | ------   | ----- |
| 40   | a    | cpu_user | 35.2  |
| ---- | ---- | ------   | ----- |
| 10   | b    | cpu_user | 76.1  |
| ---- | ---- | ------   | ----- |
| 20   | b    | cpu_user | 50.1  |
| ---- | ---- | ------   | ----- |
| 30   | b    | cpu_user | 56.3  |
| ---- | ---- | ------   | ----- |
| 40   | b    | cpu_user | 65.0  |
| ---- | ---- | ------   | ----- |

The transformation `bar.merge(keys:["host"])` produces this table with two blocks:

| Time | host | metric   | Value |
| ---- | ---- | ------   | ----- |
| 10   | a    | cpu_user | 23.1  |
| 20   | a    | cpu_user | 25.3  |
| 30   | a    | cpu_user | 28.9  |
| 40   | a    | cpu_user | 35.2  |
| ---- | ---- | ------   | ----- |
| 10   | b    | cpu_user | 76.1  |
| 20   | b    | cpu_user | 50.1  |
| 30   | b    | cpu_user | 56.3  |
| 40   | b    | cpu_user | 65.0  |

The individual records have been merged into larger blocks.

**NOTE** Merge may also split up blocks into smaller blocks.
(TODO) Should we choose a different name than merge since it is a bit of a misnomer when larger blocks are being split down into smaller blocks?

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
* `==` equal to
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

`select` will return a table of data from a specified database.
Remember that a table can represent unbounded data, so the select function logically returns all the data for the database.
In practice, only the data that is actually needed is returned.
See `range`, `where` and `limit` functions which will restrict the actual data needed to be read from the database.
You can also order the records using `sort`.

**NOTE:** A major difference between 2.0 and 1.x is that by default, series will not be merged together.
That is, all `select` statements will yield a table with each individual record as its own block.
This is what `group by *` would do in InfluxQL 1.x. With InfluxQL 2.0 it is now the default behavior.

```javascript
// Accepts: start
// Arguments:
//   database - a string, required if not chained off a matrix
// Returns: a table for the data within the database.
select(database:"foo") // get all series for database foo
```

#### Where

`where` will filter the records of table down to only records that match the given expression.
The where argument is wrapped in curley braces `{` `}`. Tag keys must be wrapped in single quotes and tag values must be wrapped in double quotes. Here are some examples:

```javascript
// Accepts: table
// Arguments:
//   exp - an expression, only records that match the expression are output.
// Returns: a table of the filtered records.
select(database:"foo").where(exp:{"metric" == "cpu"} // get all series for database foo with the tag "metric" equal to "cpu".
```

Other examples:

```javascript
select(database:"foo").where(exp:{"metric"=="cpu"})
select(database:"foo").where(exp:{"metric"=="cpu" or "metric"=="mem"})
select(database:"foo").where(exp:{("metric"=="cpu" or "metric"=="mem") and "host"=="serverA"})
select(database:"foo").where(exp:{"host" in ["a","b"]})
select(database:"foo").where(exp:{"host" not empty})

// variables can be referenced in the where expression
var hostName = "serverA"
select(database:"foo").where(exp:{"host" == hostName})
```

#### Range
`range` will restrict records by the time range specified.

```javascript
// Accepts: table
// Arguments:
//   start - a time. Could be relative e.g. `-4h` or an epoch, or a time string
//   end - a time
// Returns: a matrix
select(database:"foo").range(start:-4h)
```

#### Clear
`clear` will remove series from a table that have no points so they are not returned in the response.
(TODO) This should be the default behavior.

```javascript
select(database:"foo")
  .where(exp:{"metric"=="cpu"})
  .range(start:-1h)
  .clear()
```

#### Casting types

The value columns of a table are typed and can be cast using the type casting functions.
When converting from string to int64, float64 or others they will be parsed.
Errors can either be ignored or returned.

##### Float
`float` will convert a single value or all values in a table to `float64` type values.

```javascript
// Accepts: table, int64, uint64, bool, string
// Arguments:
//   error - boolean indicating if it should fail on casting error. default is false
// Returns: table of float64 values or other value converted to a float64
```

##### Int
`int` will convert a single value or all values in a table to `int64` type values.

```javascript
// Accepts: table, float64, uint64, bool, string
// Arguments:
//   error - boolean indicating if it should fail on casting error. default is false
// Returns: table of int64 values or other value converted to a int64
```

##### UInt
`uint` will convert a single value or all values in a table to `uint64` type values.

```javascript
// Accepts: table, float64, int64, bool, string
// Arguments:
//   error - boolean indicating if it should fail on casting error. default is false
// Returns: table of uint64 values or other value converted to a uint64
```

##### String
`string` will convert a single value or all values in a table to `string` type values.

```javascript
// Accepts: table, float64, int64, uint64, bool
// Arguments:
//   error - boolean indicating if it should fail on casting error. default is false
// Returns: table of string values or other value converted to a string
```

##### Bool
`bool` will convert a single value or all values in a table to `bool` type values.

```javascript
// Accepts: table, float64, int64, uint64, string
// Arguments:
//   error - boolean indicating if it should fail on casting error. default is false
// Returns: table of bool values or other value converted to a bool
```

#### Window

`window` will group records in a table based on the windowing criteria.
These windows can then have functions applied to them to produce aggregates or summaries of each.

```javascript
// Accepts: table
// Arguments:
//   every - start a window after this duration. Must be less than or equal to period.
//   period - windows are for this duration. If only every or period is specified,
//            the other is set to the same value by default. If the every duration
//            is less than the period, windows will overlap.
//   start - optional, time to start the windows. If not specified, the time to start
//           will be the time of the first value in each series
//   round - optional, duration to round the start time to
// Returns: table
```

For example, we start with this table `foo` where each record is in its own block:

| Time | host | metric   | Value |
| ---- | ---- | ------   | ----- |
| 10   | a    | cpu_user | 12    |
| ---- | ---- | ------   | ----- |
| 10   | b    | cpu_user | 6     |
| ---- | ---- | ------   | ----- |
| 20   | a    | cpu_user | 13    |
| ---- | ---- | ------   | ----- |
| 20   | b    | cpu_user | 5     |
| ---- | ---- | ------   | ----- |
| 30   | a    | cpu_user | 18    |
| ---- | ---- | ------   | ----- |
| 30   | b    | cpu_user | 3     |
| ---- | ---- | ------   | ----- |
| 40   | a    | cpu_user | 14    |
| ---- | ---- | ------   | ----- |
| 40   | b    | cpu_user | 2     |
| ---- | ---- | ------   | ----- |


And we call `foo.window(every:20s)` assign the records into new blocks based on their time:

| Time | host | metric   | Value |
| ---- | ---- | ------   | ----- |
| 10   | a    | cpu_user | 12    |
| 20   | a    | cpu_user | 13    |
| ---- | ---- | ------   | ----- |
| 10   | b    | cpu_user | 6     |
| 20   | b    | cpu_user | 5     |
| ---- | ---- | ------   | ----- |
| 30   | a    | cpu_user | 18    |
| 40   | a    | cpu_user | 14    |
| ---- | ---- | ------   | ----- |
| 30   | b    | cpu_user | 3     |
| 40   | b    | cpu_user | 2     |

We see that there are now four blocks of data, one for records of time `[10,30)`, and another of time `[30,50)` for each of the two unique tagsets.
**NOTE** A square bracket `[` mean inclusive and a parentheses `)` means exclusive.

Now if we call `foo.window(every:20s).sum()` we would have received the following result:

| Time | host | metric   | Value |
| ---- | ---- | ------   | ----- |
| 20   | a    | cpu_user | 25    |
| ---- | ---- | ------   | ----- |
| 20   | b    | cpu_user | 11    |
| ---- | ---- | ------   | ----- |
| 40   | a    | cpu_user | 32    |
| ---- | ---- | ------   | ----- |
| 40   | b    | cpu_user | 5     |

Now what if we wanted to sum the values across the larger time windows?

We can now define a new window `foo.window(every:20s).sum().window(every:40s).sum()`

The intermediate step would first change the blocks to be:

| Time | host | metric   | Value |
| ---- | ---- | ------   | ----- |
| 20   | a    | cpu_user | 25    |
| 40   | a    | cpu_user | 32    |
| ---- | ---- | ------   | ----- |
| 20   | b    | cpu_user | 11    |
| 40   | b    | cpu_user | 5     |

The final result would be:

| Time | host | metric   | Value |
| ---- | ---- | ------   | ----- |
| 40   | a    | cpu_user | 57    |
| ---- | ---- | ------   | ----- |
| 40   | b    | cpu_user | 16    |


##### Overlapping Windows

The windows can also be overlapping. For example, if we specified a period of `20s` but created a window every `10s` we would get overlapping windows. So if we call `foo.window(period:20s, every:10s)` we would get:

Given this table `foo` we can create overlapping windows:

| Time | host | metric   | Value |
| ---- | ---- | ------   | ----- |
| 10   | a    | cpu_user | 12    |
| ---- | ---- | ------   | ----- |
| 10   | b    | cpu_user | 6     |
| ---- | ---- | ------   | ----- |
| 20   | a    | cpu_user | 13    |
| ---- | ---- | ------   | ----- |
| 20   | b    | cpu_user | 5     |
| ---- | ---- | ------   | ----- |
| 30   | a    | cpu_user | 18    |
| ---- | ---- | ------   | ----- |
| 30   | b    | cpu_user | 3     |
| ---- | ---- | ------   | ----- |
| 40   | a    | cpu_user | 14    |
| ---- | ---- | ------   | ----- |
| 40   | b    | cpu_user | 2     |
| ---- | ---- | ------   | ----- |

Using `foo.window(every:10s, period:20s)` we will create the following blocks:

| Time | host | metric   | Value |
| ---- | ---- | ------   | ----- |
| 10   | a    | cpu_user | 12    |
| ---- | ---- | ------   | ----- |
| 10   | a    | cpu_user | 12    |
| 20   | a    | cpu_user | 13    |
| ---- | ---- | ------   | ----- |
| 20   | a    | cpu_user | 13    |
| 30   | a    | cpu_user | 18    |
| ---- | ---- | ------   | ----- |
| 30   | a    | cpu_user | 18    |
| 40   | a    | cpu_user | 14    |
| ---- | ---- | ------   | ----- |
| 10   | b    | cpu_user | 6     |
| ---- | ---- | ------   | ----- |
| 10   | b    | cpu_user | 6     |
| 20   | b    | cpu_user | 5     |
| ---- | ---- | ------   | ----- |
| 20   | b    | cpu_user | 5     |
| 30   | b    | cpu_user | 3     |
| ---- | ---- | ------   | ----- |
| 30   | b    | cpu_user | 3     |
| 40   | b    | cpu_user | 2     |
| ---- | ---- | ------   | ----- |

Notice how there are now a total of 14 records in this table instead of just 8?
This is because a single input record has been placed into multiple output blocks.
This is exactly what we want since we requested overlapping windows, i.e. we want to process the data multiple times.


And if we took the sum like: `foo.window(every:10s, period:20s).sum()` we would get:

| Time | host | metric   | Value |
| ---- | ---- | ------   | ----- |
| 10   | a    | cpu_user | 12    |
| ---- | ---- | ------   | ----- |
| 20   | a    | cpu_user | 25    |
| ---- | ---- | ------   | ----- |
| 30   | a    | cpu_user | 31    |
| ---- | ---- | ------   | ----- |
| 40   | a    | cpu_user | 32    |
| ---- | ---- | ------   | ----- |
| 10   | b    | cpu_user | 6     |
| ---- | ---- | ------   | ----- |
| 20   | b    | cpu_user | 11    |
| ---- | ---- | ------   | ----- |
| 30   | b    | cpu_user | 8     |
| ---- | ---- | ------   | ----- |
| 40   | b    | cpu_user | 5     |
| ---- | ---- | ------   | ----- |

##### Starting all windows at the same time

If we specify a `start` argument, this will be the time that all series are started from.
This makes it easy to ensure that all blocks in a table will have buckets on the same boundaries.
For example, if we have:

```javascript
// example using windows from a given start time
select(database:"foo")
  .where(exp:{"metric"=="cpu"})
  .range(start:-1h)
  .window(period:10m,start:-1h)
// if executed at 13:03:59, windows would occur at 12:03:59, 12:13:59, 12:23:59, etc.

select(database:"foo")
  .where(exp:{"metric"=="cpu"})
  .range(start:-1h)
  .window(period:10m,start:-1h,round:10m)
// if executed at 13:03:59, windows would occur at 12:00:00, 12:10:0, 12:20:0, etc.
```

#### Merge
`merge` will merge blocks in a table together based on the grouping criteria.
Note that all values in a series must be of the same type.
If a type difference is encountered, merge will error.
Use the type casting function to ensure values are of the same type before merging if necessary.

```javascript
// Accepts: table
// Arguments:
//   keys: an array of strings. This acts like a group by clause in SQL
//   keep: an array of strings. These are tag key data that should be kept in the individual points. This acts like a projection in SQL.
// Returns: matrix
select(database:"foo")
  .where(exp:{"metric"=="network_in"})
  .merge(keys:["region"])
```


Given this new table `baz`:

| Time | host | metric  | dc   | Value |
| ---- | ---- | ------  | --   | ----- |
| 10   | a    | network | west | 1     |
| ---- | ---- | ------  | --   | ----- |
| 20   | a    | network | west | 1     |
| ---- | ---- | ------  | --   | ----- |
| 10   | b    | network | east | 2     |
| ---- | ---- | ------  | --   | ----- |
| 20   | b    | network | east | 2     |
| ---- | ---- | ------  | --   | ----- |
| 10   | c    | network | east | 3     |
| ---- | ---- | ------  | --   | ----- |
| 20   | c    | network | east | 3     |
| ---- | ---- | ------  | --   | ----- |
| 10   | d    | network | west | 4     |
| ---- | ---- | ------  | --   | ----- |
| 20   | d    | network | east | 4     |
| ---- | ---- | ------  | --   | ----- |



If we called `baz.merge(keys:["metric"])` we would get a table that looks something like this:

| Time | host | metric  | dc   | Value |
| ---- | ---- | ------  | --   | ----- |
| 10   | a    | network | west | 1     |
| 20   | a    | network | west | 1     |
| 10   | b    | network | east | 2     |
| 20   | b    | network | east | 2     |
| 10   | c    | network | east | 3     |
| 20   | c    | network | east | 3     |
| 10   | d    | network | west | 4     |
| 20   | d    | network | east | 4     |

We can merge based on multiple tag keys.
For example if we called `baz.merge(keys:["metric", "dc"])` we would get a table that looks like this:

| Time | host | metric  | dc   | Value |
| ---- | ---- | ------  | --   | ----- |
| 10   | b    | network | east | 2     |
| 20   | b    | network | east | 2     |
| 10   | c    | network | east | 3     |
| 20   | c    | network | east | 3     |
| ---- | ---- | ------  | --   | ----- |
| 10   | a    | network | west | 1     |
| 20   | a    | network | west | 1     |
| 10   | d    | network | west | 4     |
| 20   | d    | network | east | 4     |

#### Keys
`keys` will return an array of tag keys that occur in the table.
The output of keys function can be used as the predecessor argument to `empty` or `not empty` in a `where` expression.
For pagination through keys, `limit` can be chained off it.

```javascript
// Accepts: table
// Arguments: none
// Returns: []string
select(database:"foo").keys()
```

#### Values
`values` returns a string array of values in sorted order for a given key and the given criteria.
For pagination through values, `limit` can be chained off of it.

```javascript
// Accepts: table
// Arguments:
//   key - required argument, a string of what tag key to return values for
// Returns: []string
select(database:"foo").values(key:"metric")
```

#### Cardinality
`cardinality` returns a table of the count of unique tag values for each key in the table.

```javascript
// Accepts: table
// Arguments:
//   keys - optional string array. only return the cardinality for the passed keys
// Returns: table
select(database:"foo")
  .where(exp:{'region'="west"})
  .cardinality()
```

#### Limit
`limit` accepts either a table or an array and will limit the number of results.
In the case of a table, it limits the number records that are returned.
In the case of an array it is used to take a slice.
It's a good practice to always apply a limit of some kind.

```javascript
// Accepts: table, array
// Arguments:
//   n - the number of results to limit to, required argument.
//   offset - the offset (inclusive) to start with. defaults to 0.
// Returns: a table if passed a table, an array if passed an array
//
select(database:"foo").limit(n:10,offset:20) // get the third page of results
```

#### Time Shift
`timeShift` will shift all returned timestamps in the table by the given duration.

```javascript
// Accepts: table
// Arguments:
//   duration - a duration
// Returns: table
select(database:"foo")
  .range(start:"2017-01-01")
  .timeShift(duration:-168h) // shift everything to a week before
```

#### Interpolate
`interpolate` will normalize the time series in a table based on different criteria. It could work across columns or on the series themselves.

```javascript
// Accepts: table
// Arguments:
//   start - time, ensure data exists starting at this time
//   every - duration, ensure that a value exists at every duration period from start
//   value - integer, float, string, bool, or expression to fill with. Expressions can
//           use functions calculated from the series like mean($)
// Returns: table
select(database:"foo")
  .where(exp:{"metric"=="requests"})
  .range(start:-4h)
  .clear()
  .window(period:10m)
  .sum()
  .interpolate(start:-4h,every:10m,value:{mean($)})
```

#### Join
`join` operates on multiple tables and performs relational joins.
The `interpolate` function can be used to ensure that timestamps for the series line up and can be matched together.

```javascript
// Accepts: table
// Arguments:
//   keys - the tag keys to group the join by
//   predicate - an expression that must evaluate to true for the resutling record to be added to the joined output. Only one of keys and predicate may be specified.
//   exp - the expression to join series on. The variables found in the expression are the tables that will be joined.
// Returns: table
var requests = select(database:"foo")
  .where(exp:{"metric"=="requests")
  .range(start:-30m)

select(database:"foo")
  .where(exp:{"metric"=="errors"})
  .range(start:-30m)
  // Join errors and requests on the "host" tag.
  .join(
    keys:["host"],
    exp:{$/requests},
  )
```

Given these two tables for the `requests` and `errors`:

Requests:

| Time | host | metric   | Value |
| ---- | ---- | ------   | ----- |
| 30   | a    | requests | 50    |
| 30   | b    | requests | 50    |
| 40   | a    | requests | 60    |
| 40   | b    | requests | 29    |
| 50   | a    | requests | 50    |
| 50   | b    | requests | 0     |

Errors:

| Time | host | metric | Value |
| ---- | ---- | ------ | ----- |
| 30   | a    | errors | 0     |
| 30   | b    | errors | 1     |
| 40   | a    | errors | 3     |
| 40   | b    | errors | 0     |
| 50   | a    | errors | 2     |
| 50   | b    | errors | 0     |

The join transformation creates the following intermediate table representation by joining records that have the same host and time attributes.

| Time | host | $.metric | requests.metric | $.Value | requests.Value |
| ---- | ---- | ------   | ------          | -----   | -----          |
| 30   | a    | errors   | requests        | 0       | 50             |
| 30   | b    | errors   | requests        | 1       | 50             |
| 40   | a    | errors   | requests        | 3       | 60             |
| 40   | b    | errors   | requests        | 0       | 29             |
| 50   | a    | errors   | requests        | 2       | 50             |
| 50   | b    | errors   | requests        | 0       | 0              |

Then the join expression is applied and non matching attributes are dropped.

| Time | host | Value |
| ---- | ---- | ----- |
| 30   | a    | 0/50  |
| 30   | b    | 1/50  |
| 40   | a    | 3/60  |
| 40   | b    | 0/29  |
| 50   | a    | 2/50  |
| 50   | b    | 0/0   |

##### Many-to-One

Joins are pure relational joins and so can be used to create a many-to-one relationship between the tables.

For example given this query:

```
// Query the number of request per http method.
var requests = select(database:"foo")
  .where(exp:{"metric"=="http_requests")
  .range(start:-30m)

// Query the number of requests that had an error status code per http method and code.
var errors = select(database:"foo")
  .where(exp:{"metric"=="http_errors"})
  .range(start:-30m)


errors.join(
  keys:["method"],
  exp:{errors/requests},
)
```

The above query will compute the error rate for each HTTP method and status code.

Given the input tables

Requests:

| Time | method | metric        | Value |
| ---- | -      | ------        | ----- |
| 30   | GET    | http_requests | 50    |
| 30   | POST   | http_requests | 20    |
| 40   | GET    | http_requests | 55    |
| 40   | POST   | http_requests | 15    |
| 50   | GET    | http_requests | 65    |
| 50   | POST   | http_requests | 30    |

Errors:

| Time | code | method | metric      | Value |
| ---- | ---- | -      | ------      | ----- |
| 30   | 404  | GET    | http_errors | 5     |
| 30   | 500  | GET    | http_errors | 3     |
| 30   | 500  | POST   | http_errors | 2     |
| 30   | 502  | POST   | http_errors | 1     |
| 40   | 404  | GET    | http_errors | 0     |
| 40   | 500  | GET    | http_errors | 0     |
| 40   | 500  | POST   | http_errors | 3     |
| 40   | 502  | POST   | http_errors | 1     |
| 50   | 404  | GET    | http_errors | 0     |
| 50   | 500  | GET    | http_errors | 7     |
| 50   | 500  | POST   | http_errors | 3     |
| 50   | 502  | POST   | http_errors | 6     |

The intermediate joined table is logically computed by taking the Cartesian product (for each record of `errors` emit and output record for each record of `requests`) and then filter the results by records that have equal times and equal method tags.
**NOTE** The actual implementation is quite different and more efficient but the result is the same.

| Time | errors.code | method | errors.metric | requests.metric | errors.Value | requests.Value |
| ---- | ----------- | ------ | ------------- | --------------- | ------------ | -------------- |
| 30   | 404         | GET    | http_errors   | http_requests   | 5            | 50             |
| 30   | 500         | GET    | http_errors   | http_requests   | 3            | 50             |
| 30   | 500         | POST   | http_errors   | http_requests   | 2            | 20             |
| 30   | 502         | POST   | http_errors   | http_requests   | 1            | 20             |
| 40   | 404         | GET    | http_errors   | http_requests   | 0            | 55             |
| 40   | 500         | GET    | http_errors   | http_requests   | 0            | 55             |
| 40   | 500         | POST   | http_errors   | http_requests   | 3            | 15             |
| 40   | 502         | POST   | http_errors   | http_requests   | 1            | 15             |
| 50   | 404         | GET    | http_errors   | http_requests   | 0            | 65             |
| 50   | 500         | GET    | http_errors   | http_requests   | 7            | 65             |
| 50   | 500         | POST   | http_errors   | http_requests   | 3            | 30             |
| 50   | 502         | POST   | http_errors   | http_requests   | 6            | 30             |

The again we drop columns for tags that do not match and we combine the value columns into a single value column using the expression.

| Time | code | method | Value  |
| ---- | ---- | ------ | -----  |
| 30   | 404  | GET    | 5 / 50 |
| 30   | 500  | GET    | 3 / 50 |
| 30   | 500  | POST   | 2 / 20 |
| 30   | 502  | POST   | 1 / 20 |
| 40   | 404  | GET    | 0 / 55 |
| 40   | 500  | GET    | 0 / 55 |
| 40   | 500  | POST   | 3 / 15 |
| 40   | 502  | POST   | 1 / 15 |
| 50   | 404  | GET    | 0 / 65 |
| 50   | 500  | GET    | 7 / 65 |
| 50   | 500  | POST   | 3 / 30 |
| 50   | 502  | POST   | 6 / 30 |

Notice the tag `code` is still around since it did not have a conflicting column, it was simply added to the table as a result of the join.


#### Union
`union` will append all the series in one table to another.

```javascript
// Accepts: table
// Arguments:
//   m - required table to union
// Returns: table
// for example, merge events stream from two databases
select(database:"foo")
  .where(exp:{"metric" == "events"})
  .range(start:-1h)
  .union(m:select(database:"bar")
    .where(exp:{"metric" == "events"})
    .range(start:-1h)
  )
```


#### Sort
`sort` will sort the series in a table in ascending order by default.
They can either be sorted by their tags or by some expression.

```javascript
// Accepts: table
// Arguments:
//   exp - an expression to specify what to sort by. Should return a value that can be sorted
//   keys - a string array of tag keys to specify the sort order, required if exp not given
//   order - string of either asc or desc, asc is default
// Returns: table
select(database:"foo")
  .where(exp:{"metric"=="cpu_system"})
  .range(start:-1h)
  .sort(exp:{mean($)})
  .limit(n:10)
```

#### Rate
`rate` calculates the average rate of increase between each record in a table.

```javascript
// Accepts: table
// Arguments:
//   unit - a duration for what the rate should be. per second, per minute, etc.
// Returns: table
selelct(database:"foo",where:{"metric" == "network_in" AND "host" == "A")
  .range(start:-1h)
  .rate(unit:1s)
```

#### Aggregate Functions
Aggregate functions will process each block in a table an produce a block with a single record.
The resulting timestamp of the aggregate will be the same as the time of the last value by default.

##### Count

##### Mean

##### Percentile

##### Stddev

#### Selector Functions
Selector functions will process each block in a table an produce a block containing the selected records.
Selector functions differ from aggregates in that they don't combine data points, but select specific ones from the block.
The default behavior for selector will leave the timestamps unchanged for the points that have been selected.

##### Min

##### Max

##### Top

#### Transformation Functions
Transformation functions will process each block in a table.

##### Difference

### Examples

Here are a bunch of query examples for different scenarios.
The imaginary data isn't necessarily consistent from query to query.
The examples are here to illustrate what the language looks like for some real world query scenarios depending on what type of schema is set up.

```javascript
// get all cpu measurements for a specific host for the last hour
select(database:"foo")
  .where(exp:{"host"=="a" AND "system"=="cpu"})
  .range(start:-1h)

// get the 90th percentile, max, and mean of all cpu measurements
// for a specific host in 5m windows over last 4h
var cpu = select(database:"foo")
  .where(exp:{"host"=="A" AND "system"=="cpu"})
  .range(start:-4h)
  .window(every:5m)

cpu.percentile(n:90)
cpu.max()
cpu.mean()

// get the last value recorded for all sensors in a specific building
select(database:"foo")
  .where(exp:{"building"=="23" AND "type"=="sensor"})
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
select(database:"foo")
  .where(exp:{"type"=="sensor" AND "region"=="west"})
  .cardinality()
// {"tag1":23, "tag2":76, ...}

// get the number of hosts that have reported data in the west data center in the last 24h
select(database:"foo")
  .where(exp:{"region"=="west"})
  .cardinality(keys:["host"])
// {"host":234}

// get unique hosts in west region, but limit so you can paginate through
select(database:"foo")
  .where(exp:{"region"=="west"})
  .values(key:"host")
  .limit(n:50,offset:100)

// get the metrics that each host has reported data for in the last 24h
select(database:"foo")
  .where(exp:{"metric" NOT EMPTY})
  // TODO what is group?
  .group(keys:["host"])
  // TODO what is op in a merge call?
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
select(database:"foo")
  .where(exp:{
      "system"=="cpu" 
      AND "metric"=="usage_user" 
      AND "host" IN select(database:"foo").where(exp:{"service"=="mysql"}).values(key:"host").range(start:-4h)
    })
  .range(start:-4h)

// get the count in 10m periods in the last 24h from an event stream and
// filter that to only include those periods that were 2 sigma above the average
var m = select(database:"foo")
  .where(exp:{"event"=="pageview"})
  .range(start:-24h)
  .merge()
  .window(every:10m)
  .count()

// this is shorthand for m.stddev.join(exp:{$ * 2})
var sigma = m.stddev() * 2

var mean = m.mean()

// compute the difference from the mean
// shorthand for m.join(exp:{(m - mean).abs())
var diff = (m - mean).abs()

// return only the counts 2 sigma above
// shorthand for diff.where(exp:{$ > sigma})
diff > sigma
```
