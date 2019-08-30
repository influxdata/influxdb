Metadata Queries
================

Background
----------

Flux meta data queries can be summarized as the following two questions:

> What are the list of available **tag keys** that have **time series data** for 
> the given **time range**?

and

> What are the list of **tag values**, for a specific **tag key**, which
> have **time series data** for the given **time range**?

It should also be noted that an arbitrary filter predicate may be specified to
reduce the search space further and that predicate may filter by series keys 
_or_ time series data.


### Flux Metadata Query Package

Flux provides a package, namely [`influxdb/v1`][], that offers several predefined
metadata queries, familiar to users of InfluxDB 1.x:


| function              | comment |
| :-------------------- | :------ |
| tagValues             | returns the unique values for a given tag |
| measurementTagValues  | returns the unique values for a given tag and measurement |
| measurements          | returns the list of measurements in a specific bucket |
| tagKeys               | returns the list of tag keys for all series that match the predicate |
| measurementTagKeys    | returns the list of tag keys for a specific measurement |


### Tag Keys

`tagKeys` is responsible for listing the tag keys or schema. The request is 
limited to a bucket. The time range and predicate have default values and are 
therefore optional. It is defined as follows:

```flux
tagKeys = (bucket, predicate=(r) => true, start=-30d) =>
    from(bucket: bucket)
        |> range(start: start)
        |> filter(fn: predicate)
        |> keys()
        |> keep(columns: ["_value"])
```

This query produces a single table, single column result set that lists
the set of tag keys for the given criteria.

It serves as the basis for `measurementTagKeys`, defined as:

```flux
measurementTagKeys = (bucket, measurement) =>
    tagKeys(bucket: bucket, predicate: (r) => r._measurement == measurement)
```


### Tag Values

As stated previously, `tagValues` returns the unique values for a given tag.
It is defined as follows:

```flux
tagValues = (bucket, tag, predicate=(r) => true, start=-30d) =>
    from(bucket: bucket)
      |> range(start: start)
      |> filter(fn: predicate)
      |> group(columns: [tag])
      |> distinct(column: tag)
      |> keep(columns: ["_value"])
```

This query also produces a single table, single column result set
listing the unique values for tag key "`tag`" with the remaining criteria 
equivalent to `tagKeys`.

`tagValues` serves as the basis for the other tag-value queries. For example,
`measurementTagValues` limits the results to a single measurement and is 
defined as follows, noting the use of a predicate:

```flux
measurementTagValues = (bucket, measurement, tag) =>
    tagValues(bucket: bucket, tag: tag, predicate: (r) => r._measurement == measurement)
```

### Current Implementation

In order to answer the meta data queries, Flux calls either the `Read` or 
`GroupRead` APIs, defined by the `Store` interface provided by the storage
layer.

```go
type Store interface {
	Read(ctx context.Context, req *datatypes.ReadRequest) (ResultSet, error)
	GroupRead(ctx context.Context, req *datatypes.ReadRequest) (GroupResultSet, error)
}
```

`ReadRequest` defines a number of parameters to control the behavior of the
API, including time range, filter predicate, optimization hints, etc. Some 
parameters, such as the `GroupKeys` and `GroupMode` are only used by the 
`GroupRead` API.

`Read` returns a `ResultSet`, the abridged version defined as follows:

```go
type ResultSet interface {
	// Next advances the ResultSet to the next cursor. It returns false
	// when there are no more cursors.
	Next() bool

	// Cursor returns the most recent cursor after a call to Next.
	Cursor() cursors.Cursor

	// Tags returns the tags for the most recent cursor after a call to Next.
	Tags() models.Tags
}
``` 

`ResultSet` is used to iterate over every series, limited by the input 
parameters (time range, filter predicate, etc). `Next()` is called to
move from one series to the next. `Tags()` returns the tag set of
the current series and `Cursor()` returns a cursor to read the time series data.

`cursors.Cursor` is specialized for each supported data type For example, 64-bit
integers are defined as:

```go
type Cursor interface {
	// ...
}

type IntegerArrayCursor interface {
	Cursor
	Next() *IntegerArray
}
```

Each call to `Next()` produces blocks of up to 1,000 values or `nil` to 
represent EOF.

`tagValues` uses this API to find all

Proposal
--------

### Current Architecture

Neither of the metadata questions posed in the overview require the time series data 
be sent to Flux for further processing, however, based upon the current 
architecture, a typical query would require the TSM data be consulted to determine the 
_have data for the given time range_ requirement. Depending on the 
query parameters, there are several levels of increasing complexity and cost 
to which the TSM data is accessed, starting from _no access_ to fully 
decoding TSM time series blocks.


#### No TSM Access

In this scenario, the time range covers the entire retention period, therefore
if the series key exists, it must have time series data. Also, no filter 
predicate is specified that requires decoding and evaluating time series data.


#### TSM Index Only

In this scenario, only the TSM index need be consulted to determine if
data exists. For this to occur, the time range is within the retention 
period, however, it either overlaps or covers an entire block. The
index contains a reference to every block and for each block, includes the 
first and last timestamp. Visually, the following shows when this
would occur:

```ascii
          +------------------------------+
          |          TSM BLOCK           |
          +------------------------------+
 +-----------------+           +-----------------+
 |     RANGE 1     |           |     RANGE 2     |
 +-----------------+           +-----------------+
+--------------------------------------------------+
|                     RANGE 3                      |
+--------------------------------------------------+
``` 

The left and right bounds of the rectangles represent start and end
timestamps. Again, no filter predicate is specified that requires decoding 
and evaluating time series data. 

#### Decode Timestamps

In this scenario, the time range is entirely within the bound of a block of 
data. Visually, `RANGE 1` is within the time bound of a single block:

```ascii
          +------------------------------+
          |          TSM BLOCK           |
          +------------------------------+
              +-----------------+
              |     RANGE 1     |
              +-----------------+
``` 


#### Decode Timestamps and Values

Albeit an edge case and not a typical metadata query executed by tools
such as Chronograf, the worst case would require decoding the timestamps 
and values in order to evaluate a filter predicate, such as the following:

```flux
tagValues(
    bucket:      "foo", 
    tag:         "host", 
    predicate: (r) => r._measurement == "cpu" AND r._field == "usage_system" AND r._value > 0.5)
```

Which says return all the values for tag `host` which have values greater than
0.5 for the field `usage_system`.


Benchmarks
----------

The benchmarks attempt to establish various upper bounds. TK

All benchmarks use the `bonitoo.toml` schema with the `influxd generate` tool 
to create a data set of 827,942 series.


### Series Keys

The following command uses a series cursor perform a linear scan of all 
series keys. Various switches enable sorting or warming of the series file.

Enumerating all the series would be required in order to determine the distinct
list of measurements

```sh
$ bin/darwin/influxd store series --org-id=0391ee02e3fae000 --bucket-id=0391ee02e3fae001
```

#### Results

| sorted | warmed | series/s    | time (ms)  |
| :----: | :----: | ----------: | :--------- |
|        |        |   1,173,091 |        703 |
|        | X      |   1,619,618 |        511 |
| X      |        |     204,180 |      4,055 |
| X      | X      |     215,110 |      3,849 |


[`influxdb/v1`]: https://github.com/influxdata/flux/blob/master/stdlib/influxdata/influxdb/v1/v1.flux