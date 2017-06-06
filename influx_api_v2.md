# InfluxDB API Redesign

Now that we have experience with people using InfluxDB and more feedback from the community around use cases, I wanted to put together some thoughts on the requirements as I see them and potential ways to structure the API going forward to best meet those requirements.

## Goals

First, let's start with some general goals of the project. People usually take these three goals as a given, but I list them here at the top just to show their importance in this project.

1. Ease of use. The API should be easy to understand and it should push people in the right direction. If the default behavior yields surprising results, we've failed.
2. Performance. Given the use cases I outline later in this document, fast performance is a requirement. This goes together with ease of use. Bad performance is a surprising result and means we've failed not only with this requirement, but requirement #1.
3. Scalability. We should be able to support millions of separate time series (if modeled like Graphite), or thousands of metrics with tag sets that have cardinality in the tens of thousands (if modeled like OpenTSDB).

Now these requirements are pretty loose. I can already see @jvshahid shaking his head at me because they're non-specific. Like, what kind of performance guarantees are we talking about here? What does ease of use mean? Easy for whom?

These are ongoing goals of the project that should be improved over time. They'll never be done because any of these three can always find room for improvement. They're just something to keep in mind when doing any work.

Ok, now onto some use cases.

## Use Cases

There are three different use cases that crop up most often with people using or interested in using InfluxDB:

1. DevOps data - what happens on servers, in their applications, etc.
2. Sensor data - IoT, industrial applications, power, etc.
3. Real-time user/business analytics - Counting page views, unique user counts, funnels, and more

When querying this data the work is usually centered around either _discoverability_ or _computations on specific series_.

### Discoverability

Discoverability is all about finding out what time series exist for given periods of time. Here are some example questions one might ask:

* Which data centers exist?
* For a given data center, what time series exist?
* For a given data center, which hosts reported CPU in this hour?
* For cpu, given data center 1, which hosts reported a load > 90%?
* Which devices haven't reported temp in the last day?

These examples are all devops focused, but sensor data ends up looking very much the same. For example, if we have buildings and we're taking measurements from different devices within those buildings:

* What buildings exist?
* For a given building, what devices or measurements exist?
* For a given device, what buildings have it?

You can do similar types of queries within the user analytics and business intelligence space.

Discoverability could be a query about metadata like what hosts have I seen in the last hour, or it could be something that requires some sort of computation like, what hosts have memory usage > 80% in the last hour. The first question only looks at metadata, while the second would require meta data lookup (which hosts exist) and then a computation against all the appropriate time series.

### Computation and transforms on series

Computation on series could be aggregate functions like `min`, `max`, `mean`, etc. But it can also require combining or morphing series by either normalizing them with values at given time intervals, applying a function like derivative on the series, down sampling to a given number of points, merging multiple series together and performing a computation against the result, dividing one by another, or doing any number of these things on many series.

## Existing approaches

Many people are coming from either a Graphite or OpenTSDB background. Some are coming from an experience of rolling their own solution on top of Cassandra or MongoDB. First, I'll talk a little bit about how Graphite and OpenTSDB frame these problems and then I'll dig into some requirements and thoughts on how Influx should be designed.

The graphite method of representing time series data is to have many series and to encode the metadata in the series names. Each series only has a single value and all series have an underlying assumption of some data collection interval (like once every 10s). For instance, if you have a server in a data center with a cpu idle: `hosta.dc1.cpu.idle` Or if you have a temperature sensor on a device: `device1.temp`.

These names can quickly get quite large. Graphite's method of drilling down on these metrics is hierarchical. That's how discovery works, by selecting down the tree (like a directory structure). You can then merge series together or perform functions against a few of them. You never need to specify what value you're working with because it's in the series name and each series only has one set of values (as opposed to columns).

OpenTSDB uses the concept of tags. In our previous example you'd have `metrics` called `cpu.idle` and `temp` respectively. The meta data of which data center, server, or device you're looking at is kept as a "tag". Tags aren't hierarchical in OpenTSDB.

Personally, I find the tag method of working with things a bit nicer. People are often using InfluxDB columns like this. The only problem is that you can quickly get into a situation where performance is very poor.

Something these two approaches have in common is that there is only a single value given a series name (in graphite) or a metric and a set of tags (in OpenTSDB). I believe this makes things simpler for graphing and monitoring libraries. They always know what it is they're going to be mapping.

## InfluxDB's design

These are some ideas on the design of the InfluxDB API. Note that these are just ideas for the next version of the API. Feedback and modifications are welcome!

### Retention Policies (was Shard Spaces)

I want to throw out the idea of shard spaces. Their entire purpose was to handle two things: replication factor and retention policy. Shard spaces as a user facing API concept is too close to how things are implemented under the hood. Instead, we'll simply define retention policies and their replication factors.

Here's an example retention policy:

```json
{
  "name": "1_week",
  "duration": "7d",
  "replicationFactor": 2
}
```

This looks very similar to the shard space definition. However, notice that we don't have split, duration is the actual retention period, and (most importantly) the regex to match against the series name is no longer present.

Within a retention policy data will still be split out into shards. Shards will store a contiguous block of time. The size of the block of time they represent will be determined automatically by Influx based on the duration of the retention policy. We could always give the user the ability to override this, but I think having it be automatic is better.

Split has also been removed to make things simpler for the user. It'll be determined automatically based on the size of the cluster when each new period of time has shards created.

Databases will have a default retention policy that all writes go into. A database definition:

```json
{
  "name": "paulDB",
  "retentionPolicies": [
    {
      "name": "1_week",
      "duration": "7d",
      "replicationFactor": 2
    },
    {
      "name": "6_months",
      "duration": "6M",
      "replicationFactor": 3
    }
  ],
  "defaultRetentionPolicy": "1_week"
}
```

Notice that the retention policies are part of the database definition. The default retention policy is what all writes will go into, unless otherwise specified at the time of the write. The same is true for any queries issued against the database. By default it will issue the query against the default retention policy.

This makes the assignment to a retention policy explicit, which was something in shard spaces that was causing a great deal of confusion for users. Obviously, this means that the retention policy is something that will have to be exposed in the query language.

Before moving onto that, I'd like to address data types, columns, and how things are structured.

### Columns, data types, and tags

By watching people use Influx over the last 6 months, I've seen a number of repeated questions and we're starting to see some regular usage patterns. I'd like those represented in the structure of the database and the API. The simplicity of Graphite and OpenTSDB's single value per series makes things like graphing and monitoring easier (at least it seems that way). I'd like to do that with Influx, but still keep our flexibility to do some of the other things people do.

Here are a few ideas around how to structure things:

Every series has a single value that can be either a boolean, double, string, or bytes. The type is set when the first value is written into the series. Any future writes that try to write a different type will throw an error. 

Series still have the old special columns of `time` and `sequence_number`. However, sequence number is now optional and not present by default. If users want `sequence_number` assigned, which you'd only want if there's a chance two data points could have the same `time`, it should be specified when the series is created. Generally you'd only use this for irregular event streams.

Other than the single value, the series name, the time, and the optional sequence number, individual data points can have tags. Tags are hierarchical key value pairs that will be indexed. This means each series can have only a single value and if you have unstructured data, it should be a string or bytes. Don't worry, the query language will make it easy to combine this with other series later.

Here's an example of a single data point (or measurement):

```json
{
  "name": "cpu_load",
  "tags": ["dataCenter/USWest/host/serverA/cpuId/1"],
  "double": 68
}
```

Note that this series is typed to use a `double`. Other types are `string`, `bool`, `bytes`. After the first write to `cpu_load` all future writes can use only the same value type.

The tags is an array of strings defined as `:key/:value/:key/:value/...`. You can have any character in the key or value. You'll just have to escape `"`, `/`, and `\`. In the case of `/` you'll have to double escape it so the escape character comes through. Like `has a \\/ in the key/some value`.

Series will be stored in a index that is arranged by the hierarchy. Note that measurements can exist in multiple hierarchies. Also, you can add other indexes later. Say for example that we often want to query across the entire data center. We could add an index like this:

```sql
create index "cpu_load_dataCenter"
ON "cpu_load" ("dataCenter")

-- or for each data center and host
create index "cpu_load_dataCenter_host"
ON "cpu_load" ("dataCenter", "host")

-- or simply for each host
create index "cpu_load_host"
ON "cpu_load" ("host")

-- and maybe drop the default index
drop index "cpu_load_dataCenter_host_cpuId"
```

What those indexes do under the hood is create new series for each of those indexes where we have the value and the tags along with it.

## Writes

Now that we have the different column types defined and retention policies, we can show how to write data in. One assumption this write structure makes is that we often have a set of measurements coming from something where the metadata is the same, and the measurements are across different sensors.

Here's the most basic non-compact representation. Post data like this:

```json
[
  {
    "name": "cpu_load",
    "values" : [
      {
        "double": 89.0,
        "tags" : ["dataCenter/USWest/host/serverA/cpuId/1"],
        "time": 1412689241000
      }
    ]
  }
]
```

As usual, time can be omitted and it will get assigned automatically by InfluxDB.

You can also write values with different tags. This essentially creates two separate series (one for each tag set).

```json
[
  {
    "name": "cpu_load",
    "values" : [
      {
        "double": 89.0,
        "tags" : [
          "dataCenter/USWest/host/serverA/cpuId/1",
          "application/someAppThatUsesThisServer"
        ],
        "time": 1412689241000
      }
    ]
  }
]
```

Any attribute that is shared across all values can be pulled out into the upper map:

```json
[
  {
    "name": "cpu_load",
    "values" : [
      {
        "double": 89.0,
      }
    ],
    "time": 1412689241000,
    "tags": ["dataCenter/USWest/host/serverA/cpuId/1"],
  }
]
```

Or you can have sequence numbers assigned to ensure that you can have two events at the same time stamp:

```json
[
  {
    "name": "events",
    "values" : [
      {
        "bool": true,
        "tags" : ["type/click/button/signup/user/paul"]
      }
    ]
    "setSequenceNumber": true
  }
]
```

Or posting to multiple series at the same time:

```json
[
  {
    "values" : [
      {
        "name": "cpu_load",
        "double": 89.0,
      },
      {
        "name": "cpu_wait",
        "double": 5
      },
      {
        "name": "top_output",
        "string": "stuff here..."
      }
    ],
    "time": 1412689241000,
    "tags": ["dataCenter/USWest/host/serverA"]
  }
]
```

Or writing into a given retention policy:

```json
[
  {
    "name": "exceptions",
    "values" : [
      {
        "string": "some stack trace",
        "tags": ["controller/Users/action/Show"]
      }
    ]
    "setSequenceNumber": true,
    "retention": "forever"
  }
]
```

The above assumes that you've created a retention policy named "forever" for the given database. It will throw an error otherwise.

As you can see from the examples, the top level elements of the map can be pulled into the values themselves and visa-versa. This makes it easy to post many values at the same time without repeating shared tags and times.

## Indexes

The section on indexing goes into the internals of how things are going to be put together. If you only care about the query language, feel free to skip this part.

We'll have to do some performance testing to see how queries work that end up merging thousands of series. My guess is we'll want to have indexes that denormalize the data.

```sql
create index cpu_load_dataCenter
ON cpu_load (dataCenter)
```

Now, when a write goes into `cpu_load` it will actually get expanded into writes for the default series `(cpu_load, USWest, serverA.influxdb.com)` and the index series. My thought is it would create a new series for `(cpu_load, USWest)` and that would have two ranges: one for the double values, and one for the non-indexed tag data associated with each point. For example if we wrote:

```json
{
  "double": 68,
  "tags": ["dataCenter/USWest/host/serverA.influxdb.com"]
}
```

We'd have three writes occur on the underlying database: one double for the `(cpu_load, USwest, serverA.influxdb.com)` series, which will map to a given id. We'll write one double for the `(cpu_load, USWest)` series, which will have its own unique id. Finally, we'll write one tag blob for the `(cpu_load, USWest)` series where the value is a set of column and value ids: `[1, 1]` where the first is the column id 1 = host and the second is the value id 1 = serverA.influxdb.com. If the metadata is new, we'll have to do writes for that too.

Of course, this is something we'll have to performance test across a number of use cases. How do queries perform when the cardinality of a column is 10, 100, 1,000, 10,000, 100,000, 1,000,000, and 10,000,000?

Note that any series will end up mapping to a single server for a given period of time. This means that you could potentially have hot spots for some uses cases. For example, if you have a series like this:

```json
{
  "name": "events"
  "bool": true,
  "tags": ["type/click/screen/home/user/23"]
}
```

And you had the following indexes:

```sql
create index "events_types"
ON "events" ("type")

create index "events_users"
ON "events" ("user")
```

Every time you write to the event stream like in the top example, you'd end up doing 5 writes: 1 for the full series, two for the types series (both value and tag blob), and two for the users series (again for value and tag blob). That itself isn't necessarily a hot spot since the user specific series can be sharded out to different servers.

However, the type series is the one that may be of concern. If the vast majority of events are of type `click`, then all those writes will be going to the same server. The hack around this would be to have another tag called `partition` that is just a random number between 1-10. Then have the index be on the type and the partition. You can always merge them back together at query time.

## Metadata Indexes

The mappings of series and tag values to ids and ids back to their tag values and series will be kept in the metadata index. It'll also have to keep other index structures for answering questions about metadata. For example:

* Given a series name, what top level tags exist?
* Given a series name and tag, what tag values exist for that tag?
* Given a series, a tag and a tag value, what other tags exist?
* Given a tag, what series exist?
* Given a tag and a value, what series exist?

This metadata needs to be indexed so that time can be taking into account. Queries for what tags and tag values exist should be able to have time limits on them. Also, series, tags, and tag values should go away if their corresponding data is aged out through retention policies.

However, we don't want to update the metadata indexes on every write, which is what we'd need for exact precision. It'll probably make sense to do something like creating buckets of time for the indexes.

We'll need indexes for each retention period. For a retention period of `inf` we'll simply have a single index. But for a retention period of `365d` we'll probably want indexes for every `30d`. Or for others we can just do indexes per day.

Let's take the per day example to see what those indexes will look like. Say we're writing data like this to a retention period that keeps data around for `30d`:

```json
{
  "name": "temperature",
  "values": [
    {"double": 70.1}
  ],
  "tags": [
    "building/1/device/23"
  ]
}
```

First, we'll need an id to uniquely identify this series `temperature/building/1/device/23` in the database. For that, we'll use something that isn't scoped by day. Say it's being written into a retention period named `30days`.

```
KEY: ID/30days/temperature/building/1/device/23
VALUE: 1
KEY: SERIES/1
VALUE: 30days/temperature/building/1/device/23
```

In this example this is the first series we've written in so it gets an id of `1`. If we had multiple tags in that post, we would have created a separate series per tag set.

We'll have to go through occasionally and clean up these IDs, but that should be done during retention cleanup periods.

Let's look at how the indexes are stored. Let's say we also had writes in the `temperature` series that had tags of `building/1/device/26` and `building/2/device/38`.

We'd write the following values for the day indexes

```
2014-10-25/names/30days/temperature -> temperature
2014-10-24/series/30days/temperature/building/1/device/23 -> 1
2014-10-24/series/30days/temperature/building/1/device/26 -> 2
2014-10-24/series/30days/temperature/building/2/device/38 -> 3
2014-10-25/tags/1/30days/temperature/building -> building
2014-10-25/tags/2/30days/temperature/building/1/device -> device
2014-10-25/tag_values/1/30days/temperature/building/1 -> 1
2014-10-25/tag_values/1/30days/temperature/building/2 -> 2
2014-10-24/tag_values/2/30days/temperature/building/1/device/1/23 -> 23
2014-10-24/tag_values/2/30days/temperature/building/1/device/1/26 -> 26
2014-10-24/tag_values/2/30days/temperature/building/2/device/2/38 -> 38
```

After those have been written once for the day, we shouldn't need to write them again for all other measurements for `30days/temperature/building/1/device/23` and the other series. The tricky part with this will be handling roll over of the index period. Because metrics are often collected at regular intervals, at the start of a new day we'd suddenly get millions of index writes all in a few seconds.

We'll have to figure out how to stagger that. Also, since every write will need to check the index, we'll need to make those lookups very fast.

The `tags` and `tag_values` portions of the tree have the depth of the tree immediately after then the rest of the tree after the depth i.e. `.../tag_values/2/30days...`. This keeps the ordering correct in the key space so that we can quickly scan through tag sets at a given depth.

Using that structure, we can answer for a given day with efficient range scans:

* What series names have shown up?
* For a given series name, what top level tags exist?
* For a given series name, top level tag and value, what lower levels exist?

Walking the hierarchy is fast with this layout. What if we wanted to answer these queries:

* For a given top level tag, what series exist?
* For a given tag and value, what series exist?
* What is the bottom of the tree for the temperature series?
* Get me all the series where name = temperature and building = 1

The brute force way would be to just scan the tree. Although I'm not sure this is going to scale. Also, some of these queries would be better to answer if the tagging structure weren't hierarchical. This begs the question: should tags be hierarchical or free form? I'll leave that as an open question for now.

Across the cluster a copy of the meta data indexes is stored on every server. Those updates are fed through the Raft topic for cluster meta data. We'll have to test how big this gets in deployments with tens of millions of distinct series (the cardinality of names + unique tag sets).

## Metadata on series

We should be able to keep metadata on different series. This will help both with graphing tools, but also if we want to attempt to automatically answer queries based on the precision that is being requested.

Here are the metadata fields:

```json
{
  "units": "ms",
  "period": "10s",
  "type": "count|timing|gauge|other"
  "dataType": "double|bool|string|bytes"
}
```

Units is used only for display purposes. A period of `0` means that it's a series for irregular event style data, not something that's collected on a regular interval. This could be log lines where the value of the series is a string, or indivdual request timings where the value is a double of the number of milliseconds taken, or a stock trade where the value is the price of the trade. The type can be either a count, timing, gauge, or other. The data type is set when the first value is written into a series.

One potentially interesting thing to do with the gauge type is to only store the value if it differs from the previous measurement. That way we get much more efficient storage and missing values can be filled on the fly at query time.

## Example Queries (function chaining style)

Here are some ideas for doing queries in a syntax other than SQL. Some of this may be inconsistent. I'm looking for feedback on which ideas work or look most readable/understandable.

### Getting names, tags, tag values, series

```javascript
// names from the 30d retention period
names("30d")
// returns tags for this level of the tree
tags("30d/temperature")
// returns tags for next level of tree
tags("30d/temperature/building/1")
// returns values for given level of the tree
values("30d/temperature/building")

// returns the series (names and ids).
// won't work all the way down the tree
series("30d/temperature/building/*")
// return for specific buildings
series("30d/temperature/building/[1,2]")
// will return series names and ids all the way down the tree
series("30d/temperature/building/**")
series("30d/temperature/building/[1,2]/**")

// any of the functions can take n arguments:
names("30d", "2year")
tags("30d/temperature", "30d/kwusage")

// any of the functions can be scoped by time
names("30d", when: "today|yesterday|-7d")
tags("30d/temperature", when: "now to -2d")
```

### Transforming series

The output of calls to series can have transformations run on them. Some examples:

```javascript
// return a separate series for every building. Assuming hierarchy of building/device:
// first, get the series and the raw values for the last 7 days
// then normalize and fill any missing device measurements from the previous value.
// forces measurements for every 10 seconds.
// then merge devices in a building into a single series per building
// then aggregate using the mean function in 10 minute intervals
series("30d/temp/building/**")
  .data(when: "now to -7d")
  .normalize(period: "10s", value: "previous")
  .merge(tags: "device")
  .aggregate(function: "mean", period: "10m")
  .filterSeries(allPoints: "== 0 or < 50", anyPoint: "< 0")
```

This shows selecting many series together, normalizing them so they have the same number of data points, merging some of them together and aggregating to output a bunch of series. Finally, we drop series that have either all points in them meeting some criteria or any point in the series meeting another criteria.

Let's take an example of finding the hosts in a data center with the greatest CPU utilization. Assume we have a structure of `30d/cpu_load/dataCenter/USWest/host/serverA`.

```javascript
series("30d/cpu_load/dataCenter/USWest/host/*")
  .data(when: "now to -4h")
  .filterSeries(top: 10)
```

And some other potential examples

```javascript
// downsampling
series("30d/cpu_load/dataCenter/USWest/host/[serverA, serverB, serverC]")
  .data(when: "now to -7d")
  .sample(function: "random", period: "30m")

// other transformations
series("30d/redis_commands_processed/dataCenter/USWest/host/*")
  .raw(when: "now to -7d")
  .nonNegativeDerivative()

// errors per minute. will ignore div by zero errors
series("30d/errors/application/myApp", "30d/requests/application/myApp")
  .data(when: "now to -2h")
  .aggregate(function: "count", period: "1m")
  .join(
    left: "30d/requests/application/myApp",
    right: "30d/errors/application/myApp",
    function: "/")

// getting two series over different criteria. For example, 
// returning normalized data for the last hour and the previous hour
(
  series("30d/cpu_load/dataCenter/USWest/host/serverA")
    .data(when: "now to -1h"),
  series("30d/cpu_load/dataCenter/USWest/host/serverA")
    .data(when: "-1h to -2h")
    .timeShift("+1h")
).normalize(period: "10s", value: "previous")
```

Those are some basic ideas around using a funcion chaining style heavily inspired by jQuery and D3. On to some more SQL-ish examples.

## Example Queries (SQL style)

Throwing out some example queries to show how to answer different kinds of questions.

### Getting the tags and tag values for a series

#### Get the list of tag names that have appeared for a series
```sql
select tags(cpu_load)
-- or limit by time
select tags(cpu_load) where time > now() - 1h
```
```json
[{
  "name": "cpu_load",
  "columns": ["name"]
  "values": [
    ["dataCenter"],
    ["host"]
  ]
}]
```

#### Get the list of tag names for multiple series names.
```sql
select tags(cpu_load), tags(cpu_wait)
```
```json
[
  {
    "name": "cpu_load",
    "columns": ["name"]
    "values": [
      ["dataCenter"],
      ["host"]
    ]
  },
  {
    "name": "cpu_wait",
    "columns": ["name"]
    "values": [
      ["dataCenter"],
      ["host"]
    ]
  }
]
```

#### Get the tag values
```sql
select tag_values(cpu_load, host)
```
```json
[{
  "name": "cpu_load",
  "columns": ["host"],
  "values": [
    ["serverA"],
    ["serverB"]
  ],
}]
```
```sql
-- or filter by data center
select tag_values(cpu_load, host)
where dataCenter = 'USWest'
```
```json
[{
  "name": "cpu_load",
  "columns": ["host"],
  "values": [
    ["serverA"]
  ],
  "tags": {
    "dataCenter": "USWest"
  }
}]
```
```sql
-- or get compound values
-- tag_values takes (name, tagNames...)
select tag_values(cpu_load, dataCenter, host)
```
```json
[{
  "name": "cpu_load",
  "func": "tag_values",
  "columns": ["dataCenter", host"],
  "values": [
    ["USWest", "serverA"],
    ["USEast", "serverB"]
  ]
}]
```

```sql
-- it's always better for the user to specify a time frame
select tag_values(cpu_load, host) where time > now() - 1h
```

#### Get number of series for a tag combination
```sql
-- find out how many tag combinations (and thus series)
-- there are for a given name
select count(tag_values(cpu_load, tags(cpu_load)))
```
```json
[{
  "name": "cpu_load",
  "columns": ["count"],
  "values": [
    [235]
  ]
}]
```

### Querying series

Here are some example queries

```sql
-- see which series names exist in the default retention policy. In this new model, the number of 
-- names should be much smaller. Probably on the order of a few hundred to a few thousand per 
-- retention policy
list names

-- or see the names for a given retention policy
list names for 6_month
-- or with special characters
list names for "6 month"

-- list retention policies

-- assume cpu_load has data_center and host and no other columns
-- get the raw data for a single series
select cpu_load
where data_center = 'us-west' and host = 'serverA' and
  time > now() - 1h

-- aggregate on the fly from a raw series
select mean(cpu_load)
where data_center = 'us-west' and host = 'serverA' and
  time > now() - 24h
group by time(10m)

-- select from multiple series
select mean(cpu_load), max(cpu_wait)
where data_center = 'us-west' and host = 'serverA' and
  time > now() - 24h
group by time(10m)
```
```json
[
  {
    "name": "cpu_load",
    "columns": ["mean", "time"],
    "values": [...],
    "tags": {
      "data_center": "us-west",
      "host": "serverA"
    }
  },
  {
    "name": "cpu_wait",
    "columns": ["mean", "time"],
    "values": [...],
    "tags": {
      "data_center": "us-west",
      "host": "serverA"
    }
  }
]
```

#### Query by regex
```sql
select log_lines
where value =~ /error/i
  and time > now() - 4h

-- query against columns on regex
select string, application from log_lines
where application =~ /^ruby.*/
  and time > now() - 1h

-- merge all hosts from a given data_center and downsample
select mean() from cpu_load
where data_center = 'us-west' and
  time > now() - 24h
group by time(10m)

-- output separate points per uniuqe host id
select mean() from cpu_load
where data_center = 'us-west' and
  time > now() - 24h
group by time(10m), host

-- expand into multiple series. Saves space not repeating column values.
-- this is also closer to what people actually want when graphing or working with data.
select mean() from cpu_load
where data_center = 'us-west' and
  time > now() - 24h
group by time(10m)
expand by host
/* returns something like
[
  {
    "name": "cpu_load",
    "sharedColumns": {
      "host": "serverA"
    },
    "columns": ["mean", "time"],
    "values": [[23.2, 1412689241000], [19.0, 1412689231000]]
  },
  {
    "name": "cpu_load",
    "sharedColumns": {
      "host": "serverB"
    }
    "columns": ["mean", "time"],
    "values": [...]
  }
]
*/

-- expanding out into all series for a name
selelct mean() from cpu_load
where time > now() - 1h
group by time(1m)
expand by columns()

-- merging separate series. I'm not sure if this is useful in any way
select mean() from merge(cpu_load, cpu_wait)
group by time(10m)
where data_center = 'us-west' and host = 'serverA'
  and time > now() - 24h
/* return something like
[
  {
    "name": "cpu_load AND cpu_wait",
    "columns": ["mean", "time", "name"],
    "values": [[23.2, 1412689241000, "cpu_load"], [1.1, 1412689241000, "cpu_wait"], ...]
  }
]
*/

-- getting the top cpu load hosts
select double from cpu_load
where data_center = 'us-west' and time > now() - 1h
order by double desc
limit 10

-- or get the top cpu load hosts from downsample
select mean() from cpu_load
where data_center = 'us-west' and time > now() - 8h
group by time(5m)
order by mean desc
limit 10

-- so order by and limit get applied after any group
-- by time perid constructs the series.
-- If an order by is given, it means the entire series
-- will be loaded into memory, so we'll have to be careful.
```

### Queries and retention policies

All of the query examples showed hitting a name, but made no mention of retention policies. Queries are directed to the default retention policy. You can override this by specifying the retention policy in query:

```sql
-- gets the raw data points from 6_month retention policy
select 6_month.cpu_load

-- or wrap in quotes if you have special characters
select "6 month"."CPU Load"
```

So all queries are directed to an explicitly specified retention policy.

#### Automatically selecting lower precision data

However, the next section covers continuous queries, which will let you aggregate and downsample into other retention policies. You can have queries attempt to automatically pick which retention policy to query based on which continuous queries and aggregations have been running.

Simply replace the `select` keyword in any query with `scale`.

### Continuous queries

I think changing the continuous query syntax slightly is a good idea. We'll have to do that anyway for the retention policy stuff.

```sql
-- a continous query to downsample from the default retention
-- into a longer term retention. Will create new series there
-- with the column data carried over
select mean() from cpu_load
group by time(1h)
expand by columns()
continuously into "6_month"."cpu_load"

-- and chain it: obviously the mean is now a mean of means.
-- Don't worry, the ends justify it.
select mean() from 6_month.cpu_load
group by time(1d)
expand by columns()
continuously into "3_years"."cpu_load"
```

## Open Questions

* Which syntax style is preferred (SQL or function chaining)?
* Should tags be strictly hierarchical or open ended?

## Tasks

Some tasks that I think are good places to break things out. The first two will probably need to be done before the others, but the rest can probably be done in parallel.

- [ ] Refactor protocol with new schema style
- [ ] Create query object/interface
- [ ] Create metastore with tag indexes (should be backed by a disk based DB (level, bolt, etc))
- [ ] Create query parser (should be pure Go)
- [ ] Refactor datastore to work with new protocol
- [ ] Refactor engine code to take new API concepts into account and work with new protocol. Should expose interfaces that make it easy to define new transformations, merge functions, join functions, and aggregates. Interface should account for doing map/reduce style computation.
- [ ] Implement indexes
