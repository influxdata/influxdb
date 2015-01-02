# Continuous Queries

This document lays out continuous queries and a proposed architecture for how they'll work within an InfluxDB cluster.

## Definition of Continuous Queries

Continuous queries serve two purposes in InfluxDB: denormalizing data into many series or into a single series and aggregating and downsampling data. We refer to these as `fan-out`, `fan-in`, or `aggregation`. Before we get to the continuous query examples, we need to define the `INTO` syntax of queries.

### INTO

`INTO` is a method for running a query and having it output into either another measurement name, retention policy, or database. The syntax looks like this:

```sql
SELECT *
INTO [<retention policy>.]<measurement> [ON <database>]
FROM <measurement>
[WHERE ...]
[GROUP BY ...]
```

The syntax states that the retention policy, database, where clause, and group by clause are all optional. If a retention policy isn't specified, the database's default retention policy will be written into. If the database isn't specified, the database the query is running from will be written into.

By selecting specific fields, `INTO` can merge many series into one that will go into a new either a new measurement, retention policy, or database. For example:

```sql
SELECT mean(value) as value, region
INTO 1h.cpu_load
FROM cpu_load
GROUP BY time(1h), region
```

That will give 1h summaries of the mean value of the `cpu_load` for each `region`. Specifying `region` in the `GROUP BY` clause is unnecessary since having it in the `SELECT` clause forces it to be grouped by that tag, we've just included it in the example for clarity.

With `SELECT ... INTO`, fields will be written as fields and tags will be written as tags.

### Continuous Query Syntax

The `INTO` queries run once. Continuous queries will turn `INTO` queries into something that run in the background in the cluster. They're kind of like triggers in SQL.

```sql
CREATE CONTINUOUS QUERY 1h_cpu_load
ON database_name
BEGIN
  SELECT mean(value) as value, region
  INTO 1h.cpu_load
  FROM cpu_load
  GROUP BY time(1h), region
END
```

Or chain them together:

```sql
CREATE CONTINUOUS QUERY 10m_event_count
ON database_name
BEGIN
  SELECT count(value)
  INTO 10m.events
  GROUP BY time(10m)
END

-- this selects from the output of one continuous query and outputs to another series
CREATE CONTINUOUS QUERY 1h_event_count
ON database_name
BEGIN
  SELECT sum(count) as count
  INTO 1h.events
  GROUP BY time(1h)
END
```

Or aggregation in different intervals from all series in a measurement. This example assumes you have a retention policy named for each time interval that you're doing a `GROUP BY` on. e.g. one policy named `10m` and one `1h`.

```sql
CREATE CONTINUOUS QUERY cpu_aggregation
ON database_name
BEGIN
  SELECT mean(value), percentile(80, value) as percentile_80, percentile(95, value) as percentile_95
  INTO time().events
  GROUP BY time(10m), time(1h), *
END
```

The `GROUP BY *` indicates that we want to group by the tagset of the points written in. The same tags will be written to the output series. The multiple aggregates in the `SELECT` clause (percentile, mean) will be written in as fields to the resulting series. The time specifies that we want multiple aggregation windows.

Showing what continuous queries we have:

```sql
SHOW CONTINUOUS QUERIES
```

Dropping continuous queries:

```sql
DROP CONTINUOUS QUERY <name>
ON <database>
```

### Security

To create a continuous query, the user must have both read and write access to the database that is being queried from and written into.

## Proposed Architecture

