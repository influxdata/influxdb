# InfluxQL Transpiler

The InfluxQL Transpiler exists to rewrite an InfluxQL query into its equivalent query in Flux. The transpiler works off of a few simple rules that match with the equivalent method of constructing queries in InfluxDB.

**NOTE:** The transpiler code is not finished and may not necessarily reflect what is in this document. When they conflict, this document is considered to be the correct way to do it. If you wish to change how the transpiler works, modify this file first.

1. [Identify the cursors](#identify-cursors)
2. [Identify the query type](#identify-query-type)
3. [Group the cursors](#group-cursors)
4. [Create the cursors for each group](#group-cursors)
    1. [Identify the variables](#identify-variables)
    2. [Generate each cursor](#generate-cursor)
    3. [Join the cursors](#join-cursors)
    4. [Evaluate the condition](#evaluate-condition)
    5. [Perform the grouping](#perform-grouping)
    6. [Evaluate the function](#evaluate-function)
5. [Join the groups](#join-groups)
6. [Map and eval columns](#map-and-eval)

## <a name="identify-cursors"></a> Identify the cursors

The InfluxQL query engine works by filling in variables and evaluating the query for the values in each row. The first step of transforming a query is identifying the cursors so we can figure out how to fill them correctly. A cursor is any point in the query that has a **variable or a function call**. Math functions do not count as function calls and are handled in the eval phase.

For the following query, it is easy to identify the cursors:

    SELECT max(usage_user), usage_system FROM telegraf..cpu

`max(usage_user)` and `usage_system` are the cursors that we need to fill in for each row. Cursors are global and are not per-field.

## <a name="identify-query-type"></a> Identify the query type

There are three types of queries: raw, aggregate, and selector. A raw query is one where all of the cursors reference a variable. An aggregate is one where all of the cursors reference a function call. A selector is one where there is exactly one function call that is a selector (such as `max()` or `min()`) and the remaining variables, if there are any, are variables. If there is only one function call with no variables and that function is a selector, then the function type is a selector.

## <a name="group-cursors"></a> Group the cursors

We group the cursors based on the query type. For raw queries and selectors, all of the cursors are put into the same group. For aggregates, each function call is put into a separate group so they can be joined at the end.

## <a name="create-groups"></a> Create the cursors for each group

We create the cursors within each group. This process is repeated for every group.

### <a name="identify-variables"></a> Identify the variables

Each of the variables in the group are identified. This involves inspecting the condition to collect the common variables in the expression while also retrieving the variables for each expression within the group. For a function call, this retrieves the variable used as a function argument rather than the function itself.

If a wildcard is identified, then the schema must be consulted for all of the fields and tags. If there is a wildcard in the dimensions (the group by clause), then the dimensions are excluded from the field expansion. If there is a specific listing of dimensions in the grouping, then those specific tags are excluded.

### <a name="generate-cursor"></a> Generate each cursor

The base cursor for each variable is generated using the following template:

    create_cursor = (db, rp="autogen", start, stop=now(), m, f) => from(db: db+"/"+rp)
        |> range(start: start, stop: stop)
        |> filter(fn: (r) => r._measurement == m and r._field == f)

### <a name="join-cursors"></a> Join the cursors

After creating the base cursors, each of them is joined into a single stream using an `inner_join`.

**TODO(jsternberg):** Raw queries need to evaluate `fill()` at this stage while selectors and aggregates should not.

    > SELECT usage_user, usage_system FROM telegraf..cpu WHERE time >= now() - 5m
    val1 = create_cursor(db: "telegraf", start: -5m, m: "cpu", f: "usage_user")
    val1 = create_cursor(db: "telegraf", start: -5m, m: "cpu", f: "usage_system")
    inner_join(tables: {val1: val1, val2: val2}, except: ["_field"], fn: (tables) => {val1: tables.val1, val2: tables.val2})

If there is only one cursor, then nothing needs to be done.

### <a name="evaluate-condition"></a> Evaluate the condition

At this point, generate the `filter` call to evaluate the condition. If there is no condition outside of the time selector, then this step is skipped.

### <a name="perform-grouping"></a> Perform the grouping

We group together the streams based on the `GROUP BY` clause. As an example:

    > SELECT mean(usage_user) FROM telegraf..cpu WHERE time >= now() - 5m GROUP BY time(5m), host
    ... |> group(by: ["_measurement", "host"]) |> window(every: 5m)

If the `GROUP BY time(...)` doesn't exist, `window()` is skipped. If there is no `GROUP BY` clause, it always groups by `_measurement`. If a wildcard is used for grouping, then this step is skipped.

### <a name="evaluate-function"></a> Evaluate the function

If this group contains a function call, the function is evaluated at this stage and invoked on the specific column. As an example:

    > SELECT max(usage_user), usage_system FROM telegraf..cpu
    val1 = create_cursor(db: "telegraf", start: -5m, m: "cpu", f: "usage_user")
    val1 = create_cursor(db: "telegraf", start: -5m, m: "cpu", f: "usage_system")
    inner_join(tables: {val1: val1, val2: val2}, except: ["_field"], fn: (tables) => {val1: tables.val1, val2: tables.val2})
        |> max(column: "val1")

For an aggregate, the following is used instead:

    > SELECT mean(usage_user) FROM telegraf..cpu
    create_cursor(db: "telegraf", start: -5m, m: "cpu", f: "usage_user")
        |> group(except: ["_field"])
        |> mean(timeSrc: "_start", columns: ["_value"])

If the aggregate is combined with conditions, the column name of `_value` is replaced with whatever the generated column name is.

## <a name="join-groups"></a> Join the groups

If there is only one group, this does not need to be done and can be skipped.

If there are multiple groups, as is the case when there are multiple function calls, then we perform an `outer_join` using the time and any remaining partition keys.

## <a name="map-and-eval"></a> Map and eval the columns

After joining the results if a join was required, then a `map` call is used to both evaluate the math functions and name the columns.

    result |> map(fn: (r) => {max: r.val1, usage_system: r.val2})

This is the final result. It will also include any tags in the partition key and the time will be located in the `_time` variable.

**TODO(jsternberg):** Find a way for a column to be both used as a tag and a field. This is not currently possible because the encoder can't tell the difference between the two.
