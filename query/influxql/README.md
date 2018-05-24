# InfluxQL Transpiler

The InfluxQL Transpiler exists to rewrite an InfluxQL query into its equivalent query in IFQL. The transpiler works off of a few simple rules that match with the equivalent method of constructing queries in InfluxDB.

## Identify the variables

The InfluxQL query engine works by filling in variables and evaluating the query for the values in each row. The first step of transforming a query is identifying the variables so we can figure out how to fill them correctly. A variable is any point in the query that has a **variable or a function call**. Math functions do not count as function calls and are handled in the eval phase.

For the following query, it is easy to identify the points:

    SELECT max(usage_user), usage_system FROM telegraf..cpu

`max(usage_user)` and `usage_system` are the variables that we need to fill in for each row.

## Create the iterators/cursors

Each variable has a separate cursor created. We generate a separate `from(db: ...)` query chain for each of the variables above and then join them together at the end. The above would generate something like this:

    max = from(db: "telegraf")
        |> range(...)
        |> filter(fn: (r) => r._measurement == "cpu" and r._field == "usage_user")
        |> group()
        |> max()
    usage_system = from(db: "telegraf")
        |> range(...)
        |> filter(fn: (r) => r._measurement == "cpu" and r._field == "usage_user")

These are the primary entry points that retrieve data. Aggregates and selectors will include the group by parameters in the `group()` section of the chain. If `GROUP BY *` is used, then `group()` will be excluded. If a `GROUP BY time(...)` is used, then `window()` is used to window the results.

## Join the iterators/cursors

Each of the iterators/cursors created above will be joined together into a single stream. Depending on the type of query, this may be an `outer_join` or `left_join`. If the query is an aggregate or a raw query, an outer join is used. If the query is a selector, a left join is used using the selector as the leftmost column. This way, any of the points that are not selected will be dropped.

As an example:

    // TODO(jsternberg): Figure out the real command that makes this work
    // since it doesn't exist yet.
    result = left_join(tables: {val1: max, val2: usage_system}, by: "val1", fn: (tables) => {val1: val1, val2: val2})

The join is skipped when there is only one cursor. Replace `left_join` with `outer_join` if this is an aggregate or raw query.

**TODO:** It needs to be resolved how this works because the necessary tags for join to work get stripped in the create cursors/iterators step.

## Map and eval the columns

After joining the results if a join was required, then a `map` call is used to both evaluate the math functions and name the columns.

    result |> map(fn: (r) => {max: r.val1, usage_system: r.val2})

This is the final result.

## Evaluating conditions

Conditions are evaluated by inspecting the condition and using a filter. When inspecting the condition, it looks for all variable references and identifies which ones are tags and which are fields. For a query that does not have any extra fields (only tags), a filter gets added like this:

    > SELECT max(usage_user) FROM telegraf..cpu WHERE host = 'server01'
    max = from(db: "telegraf")
        |> range(...)
        |> filter(fn: (r) => r._measurement == "cpu" and r._field == "usage_user")
        |> filter(fn: (r) => r.host == 'server01')

If the condition includes the primary field that is selected, the filter will use `_value`.

    > SELECT max(usage_user) FROM telegraf..cpu WHERE usage_user < 100
    max = from(db: "telegraf")
        |> range(...)
        |> filter(fn: (r) => r._measurement == "cpu" and r._field == "usage_user")
        |> filter(fn: (r) => r._value < 100)

If there are any variables that are fields and are not the primary field being selected, they will be selected separately and joined.

    > SELECT max(usage_user) FROM telegraf..cpu WHERE usage_system < 20
    val1 = from(db: "telegraf")
        |> range(...)
        |> filter(fn: (r) => r._measurement == "cpu" and r._field == "usage_user")
    val2 = from(db: "telegraf")
        |> range(...)
        |> filter(fn: (r) => r._measurement == "cpu" and r._field == "usage_system")
    max = left_join(tables: {val1: val1, val2: val2}, by: "val1", fn: (tables) => {val1: val1, val2: val2})
        |> filter(fn: (r) => r.val2 < 20)
        |> map(fn: (r) => {_value: r.val1})

The created cursor is then used to map the results.

## Yielding Results

Each result is yielded with the statement id as the name.

    > SELECT max(usage_user) FROM telegraf..cpu
    ... |> yield(name: "0")

Successive commands will increment the name used by yield so that results can be ordered correctly when encoding the result.
