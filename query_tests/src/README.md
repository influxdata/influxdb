This crate contains "integration" tests for the query
engine. Specifically, it runs queries against a fully created `Db`
instance, records the output, and compares it to expected output.

This crate does not (yet) run queries against a fully functioning
server (aka run of `influxdb_iox` binary) but that may be added at
some future time.

Some tests simply have their inputs and outputs hard coded into
`#[test]` annotated tests as is rust's norm

The tests in `src/runner` are driven somewhat more dynamically based on input files


# Cookbook: Adding a new Test

How do you make a new test:
1. Add a new file .sql to the `cases/in` directory
2. Run the tests `` cargo test -p query_tests`
3. You will get a failure message that contains examples of how to update the files


## Example output
Possibly helpful commands:
  # See diff
  diff -du "/Users/alamb/Software/influxdb_iox/query_tests/cases/in/pushdown.expected" "/Users/alamb/Software/influxdb_iox/query_tests/cases/out/pushdown.out"
  # Update expected
  cp -f "/Users/alamb/Software/influxdb_iox/query_tests/cases/in/pushdown.out" "/Users/alamb/Software/influxdb_iox/query_tests/cases/out/pushdown.expected"

# Cookbook: Adding a new test scenario

Each test can be defined in terms of a "setup" (a set of actions taken to prepare the state of database)

In the future we envision more fine grained control of these setups (by implementing some of the database commands as IOX_TEST commands) but for now they are hard coded.

The SQL files refer to the setups with a specially formatted comment:

```sql
-- IOX_SETUP: OneMeasurementFourChunksWithDuplicates
```

To add a new setup, follow the pattern in scenario.rs of `get_all_setups`;
