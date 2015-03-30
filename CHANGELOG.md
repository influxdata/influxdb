## v0.9.0-rc18 [Unreleased]

### Bugfixes
- [#2100](https://github.com/influxdb/influxdb/pull/2100): Use channel to synchronize collectd shutdown.
- [#2100](https://github.com/influxdb/influxdb/pull/2100): Synchronize access to shard index.

## v0.9.0-rc17 [2015-03-29]

### Features
- [#2076](https://github.com/influxdb/influxdb/pull/2076): Separate stdout and stderr output in init.d script
- [#2091](https://github.com/influxdb/influxdb/pull/2091): Support disabling snapshot endpoint.
- [#2081](https://github.com/influxdb/influxdb/pull/2081): Support writing diagnostic data into the internal database.
- [#2095](https://github.com/influxdb/influxdb/pull/2095): Improved InfluxDB client docs. Thanks @derailed

### Bugfixes
- [#2093](https://github.com/influxdb/influxdb/pull/2093): Point precision not marshalled correctly. Thanks @derailed
- [#2084](https://github.com/influxdb/influxdb/pull/2084): Allowing leading underscores in identifiers.
- [#2080](https://github.com/influxdb/influxdb/pull/2080): Graphite logs in seconds, not milliseconds.
- [#2101](https://github.com/influxdb/influxdb/pull/2101): SHOW DATABASES should name returned series "databases".
- [#2104](https://github.com/influxdb/influxdb/pull/2104): Include NEQ when calculating field filters.
- [#2112](https://github.com/influxdb/influxdb/pull/2112): Set GOMAXPROCS on startup. This may have been causing extra leader elections, which would cause a number of other bugs or instability.
- [#2111](https://github.com/influxdb/influxdb/pull/2111) and [#2025](https://github.com/influxdb/influxdb/issues/2025): Raft stability fixes. Non-contiguous log error and others.
- [#2114](https://github.com/influxdb/influxdb/pull/2114): Correctly start influxd on platforms without start-stop-daemon.

## v0.9.0-rc16 [2015-03-24]

### Features
- [#2058](https://github.com/influxdb/influxdb/pull/2058): Track number of queries executed in stats.
- [#2059](https://github.com/influxdb/influxdb/pull/2059): Retention policies sorted by name on return to client.
- [#2061](https://github.com/influxdb/influxdb/pull/2061): Implement SHOW DIAGNOSTICS.
- [#2064](https://github.com/influxdb/influxdb/pull/2064): Allow init.d script to return influxd version.
- [#2053](https://github.com/influxdb/influxdb/pull/2053): Implment backup and restore.
- [#1631](https://github.com/influxdb/influxdb/pull/1631): Wire up DROP CONTINUOUS QUERY.

### Bugfixes
- [#2037](https://github.com/influxdb/influxdb/pull/2037): Don't check 'configExists' at Run() level.
- [#2039](https://github.com/influxdb/influxdb/pull/2039): Don't panic if getting current user fails.
- [#2034](https://github.com/influxdb/influxdb/pull/2034): GROUP BY should require an aggregate.
- [#2040](https://github.com/influxdb/influxdb/pull/2040): Add missing top-level help for config command.
- [#2057](https://github.com/influxdb/influxdb/pull/2057): Move racy "in order" test to integration test suite.
- [#2060](https://github.com/influxdb/influxdb/pull/2060): Reload server shard map on restart.
- [#2068](https://github.com/influxdb/influxdb/pull/2068): Fix misspelled JSON field.
- [#2067](https://github.com/influxdb/influxdb/pull/2067): Fixed issue where some queries didn't properly pull back data (introduced in RC15). Fixing intervals for GROUP BY.

## v0.9.0-rc15 [2015-03-19]

### Features
- [#2000](https://github.com/influxdb/influxdb/pull/2000): Log broker path when broker fails to start. Thanks @gst.
- [#2007](https://github.com/influxdb/influxdb/pull/2007): Track shard-level stats.

### Bugfixes
- [#2001](https://github.com/influxdb/influxdb/pull/2001): Ensure measurement not found returns status code 200.
- [#1985](https://github.com/influxdb/influxdb/pull/1985): Set content-type JSON header before actually writing header. Thanks @dstrek.
- [#2003](https://github.com/influxdb/influxdb/pull/2003): Set timestamp when writing monitoring stats.
- [#2004](https://github.com/influxdb/influxdb/pull/2004): Limit group by to MaxGroupByPoints (currently 100,000).
- [#2016](https://github.com/influxdb/influxdb/pull/2016): Fixing bucket alignment for group by. Thanks @jnutzmann
- [#2021](https://github.com/influxdb/influxdb/pull/2021): Remove unnecessary formatting from log message. Thanks @simonkern


## v0.9.0-rc14 [2015-03-18]

### Bugfixes
- [#1999](https://github.com/influxdb/influxdb/pull/1999): Return status code 200 for measurement not found errors on show series.

## v0.9.0-rc13 [2015-03-17]

### Features
- [#1974](https://github.com/influxdb/influxdb/pull/1974): Add time taken for request to the http server logs.

### Bugfixes
- [#1971](https://github.com/influxdb/influxdb/pull/1971): Fix leader id initialization.
- [#1975](https://github.com/influxdb/influxdb/pull/1975): Require `q` parameter for query endpoint.
- [#1969](https://github.com/influxdb/influxdb/pull/1969): Print loaded config.
- [#1987](https://github.com/influxdb/influxdb/pull/1987): Fix config print startup statement for when no config is provided.
- [#1990](https://github.com/influxdb/influxdb/pull/1990): Drop measurement was taking too long due to transactions.

## v0.9.0-rc12 [2015-03-15]

### Bugfixes
- [#1942](https://github.com/influxdb/influxdb/pull/1942): Sort wildcard names.
- [#1957](https://github.com/influxdb/influxdb/pull/1957): Graphite numbers are always float64.
- [#1955](https://github.com/influxdb/influxdb/pull/1955): Prohibit creation of databases with no name. Thanks @dullgiulio
- [#1952](https://github.com/influxdb/influxdb/pull/1952): Handle delete statement with an error. Thanks again to @dullgiulio

### Features
- [#1935](https://github.com/influxdb/influxdb/pull/1935): Implement stateless broker for Raft.
- [#1936](https://github.com/influxdb/influxdb/pull/1936): Implement "SHOW STATS" and self-monitoring

### Features
- [#1909](https://github.com/influxdb/influxdb/pull/1909): Implement a dump command.

## v0.9.0-rc11 [2015-03-13]

### Bugfixes
- [#1917](https://github.com/influxdb/influxdb/pull/1902): Creating Infinite Retention Policy Failed.
- [#1758](https://github.com/influxdb/influxdb/pull/1758): Add Graphite Integration Test.
- [#1929](https://github.com/influxdb/influxdb/pull/1929): Default Retention Policy incorrectly auto created.
- [#1930](https://github.com/influxdb/influxdb/pull/1930): Auto create database for graphite if not specified.
- [#1908](https://github.com/influxdb/influxdb/pull/1908): Cosmetic CLI output fixes.
- [#1931](https://github.com/influxdb/influxdb/pull/1931): Add default column to SHOW RETENTION POLICIES.
- [#1937](https://github.com/influxdb/influxdb/pull/1937): OFFSET should be allowed to be 0.

### Features
- [#1902](https://github.com/influxdb/influxdb/pull/1902): Enforce retention policies to have a minimum duration.
- [#1906](https://github.com/influxdb/influxdb/pull/1906): Add show servers to query language.
- [#1925](https://github.com/influxdb/influxdb/pull/1925): Add `fill(none)`, `fill(previous)`, and `fill(<num>)` to queries.

## v0.9.0-rc10 [2015-03-09]

### Bugfixes
- [#1867](https://github.com/influxdb/influxdb/pull/1867): Fix race accessing topic replicas map
- [#1864](https://github.com/influxdb/influxdb/pull/1864): fix race in startStateLoop
- [#1753](https://github.com/influxdb/influxdb/pull/1874): Do Not Panic on Missing Dirs
- [#1877](https://github.com/influxdb/influxdb/pull/1877): Broker clients track broker leader
- [#1862](https://github.com/influxdb/influxdb/pull/1862): Fix memory leak in `httpd.serveWait`. Thanks @mountkin
- [#1883](https://github.com/influxdb/influxdb/pull/1883): RLock server during retention policy enforcement. Thanks @grisha
- [#1868](https://github.com/influxdb/influxdb/pull/1868): Use `BatchPoints` for `client.Write` method. Thanks @vladlopes, @georgmu, @d2g, @evanphx, @akolosov.
- [#1881](https://github.com/influxdb/influxdb/pull/1881): Update documentation for `client` package.  Misc library tweaks.
- Fix queries with multiple where clauses on tags, times and fields. Fix queries that have where clauses on fields not in the select

### Features
- [#1875](https://github.com/influxdb/influxdb/pull/1875): Support trace logging of Raft.
- [#1895](https://github.com/influxdb/influxdb/pull/1895): Auto-create a retention policy when a database is created.
- [#1897](https://github.com/influxdb/influxdb/pull/1897): Pre-create shard groups.
- [#1900](https://github.com/influxdb/influxdb/pull/1900): Change `LIMIT` to `SLIMIT` and implement `LIMIT` and `OFFSET`

## v0.9.0-rc9 [2015-03-06]

### Bugfixes
- [#1872](https://github.com/influxdb/influxdb/pull/1872): Fix "stale term" errors with raft

## v0.9.0-rc8 [2015-03-05]

### Bugfixes
- [#1836](https://github.com/influxdb/influxdb/pull/1836): Store each parsed shell command in history file.
- [#1789](https://github.com/influxdb/influxdb/pull/1789): add --config-files option to fpm command. Thanks @kylezh
- [#1859](https://github.com/influxdb/influxdb/pull/1859): Queries with a `GROUP BY *` clause were returning a 500 if done against a measurement that didn't exist

### Features
- [#1755](https://github.com/influxdb/influxdb/pull/1848): Support JSON data ingest over UDP
- [#1857](https://github.com/influxdb/influxdb/pull/1857): Support retention policies with infinite duration
- [#1858](https://github.com/influxdb/influxdb/pull/1858): Enable detailed tracing of write path

## v0.9.0-rc7 [2015-03-02]

### Features
- [#1813](https://github.com/influxdb/influxdb/pull/1813): Queries for missing measurements or fields now return a 200 with an error message in the series JSON.
- [#1826](https://github.com/influxdb/influxdb/pull/1826), [#1827](https://github.com/influxdb/influxdb/pull/1827): Fixed queries with `WHERE` clauses against fields.

### Bugfixes

- [#1744](https://github.com/influxdb/influxdb/pull/1744): Allow retention policies to be modified without specifying replication factor. Thanks @kylezh
- [#1809](https://github.com/influxdb/influxdb/pull/1809): Packaging post-install script unconditionally removes init.d symlink. Thanks @sineos

## v0.9.0-rc6 [2015-02-27]

### Bugfixes

- [#1780](https://github.com/influxdb/influxdb/pull/1780): Malformed identifiers get through the parser
- [#1775](https://github.com/influxdb/influxdb/pull/1775): Panic "index out of range" on some queries
- [#1744](https://github.com/influxdb/influxdb/pull/1744): Select shard groups which completely encompass time range. Thanks @kylezh.

## v0.9.0-rc5 [2015-02-27]

### Bugfixes

- [#1752](https://github.com/influxdb/influxdb/pull/1752): remove debug log output from collectd.
- [#1720](https://github.com/influxdb/influxdb/pull/1720): Parse Series IDs as unsigned 32-bits.
- [#1767](https://github.com/influxdb/influxdb/pull/1767): Drop Series was failing across shards.  Issue #1761.
- [#1773](https://github.com/influxdb/influxdb/pull/1773): Fix bug when merging series together that have unequal number of points in a group by interval
- [#1771](https://github.com/influxdb/influxdb/pull/1771): Make `SHOW SERIES` return IDs and support `LIMIT` and `OFFSET`

### Features

- [#1698](https://github.com/influxdb/influxdb/pull/1698): Wire up DROP MEASUREMENT

## v0.9.0-rc4 [2015-02-24]

### Bugfixes

- Fix authentication issue with continuous queries
- Print version in the log on startup

## v0.9.0-rc3 [2015-02-23]

### Features

- [#1659](https://github.com/influxdb/influxdb/pull/1659): WHERE against regexes: `WHERE =~ '.*asdf'
- [#1580](https://github.com/influxdb/influxdb/pull/1580): Add support for fields with bool, int, or string data types
- [#1687](https://github.com/influxdb/influxdb/pull/1687): Change `Rows` to `Series` in results output. BREAKING API CHANGE
- [#1629](https://github.com/influxdb/influxdb/pull/1629): Add support for `DROP SERIES` queries
- [#1632](https://github.com/influxdb/influxdb/pull/1632): Add support for `GROUP BY *` to return all series within a measurement
- [#1689](https://github.com/influxdb/influxdb/pull/1689): Change `SHOW TAG VALUES WITH KEY="foo"` to use the key name in the result. BREAKING API CHANGE
- [#1699](https://github.com/influxdb/influxdb/pull/1699): Add CPU and memory profiling options to daemon
- [#1672](https://github.com/influxdb/influxdb/pull/1672): Add index tracking to metastore. Makes downed node recovery actually work
- [#1591](https://github.com/influxdb/influxdb/pull/1591): Add `spread` aggregate function
- [#1576](https://github.com/influxdb/influxdb/pull/1576): Add `first` and `last` aggregate functions
- [#1573](https://github.com/influxdb/influxdb/pull/1573): Add `stddev` aggregate function
- [#1565](https://github.com/influxdb/influxdb/pull/1565): Add the admin interface back into the server and update for new API
- [#1562](https://github.com/influxdb/influxdb/pull/1562): Enforce retention policies
- [#1700](https://github.com/influxdb/influxdb/pull/1700): Change `Values` to `Fields` on writes.  BREAKING API CHANGE
- [#1706](https://github.com/influxdb/influxdb/pull/1706): Add support for `LIMIT` and `OFFSET`, which work on the number of series returned in a query. To limit the number of data points use a `WHERE time` clause

### Bugfixes

- [#1636](https://github.com/influxdb/influxdb/issues/1636): Don't store number of fields in raw data. THIS IS A BREAKING DATA CHANGE. YOU MUST START WITH A FRESH DATABASE
- [#1701](https://github.com/influxdb/influxdb/pull/1701), [#1667](https://github.com/influxdb/influxdb/pull/1667), [#1663](https://github.com/influxdb/influxdb/pull/1663), [#1615](https://github.com/influxdb/influxdb/pull/1615): Raft fixes
- [#1644](https://github.com/influxdb/influxdb/pull/1644): Add batching support for significantly improved write performance
- [#1704](https://github.com/influxdb/influxdb/pull/1704): Fix queries that pull back raw data (i.e. ones without aggregate functions)
- [#1718](https://github.com/influxdb/influxdb/pull/1718): Return an error on write if any of the points are don't have at least one field
- [#1806](https://github.com/influxdb/influxdb/pull/1806): Fix regex parsing.  Change regex syntax to use / delimiters.


## v0.9.0-rc1,2 [no public release]

### Features

- Support for tags added
- New queries for showing measurement names, tag keys, and tag values
- Renamed shard spaces to retention policies
- Deprecated matching against regex in favor of explicit writing and querying on retention policies
- Pure Go InfluxQL parser
- Switch to BoltDB as underlying datastore
- BoltDB backed metastore to store schema information
- Updated HTTP API to only have two endpoints `/query` and `/write`
- Added all administrative functions to the query language
- Change cluster architecture to have brokers and data nodes
- Switch to streaming Raft implementation
- In memory inverted index of the tag data
- Pure Go implementation!

## v0.8.6 [2014-11-15]

### Features

- [Issue #973](https://github.com/influxdb/influxdb/issues/973). Support
  joining using a regex or list of time series
- [Issue #1068](https://github.com/influxdb/influxdb/issues/1068). Print
  the processor chain when the query is started

### Bugfixes

- [Issue #584](https://github.com/influxdb/influxdb/issues/584). Don't
  panic if the process died while initializing
- [Issue #663](https://github.com/influxdb/influxdb/issues/663). Make
  sure all sub servies are closed when are stopping InfluxDB
- [Issue #671](https://github.com/influxdb/influxdb/issues/671). Fix
  the Makefile package target for Mac OSX
- [Issue #800](https://github.com/influxdb/influxdb/issues/800). Use
  su instead of sudo in the init script. This fixes the startup problem
  on RHEL 6.
- [Issue #925](https://github.com/influxdb/influxdb/issues/925). Don't
  generate invalid query strings for single point queries
- [Issue #943](https://github.com/influxdb/influxdb/issues/943). Don't
  take two snapshots at the same time
- [Issue #947](https://github.com/influxdb/influxdb/issues/947). Exit
  nicely if the daemon doesn't have permission to write to the log.
- [Issue #959](https://github.com/influxdb/influxdb/issues/959). Stop using
  closed connections in the protobuf client.
- [Issue #978](https://github.com/influxdb/influxdb/issues/978). Check
  for valgrind and mercurial in the configure script
- [Issue #996](https://github.com/influxdb/influxdb/issues/996). Fill should
  fill the time range even if no points exists in the given time range
- [Issue #1008](https://github.com/influxdb/influxdb/issues/1008). Return
  an appropriate exit status code depending on whether the process exits
  due to an error or exits gracefully.
- [Issue #1024](https://github.com/influxdb/influxdb/issues/1024). Hitting
  open files limit causes influxdb to create shards in loop.
- [Issue #1069](https://github.com/influxdb/influxdb/issues/1069). Fix
  deprecated interface endpoint in Admin UI.
- [Issue #1076](https://github.com/influxdb/influxdb/issues/1076). Fix
  the timestamps of data points written by the collectd plugin. (Thanks,
  @renchap for reporting this bug)
- [Issue #1078](https://github.com/influxdb/influxdb/issues/1078). Make sure
  we don't resurrect shard directories for shards that have already expired
- [Issue #1085](https://github.com/influxdb/influxdb/issues/1085). Set
  the connection string of the local raft node
- [Issue #1092](https://github.com/influxdb/influxdb/issues/1093). Set
  the connection string of the local node in the raft snapshot.
- [Issue #1100](https://github.com/influxdb/influxdb/issues/1100). Removing
  a non-existent shard space causes the cluster to panic.
- [Issue #1113](https://github.com/influxdb/influxdb/issues/1113). A nil
  engine.ProcessorChain causes a panic.

## v0.8.5 [2014-10-27]

### Features

- [Issue #1055](https://github.com/influxdb/influxdb/issues/1055). Allow
  graphite and collectd input plugins to have separate binding address

### Bugfixes

- [Issue #1058](https://github.com/influxdb/influxdb/issues/1058). Use
  the query language instead of the continuous query endpoints that
  were removed in 0.8.4
- [Issue #1022](https://github.com/influxdb/influxdb/issues/1022). Return
  an +Inf or NaN instead of panicing when we encounter a divide by zero
- [Issue #821](https://github.com/influxdb/influxdb/issues/821). Don't
  scan through points when we hit the limit
- [Issue #1051](https://github.com/influxdb/influxdb/issues/1051). Fix
  timestamps when the collectd is used and low resolution timestamps
  is set.

## v0.8.4 [2014-10-24]

### Bugfixes

- Remove the continuous query api endpoints since the query language
  has all the features needed to list and delete continuous queries.
- [Issue #778](https://github.com/influxdb/influxdb/issues/778). Selecting
  from a non-existent series should give a better error message indicating
  that the series doesn't exist
- [Issue #988](https://github.com/influxdb/influxdb/issues/988). Check
  the arguments of `top()` and `bottom()`
- [Issue #1021](https://github.com/influxdb/influxdb/issues/1021). Make
  redirecting to standard output and standard error optional instead of
  going to `/dev/null`. This can now be configured by setting `$STDOUT`
  in `/etc/default/influxdb`
- [Issue #985](https://github.com/influxdb/influxdb/issues/985). Make
  sure we drop a shard only when there's no one using it. Otherwise, the
  shard can be closed when another goroutine is writing to it which will
  cause random errors and possibly corruption of the database.

### Features

- [Issue #1047](https://github.com/influxdb/influxdb/issues/1047). Allow
  merge() to take a list of series (as opposed to a regex in #72)

## v0.8.4-rc.1 [2014-10-21]

### Bugfixes

- [Issue #1040](https://github.com/influxdb/influxdb/issues/1040). Revert
  to older raft snapshot if the latest one is corrupted
- [Issue #1004](https://github.com/influxdb/influxdb/issues/1004). Querying
  for data outside of existing shards returns an empty response instead of
  throwing a `Couldn't lookup columns` error
- [Issue #1020](https://github.com/influxdb/influxdb/issues/1020). Change
  init script exit codes to conform to the lsb standards. (Thanks, @spuder)
- [Issue #1011](https://github.com/influxdb/influxdb/issues/1011). Fix
  the tarball for homebrew so that rocksdb is included and the directory
  structure is clean
- [Issue #1007](https://github.com/influxdb/influxdb/issues/1007). Fix
  the content type when an error occurs and the client requests
  compression.
- [Issue #916](https://github.com/influxdb/influxdb/issues/916). Set
  the ulimit in the init script with a way to override the limit
- [Issue #742](https://github.com/influxdb/influxdb/issues/742). Fix
  rocksdb for Mac OSX
- [Issue #387](https://github.com/influxdb/influxdb/issues/387). Aggregations
  with group by time(1w), time(1m) and time(1y) (for week, month and
  year respectively) will cause the start time and end time of the bucket
  to fall on the logical boundaries of the week, month or year.
- [Issue #334](https://github.com/influxdb/influxdb/issues/334). Derivative
  for queries with group by time() and fill(), will take the difference
  between the first value in the bucket and the first value of the next
  bucket.
- [Issue #972](https://github.com/influxdb/influxdb/issues/972). Don't
  assign duplicate server ids

### Features

- [Issue #722](https://github.com/influxdb/influxdb/issues/722). Add
  an install target to the Makefile
- [Issue #1032](https://github.com/influxdb/influxdb/issues/1032). Include
  the admin ui static assets in the binary
- [Issue #1019](https://github.com/influxdb/influxdb/issues/1019). Upgrade
  to rocksdb 3.5.1
- [Issue #992](https://github.com/influxdb/influxdb/issues/992). Add
  an input plugin for collectd. (Thanks, @kimor79)
- [Issue #72](https://github.com/influxdb/influxdb/issues/72). Support merge
  for multiple series using regex syntax

## v0.8.3 [2014-09-24]

### Bugfixes

- [Issue #885](https://github.com/influxdb/influxdb/issues/885). Multiple
  queries separated by semicolons work as expected. Queries are process
  sequentially
- [Issue #652](https://github.com/influxdb/influxdb/issues/652). Return an
  error if an invalid column is used in the where clause
- [Issue #794](https://github.com/influxdb/influxdb/issues/794). Fix case
  insensitive regex matching
- [Issue #853](https://github.com/influxdb/influxdb/issues/853). Move
  cluster config from raft to API.
- [Issue #714](https://github.com/influxdb/influxdb/issues/714). Don't
  panic on invalid boolean operators.
- [Issue #843](https://github.com/influxdb/influxdb/issues/843). Prevent blank database names
- [Issue #780](https://github.com/influxdb/influxdb/issues/780). Fix
  fill() for all aggregators
- [Issue #923](https://github.com/influxdb/influxdb/issues/923). Enclose
  table names in double quotes in the result of GetQueryString()
- [Issue #923](https://github.com/influxdb/influxdb/issues/923). Enclose
  table names in double quotes in the result of GetQueryString()
- [Issue #967](https://github.com/influxdb/influxdb/issues/967). Return an
  error if the storage engine can't be created
- [Issue #954](https://github.com/influxdb/influxdb/issues/954). Don't automatically
  create shards which was causing too many shards to be created when used with
  grafana
- [Issue #939](https://github.com/influxdb/influxdb/issues/939). Aggregation should
  ignore null values and invalid values, e.g. strings with mean().
- [Issue #964](https://github.com/influxdb/influxdb/issues/964). Parse
  big int in queries properly.

## v0.8.2 [2014-09-05]

### Bugfixes

- [Issue #886](https://github.com/influxdb/influxdb/issues/886). Update shard space to not set defaults

- [Issue #867](https://github.com/influxdb/influxdb/issues/867). Add option to return shard space mappings in list series

### Bugfixes

- [Issue #652](https://github.com/influxdb/influxdb/issues/652). Return
  a meaningful error if an invalid column is used in where clause
  after joining multiple series

## v0.8.2 [2014-09-08]

### Features

- Added API endpoint to update shard space definitions

### Bugfixes

- [Issue #886](https://github.com/influxdb/influxdb/issues/886). Shard space regexes reset after restart of InfluxDB

## v0.8.1 [2014-09-03]

- [Issue #896](https://github.com/influxdb/influxdb/issues/896). Allow logging to syslog. Thanks @malthe

### Bugfixes

- [Issue #868](https://github.com/influxdb/influxdb/issues/868). Don't panic when upgrading a snapshot from 0.7.x
- [Issue #887](https://github.com/influxdb/influxdb/issues/887). The first continuous query shouldn't trigger backfill if it had backfill disabled
- [Issue #674](https://github.com/influxdb/influxdb/issues/674). Graceful exit when config file is invalid. (Thanks, @DavidBord)
- [Issue #857](https://github.com/influxdb/influxdb/issues/857). More informative list servers api. (Thanks, @oliveagle)

## v0.8.0 [2014-08-22]

### Features

- [Issue #850](https://github.com/influxdb/influxdb/issues/850). Makes the server listing more informative

### Bugfixes

- [Issue #779](https://github.com/influxdb/influxdb/issues/779). Deleting expired shards isn't thread safe.
- [Issue #860](https://github.com/influxdb/influxdb/issues/860). Load database config should validate shard spaces.
- [Issue #862](https://github.com/influxdb/influxdb/issues/862). Data migrator should have option to set delay time.

## v0.8.0-rc.5 [2014-08-15]

### Features

- [Issue #376](https://github.com/influxdb/influxdb/issues/376). List series should support regex filtering
- [Issue #745](https://github.com/influxdb/influxdb/issues/745). Add continuous queries to the database config
- [Issue #746](https://github.com/influxdb/influxdb/issues/746). Add data migration tool for 0.8.0

### Bugfixes

- [Issue #426](https://github.com/influxdb/influxdb/issues/426). Fill should fill the entire time range that is requested
- [Issue #740](https://github.com/influxdb/influxdb/issues/740). Don't emit non existent fields when joining series with different fields
- [Issue #744](https://github.com/influxdb/influxdb/issues/744). Admin site should have all assets locally
- [Issue #767](https://github.com/influxdb/influxdb/issues/768). Remove shards whenever they expire
- [Issue #781](https://github.com/influxdb/influxdb/issues/781). Don't emit non existent fields when joining series with different fields
- [Issue #791](https://github.com/influxdb/influxdb/issues/791). Move database config loader to be an API endpoint
- [Issue #809](https://github.com/influxdb/influxdb/issues/809). Migration path from 0.7 -> 0.8
- [Issue #811](https://github.com/influxdb/influxdb/issues/811). Gogoprotobuf removed `ErrWrongType`, which is depended on by Raft
- [Issue #820](https://github.com/influxdb/influxdb/issues/820). Query non-local shard with time range to avoid getting back points not in time range
- [Issue #827](https://github.com/influxdb/influxdb/issues/827). Don't leak file descriptors in the WAL
- [Issue #830](https://github.com/influxdb/influxdb/issues/830). List series should return series in lexicographic sorted order
- [Issue #831](https://github.com/influxdb/influxdb/issues/831). Move create shard space to be db specific

## v0.8.0-rc.4 [2014-07-29]

### Bugfixes

- [Issue #774](https://github.com/influxdb/influxdb/issues/774). Don't try to parse "inf" shard retention policy
- [Issue #769](https://github.com/influxdb/influxdb/issues/769). Use retention duration when determining expired shards. (Thanks, @shugo)
- [Issue #736](https://github.com/influxdb/influxdb/issues/736). Only db admins should be able to drop a series
- [Issue #713](https://github.com/influxdb/influxdb/issues/713). Null should be a valid fill value
- [Issue #644](https://github.com/influxdb/influxdb/issues/644). Graphite api should write data in batches to the coordinator
- [Issue #740](https://github.com/influxdb/influxdb/issues/740). Panic when distinct fields are selected from an inner join
- [Issue #781](https://github.com/influxdb/influxdb/issues/781). Panic when distinct fields are added after an inner join

## v0.8.0-rc.3 [2014-07-21]

### Bugfixes

- [Issue #752](https://github.com/influxdb/influxdb/issues/752). `./configure` should use goroot to find gofmt
- [Issue #758](https://github.com/influxdb/influxdb/issues/758). Clarify the reason behind graphite input plugin not starting. (Thanks, @otoolep)
- [Issue #759](https://github.com/influxdb/influxdb/issues/759). Don't revert the regex in the shard space. (Thanks, @shugo)
- [Issue #760](https://github.com/influxdb/influxdb/issues/760). Removing a server should remove it from the shard server ids. (Thanks, @shugo)
- [Issue #772](https://github.com/influxdb/influxdb/issues/772). Add sentinel values to all db. This caused the last key in the db to not be fetched properly.


## v0.8.0-rc.2 [2014-07-15]

- This release is to fix a build error in rc1 which caused rocksdb to not be available
- Bump up the `max-open-files` option to 1000 on all storage engines
- Lower the `write-buffer-size` to 1000

## v0.8.0-rc.1 [2014-07-15]

### Features

- [Issue #643](https://github.com/influxdb/influxdb/issues/643). Support pretty print json. (Thanks, @otoolep)
- [Issue #641](https://github.com/influxdb/influxdb/issues/641). Support multiple storage engines
- [Issue #665](https://github.com/influxdb/influxdb/issues/665). Make build tmp directory configurable in the make file. (Thanks, @dgnorton)
- [Issue #667](https://github.com/influxdb/influxdb/issues/667). Enable compression on all GET requests and when writing data
- [Issue #648](https://github.com/influxdb/influxdb/issues/648). Return permissions when listing db users. (Thanks, @nicolai86)
- [Issue #682](https://github.com/influxdb/influxdb/issues/682). Allow continuous queries to run without backfill (Thanks, @dhammika)
- [Issue #689](https://github.com/influxdb/influxdb/issues/689). **REQUIRES DATA MIGRATION** Move metadata into raft
- [Issue #255](https://github.com/influxdb/influxdb/issues/255). Support millisecond precision using `ms` suffix
- [Issue #95](https://github.com/influxdb/influxdb/issues/95). Drop database should not be synchronous
- [Issue #571](https://github.com/influxdb/influxdb/issues/571). Add support for arbitrary number of shard spaces and retention policies
- Default storage engine changed to RocksDB

### Bugfixes

- [Issue #651](https://github.com/influxdb/influxdb/issues/651). Change permissions of symlink which fix some installation issues. (Thanks, @Dieterbe)
- [Issue #670](https://github.com/influxdb/influxdb/issues/670). Don't warn on missing influxdb user on fresh installs
- [Issue #676](https://github.com/influxdb/influxdb/issues/676). Allow storing high precision integer values without losing any information
- [Issue #695](https://github.com/influxdb/influxdb/issues/695). Prevent having duplicate field names in the write payload. (Thanks, @seunglee150)
- [Issue #731](https://github.com/influxdb/influxdb/issues/731). Don't enable the udp plugin if the `enabled` option is set to false
- [Issue #733](https://github.com/influxdb/influxdb/issues/733). Print an `INFO` message when the input plugin is disabled
- [Issue #707](https://github.com/influxdb/influxdb/issues/707). Graphite input plugin should work payload delimited by any whitespace character
- [Issue #734](https://github.com/influxdb/influxdb/issues/734). Don't buffer non replicated writes
- [Issue #465](https://github.com/influxdb/influxdb/issues/465). Recreating a currently deleting db or series doesn't bring back the old data anymore
- [Issue #358](https://github.com/influxdb/influxdb/issues/358). **BREAKING** List series should return as a single series
- [Issue #499](https://github.com/influxdb/influxdb/issues/499). **BREAKING** Querying non-existent database or series will return an error
- [Issue #570](https://github.com/influxdb/influxdb/issues/570). InfluxDB crashes during delete/drop of database
- [Issue #592](https://github.com/influxdb/influxdb/issues/592). Drop series is inefficient

## v0.7.3 [2014-06-13]

### Bugfixes

- [Issue #637](https://github.com/influxdb/influxdb/issues/637). Truncate log files if the last request wasn't written properly
- [Issue #646](https://github.com/influxdb/influxdb/issues/646). CRITICAL: Duplicate shard ids for new shards if old shards are deleted.

## v0.7.2 [2014-05-30]

### Features

- [Issue #521](https://github.com/influxdb/influxdb/issues/521). MODE works on all datatypes (Thanks, @richthegeek)

### Bugfixes

- [Issue #418](https://github.com/influxdb/influxdb/pull/418). Requests or responses larger than MAX_REQUEST_SIZE break things.
- [Issue #606](https://github.com/influxdb/influxdb/issues/606). InfluxDB will fail to start with invalid permission if log.txt didn't exist
- [Issue #602](https://github.com/influxdb/influxdb/issues/602). Merge will fail to work across shards

### Features

## v0.7.1 [2014-05-29]

### Bugfixes

- [Issue #579](https://github.com/influxdb/influxdb/issues/579). Reject writes to nonexistent databases
- [Issue #597](https://github.com/influxdb/influxdb/issues/597). Force compaction after deleting data

### Features

- [Issue #476](https://github.com/influxdb/influxdb/issues/476). Support ARM architecture
- [Issue #578](https://github.com/influxdb/influxdb/issues/578). Support aliasing for expressions in parenthesis
- [Issue #544](https://github.com/influxdb/influxdb/pull/544). Support forcing node removal from a cluster
- [Issue #591](https://github.com/influxdb/influxdb/pull/591). Support multiple udp input plugins (Thanks, @tpitale)
- [Issue #600](https://github.com/influxdb/influxdb/pull/600). Report version, os, arch, and raftName once per day.

## v0.7.0 [2014-05-23]

### Bugfixes

- [Issue #557](https://github.com/influxdb/influxdb/issues/557). Group by time(1y) doesn't work while time(365d) works
- [Issue #547](https://github.com/influxdb/influxdb/issues/547). Add difference function (Thanks, @mboelstra)
- [Issue #550](https://github.com/influxdb/influxdb/issues/550). Fix tests on 32-bit ARM
- [Issue #524](https://github.com/influxdb/influxdb/issues/524). Arithmetic operators and where conditions don't play nice together
- [Issue #561](https://github.com/influxdb/influxdb/issues/561). Fix missing query in parsing errors
- [Issue #563](https://github.com/influxdb/influxdb/issues/563). Add sample config for graphite over udp
- [Issue #537](https://github.com/influxdb/influxdb/issues/537). Incorrect query syntax causes internal error
- [Issue #565](https://github.com/influxdb/influxdb/issues/565). Empty series names shouldn't cause a panic
- [Issue #575](https://github.com/influxdb/influxdb/issues/575). Single point select doesn't interpret timestamps correctly
- [Issue #576](https://github.com/influxdb/influxdb/issues/576). We shouldn't set timestamps and sequence numbers when listing cq
- [Issue #560](https://github.com/influxdb/influxdb/issues/560). Use /dev/urandom instead of /dev/random
- [Issue #502](https://github.com/influxdb/influxdb/issues/502). Fix a
  race condition in assigning id to db+series+field (Thanks @ohurvitz
  for reporting this bug and providing a script to repro)

### Features

- [Issue #567](https://github.com/influxdb/influxdb/issues/567). Allow selecting from multiple series names by separating them with commas (Thanks, @peekeri)

### Deprecated

- [Issue #460](https://github.com/influxdb/influxdb/issues/460). Don't start automatically after installing
- [Issue #529](https://github.com/influxdb/influxdb/issues/529). Don't run influxdb as root
- [Issue #443](https://github.com/influxdb/influxdb/issues/443). Use `name` instead of `username` when returning cluster admins

## v0.6.5 [2014-05-19]

### Features

- [Issue #551](https://github.com/influxdb/influxdb/issues/551). Add TOP and BOTTOM aggregate functions (Thanks, @chobie)

### Bugfixes

- [Issue #555](https://github.com/influxdb/influxdb/issues/555). Fix a regression introduced in the raft snapshot format

## v0.6.4 [2014-05-16]

### Features

- Make the write batch size configurable (also applies to deletes)
- Optimize writing to multiple series
- [Issue #546](https://github.com/influxdb/influxdb/issues/546). Add UDP support for Graphite API (Thanks, @peekeri)

### Bugfixes

- Fix a bug in shard logic that caused short term shards to be clobbered with long term shards
- [Issue #489](https://github.com/influxdb/influxdb/issues/489). Remove replication factor from CreateDatabase command

## v0.6.3 [2014-05-13]

### Features

- [Issue #505](https://github.com/influxdb/influxdb/issues/505). Return a version header with http the response (Thanks, @majst01)
- [Issue #520](https://github.com/influxdb/influxdb/issues/520). Print the version to the log file

### Bugfixes

- [Issue #516](https://github.com/influxdb/influxdb/issues/516). Close WAL log/index files when they aren't being used
- [Issue #532](https://github.com/influxdb/influxdb/issues/532). Don't log graphite connection EOF as an error
- [Issue #535](https://github.com/influxdb/influxdb/issues/535). WAL Replay hangs if response isn't received
- [Issue #538](https://github.com/influxdb/influxdb/issues/538). Don't panic if the same series existed twice in the request with different columns
- [Issue #536](https://github.com/influxdb/influxdb/issues/536). Joining the cluster after shards are creating shouldn't cause new nodes to panic
- [Issue #539](https://github.com/influxdb/influxdb/issues/539). count(distinct()) with fill shouldn't panic on empty groups
- [Issue #534](https://github.com/influxdb/influxdb/issues/534). Create a new series when interpolating

## v0.6.2 [2014-05-09]

### Bugfixes

- [Issue #511](https://github.com/influxdb/influxdb/issues/511). Don't automatically create the database when a db user is created
- [Issue #512](https://github.com/influxdb/influxdb/issues/512). Group by should respect null values
- [Issue #518](https://github.com/influxdb/influxdb/issues/518). Filter Infinities and NaNs from the returned json
- [Issue #522](https://github.com/influxdb/influxdb/issues/522). Committing requests while replaying caused the WAL to skip some log files
- [Issue #369](https://github.com/influxdb/influxdb/issues/369). Fix some edge cases with WAL recovery

## v0.6.1 [2014-05-06]

### Bugfixes

- [Issue #500](https://github.com/influxdb/influxdb/issues/500). Support `y` suffix in time durations
- [Issue #501](https://github.com/influxdb/influxdb/issues/501). Writes with invalid payload should be rejected
- [Issue #507](https://github.com/influxdb/influxdb/issues/507). New cluster admin passwords don't propagate properly to other nodes in a cluster
- [Issue #508](https://github.com/influxdb/influxdb/issues/508). Don't replay WAL entries for servers with no shards
- [Issue #464](https://github.com/influxdb/influxdb/issues/464). Admin UI shouldn't draw graphs for string columns
- [Issue #480](https://github.com/influxdb/influxdb/issues/480). Large values on the y-axis get cut off

## v0.6.0 [2014-05-02]

### Feature

- [Issue #477](https://github.com/influxdb/influxdb/issues/477). Add a udp json interface (Thanks, Julien Ammous)
- [Issue #491](https://github.com/influxdb/influxdb/issues/491). Make initial root password settable through env variable (Thanks, Edward Muller)

### Bugfixes

- [Issue #469](https://github.com/influxdb/influxdb/issues/469). Drop continuous queries when a database is dropped
- [Issue #431](https://github.com/influxdb/influxdb/issues/431). Don't log to standard output if a log file is specified in the config file
- [Issue #483](https://github.com/influxdb/influxdb/issues/483). Return 409 if a database already exist (Thanks, Edward Muller)
- [Issue #486](https://github.com/influxdb/influxdb/issues/486). Columns used in the target of continuous query shouldn't be inserted in the time series
- [Issue #490](https://github.com/influxdb/influxdb/issues/490). Database user password's cannot be changed (Thanks, Edward Muller)
- [Issue #495](https://github.com/influxdb/influxdb/issues/495). Enforce write permissions properly

## v0.5.12 [2014-04-29]

### Bugfixes

- [Issue #419](https://github.com/influxdb/influxdb/issues/419),[Issue #478](https://github.com/influxdb/influxdb/issues/478). Allow hostname, raft and protobuf ports to be changed, without requiring manual intervention from the user

## v0.5.11 [2014-04-25]

### Features

- [Issue #471](https://github.com/influxdb/influxdb/issues/471). Read and write permissions should be settable through the http api

### Bugfixes

- [Issue #323](https://github.com/influxdb/influxdb/issues/323). Continuous queries should guard against data loops
- [Issue #473](https://github.com/influxdb/influxdb/issues/473). Engine memory optimization

## v0.5.10 [2014-04-22]

### Features

- [Issue #463](https://github.com/influxdb/influxdb/issues/463). Allow series names to use any character (escape by wrapping in double quotes)
- [Issue #447](https://github.com/influxdb/influxdb/issues/447). Allow @ in usernames
- [Issue #466](https://github.com/influxdb/influxdb/issues/466). Allow column names to use any character (escape by wrapping in double quotes)

### Bugfixes

- [Issue #458](https://github.com/influxdb/influxdb/issues/458). Continuous queries with group by time() and a column should insert sequence numbers of 1
- [Issue #457](https://github.com/influxdb/influxdb/issues/457). Deleting series that start with capital letters should work

## v0.5.9 [2014-04-18]

### Bugfixes

- [Issue #446](https://github.com/influxdb/influxdb/issues/446). Check for (de)serialization errors
- [Issue #456](https://github.com/influxdb/influxdb/issues/456). Continuous queries failed if one of the group by columns had null value
- [Issue #455](https://github.com/influxdb/influxdb/issues/455). Comparison operators should ignore null values

## v0.5.8 [2014-04-17]

- Renamed config.toml.sample to config.sample.toml

### Bugfixes

- [Issue #244](https://github.com/influxdb/influxdb/issues/244). Reconstruct the query from the ast
- [Issue #449](https://github.com/influxdb/influxdb/issues/449). Heartbeat timeouts can cause reading from connection to lock up
- [Issue #451](https://github.com/influxdb/influxdb/issues/451). Reduce the aggregation state that is kept in memory so that
  aggregation queries over large periods of time don't take insance amount of memory

## v0.5.7 [2014-04-15]

### Features

- Queries are now logged as INFO in the log file before they run

### Bugfixes

- [Issue #328](https://github.com/influxdb/influxdb/issues/328). Join queries with math expressions don't work
- [Issue #440](https://github.com/influxdb/influxdb/issues/440). Heartbeat timeouts in logs
- [Issue #442](https://github.com/influxdb/influxdb/issues/442). shouldQuerySequentially didn't work as expected
  causing count(*) queries on large time series to use
  lots of memory
- [Issue #437](https://github.com/influxdb/influxdb/issues/437). Queries with negative constants don't parse properly
- [Issue #432](https://github.com/influxdb/influxdb/issues/432). Deleted data using a delete query is resurrected after a server restart
- [Issue #439](https://github.com/influxdb/influxdb/issues/439). Report the right location of the error in the query
- Fix some bugs with the WAL recovery on startup

## v0.5.6 [2014-04-08]

### Features

- [Issue #310](https://github.com/influxdb/influxdb/issues/310). Request should support multiple timeseries
- [Issue #416](https://github.com/influxdb/influxdb/issues/416). Improve the time it takes to drop database

### Bugfixes

- [Issue #413](https://github.com/influxdb/influxdb/issues/413). Don't assume that group by interval is greater than a second
- [Issue #415](https://github.com/influxdb/influxdb/issues/415). Include the database when sending an auth error back to the user
- [Issue #421](https://github.com/influxdb/influxdb/issues/421). Make read timeout a config option
- [Issue #392](https://github.com/influxdb/influxdb/issues/392). Different columns in different shards returns invalid results when a query spans those shards

### Bugfixes

## v0.5.5 [2014-04-04]

- Upgrade leveldb 1.10 -> 1.15

  This should be a backward compatible change, but is here for documentation only

### Feature

- Add a command line option to repair corrupted leveldb databases on startup
- [Issue #401](https://github.com/influxdb/influxdb/issues/401). No limit on the number of columns in the group by clause

### Bugfixes

- [Issue #398](https://github.com/influxdb/influxdb/issues/398). Support now() and NOW() in the query lang
- [Issue #403](https://github.com/influxdb/influxdb/issues/403). Filtering should work with join queries
- [Issue #404](https://github.com/influxdb/influxdb/issues/404). Filtering with invalid condition shouldn't crash the server
- [Issue #405](https://github.com/influxdb/influxdb/issues/405). Percentile shouldn't crash for small number of values
- [Issue #408](https://github.com/influxdb/influxdb/issues/408). Make InfluxDB recover from internal bugs and panics
- [Issue #390](https://github.com/influxdb/influxdb/issues/390). Multiple response.WriteHeader when querying as admin
- [Issue #407](https://github.com/influxdb/influxdb/issues/407). Start processing continuous queries only after the WAL is initialized
- Close leveldb databases properly if we couldn't create a new Shard. See leveldb\_shard\_datastore\_test:131

## v0.5.4 [2014-04-02]

### Bugfixes

- [Issue #386](https://github.com/influxdb/influxdb/issues/386). Drop series should work with series containing dots
- [Issue #389](https://github.com/influxdb/influxdb/issues/389). Filtering shouldn't stop prematurely
- [Issue #341](https://github.com/influxdb/influxdb/issues/341). Make the number of shards that are queried in parallel configurable
- [Issue #394](https://github.com/influxdb/influxdb/issues/394). Support count(distinct) and count(DISTINCT)
- [Issue #362](https://github.com/influxdb/influxdb/issues/362). Limit should be enforced after aggregation

## v0.5.3 [2014-03-31]

### Bugfixes

- [Issue #378](https://github.com/influxdb/influxdb/issues/378). Indexing should return if there are no requests added since the last index
- [Issue #370](https://github.com/influxdb/influxdb/issues/370). Filtering and limit should be enforced on the shards
- [Issue #379](https://github.com/influxdb/influxdb/issues/379). Boolean columns should be usable in where clauses
- [Issue #381](https://github.com/influxdb/influxdb/issues/381). Should be able to do deletes as a cluster admin

## v0.5.2 [2014-03-28]

### Bugfixes

- [Issue #342](https://github.com/influxdb/influxdb/issues/342). Data resurrected after a server restart
- [Issue #367](https://github.com/influxdb/influxdb/issues/367). Influxdb won't start if the api port is commented out
- [Issue #355](https://github.com/influxdb/influxdb/issues/355). Return an error on wrong time strings
- [Issue #331](https://github.com/influxdb/influxdb/issues/331). Allow negative time values in the where clause
- [Issue #371](https://github.com/influxdb/influxdb/issues/371). Seris index isn't deleted when the series is dropped
- [Issue #360](https://github.com/influxdb/influxdb/issues/360). Store and recover continuous queries

## v0.5.1 [2014-03-24]

### Bugfixes

- Revert the version of goraft due to a bug found in the latest version

## v0.5.0 [2014-03-24]

### Features

- [Issue #293](https://github.com/influxdb/influxdb/pull/293). Implement a Graphite listener

### Bugfixes

- [Issue #340](https://github.com/influxdb/influxdb/issues/340). Writing many requests while replaying seems to cause commits out of order

## v0.5.0-rc.6 [2014-03-20]

### Bugfixes

- Increase raft election timeout to avoid unecessary relections
- Sort points before writing them to avoid an explosion in the request
  number when the points are written randomly
- [Issue #335](https://github.com/influxdb/influxdb/issues/335). Fixes regexp for interpolating more than one column value in continuous queries
- [Issue #318](https://github.com/influxdb/influxdb/pull/318). Support EXPLAIN queries
- [Issue #333](https://github.com/influxdb/influxdb/pull/333). Fail
  when the password is too short or too long instead of passing it to
  the crypto library

## v0.5.0-rc.5 [2014-03-11]

### Bugfixes

- [Issue #312](https://github.com/influxdb/influxdb/issues/312). WAL should wait for server id to be set before recovering
- [Issue #301](https://github.com/influxdb/influxdb/issues/301). Use ref counting to guard against race conditions in the shard cache
- [Issue #319](https://github.com/influxdb/influxdb/issues/319). Propagate engine creation error correctly to the user
- [Issue #316](https://github.com/influxdb/influxdb/issues/316). Make
  sure we don't starve goroutines if we get an access denied error
  from one of the shards
- [Issue #306](https://github.com/influxdb/influxdb/issues/306). Deleting/Dropping database takes a lot of memory
- [Issue #302](https://github.com/influxdb/influxdb/issues/302). Should be able to set negative timestamps on points
- [Issue #327](https://github.com/influxdb/influxdb/issues/327). Make delete queries not use WAL. This addresses #315, #317 and #314
- [Issue #321](https://github.com/influxdb/influxdb/issues/321). Make sure we split points on shards properly

## v0.5.0-rc.4 [2014-03-07]

### Bugfixes

- [Issue #298](https://github.com/influxdb/influxdb/issues/298). Fix limit when querying multiple shards
- [Issue #305](https://github.com/influxdb/influxdb/issues/305). Shard ids not unique after restart
- [Issue #309](https://github.com/influxdb/influxdb/issues/309). Don't relog the requests on the remote server
- Fix few bugs in the WAL and refactor the way it works (this requires purging the WAL from previous rc)

## v0.5.0-rc.3 [2014-03-03]

### Bugfixes
- [Issue #69](https://github.com/influxdb/influxdb/issues/69). Support column aliases
- [Issue #287](https://github.com/influxdb/influxdb/issues/287). Make the lru cache size configurable
- [Issue #38](https://github.com/influxdb/influxdb/issues/38). Fix a memory leak discussed in this story
- [Issue #286](https://github.com/influxdb/influxdb/issues/286). Make the number of open shards configurable
- Make LevelDB use the max open files configuration option.

## v0.5.0-rc.2 [2014-02-27]

### Bugfixes

- [Issue #274](https://github.com/influxdb/influxdb/issues/274). Crash after restart
- [Issue #277](https://github.com/influxdb/influxdb/issues/277). Ensure duplicate shards won't be created
- [Issue #279](https://github.com/influxdb/influxdb/issues/279). Limits not working on regex queries
- [Issue #281](https://github.com/influxdb/influxdb/issues/281). `./influxdb -v` should print the sha when building from source
- [Issue #283](https://github.com/influxdb/influxdb/issues/283). Dropping shard and restart in cluster causes panic.
- [Issue #288](https://github.com/influxdb/influxdb/issues/288). Sequence numbers should be unique per server id

## v0.5.0-rc.1 [2014-02-25]

### Bugfixes

- Ensure large deletes don't take too much memory
- [Issue #240](https://github.com/influxdb/influxdb/pull/240). Unable to query against columns with `.` in the name.
- [Issue #250](https://github.com/influxdb/influxdb/pull/250). different result between normal and continuous query with "group by" clause
- [Issue #216](https://github.com/influxdb/influxdb/pull/216). Results with no points should exclude columns and points

### Features

- [Issue #243](https://github.com/influxdb/influxdb/issues/243). Should have endpoint to GET a user's attributes.
- [Issue #269](https://github.com/influxdb/influxdb/pull/269), [Issue #65](https://github.com/influxdb/influxdb/issues/65) New clustering architecture (see docs), with the side effect that queries can be distributed between multiple shards
- [Issue #164](https://github.com/influxdb/influxdb/pull/269),[Issue #103](https://github.com/influxdb/influxdb/pull/269),[Issue #166](https://github.com/influxdb/influxdb/pull/269),[Issue #165](https://github.com/influxdb/influxdb/pull/269),[Issue #132](https://github.com/influxdb/influxdb/pull/269) Make request log a log file instead of leveldb with recovery on startup

### Deprecated

- [Issue #189](https://github.com/influxdb/influxdb/issues/189). `/cluster_admins` and `/db/:db/users` return usernames in a `name` key instead of `username` key.
- [Issue #216](https://github.com/influxdb/influxdb/pull/216). Results with no points should exclude columns and points

## v0.4.4 [2014-02-05]

### Features

- Make the leveldb max open files configurable in the toml file

## v0.4.3 [2014-01-31]

### Bugfixes

- [Issue #225](https://github.com/influxdb/influxdb/issues/225). Remove a hard limit on the points returned by the datastore
- [Issue #223](https://github.com/influxdb/influxdb/issues/223). Null values caused count(distinct()) to panic
- [Issue #224](https://github.com/influxdb/influxdb/issues/224). Null values broke replication due to protobuf limitation

## v0.4.1 [2014-01-30]

### Features

- [Issue #193](https://github.com/influxdb/influxdb/issues/193). Allow logging to stdout. Thanks @schmurfy
- [Issue #190](https://github.com/influxdb/influxdb/pull/190). Add support for SSL.
- [Issue #194](https://github.com/influxdb/influxdb/pull/194). Should be able to disable Admin interface.

### Bugfixes

- [Issue #33](https://github.com/influxdb/influxdb/issues/33). Don't call WriteHeader more than once per request
- [Issue #195](https://github.com/influxdb/influxdb/issues/195). Allow the bind address to be configurable, Thanks @schmurfy.
- [Issue #199](https://github.com/influxdb/influxdb/issues/199). Make the test timeout configurable
- [Issue #200](https://github.com/influxdb/influxdb/issues/200). Selecting `time` or `sequence_number` silently fail
- [Issue #215](https://github.com/influxdb/influxdb/pull/215). Server fails to start up after Raft log compaction and restart.

## v0.4.0 [2014-01-17]

## Features

- [Issue #86](https://github.com/influxdb/influxdb/issues/86). Support arithmetic expressions in select clause
- [Issue #92](https://github.com/influxdb/influxdb/issues/92). Change '==' to '=' and '!=' to '<>'
- [Issue #88](https://github.com/influxdb/influxdb/issues/88). Support datetime strings
- [Issue #64](https://github.com/influxdb/influxdb/issues/64). Shard writes and queries across cluster with replay for briefly downed nodes (< 24 hrs)
- [Issue #78](https://github.com/influxdb/influxdb/issues/78). Sequence numbers persist across restarts so they're not reused
- [Issue #102](https://github.com/influxdb/influxdb/issues/102). Support expressions in where condition
- [Issue #101](https://github.com/influxdb/influxdb/issues/101). Support expressions in aggregates
- [Issue #62](https://github.com/influxdb/influxdb/issues/62). Support updating and deleting column values
- [Issue #96](https://github.com/influxdb/influxdb/issues/96). Replicate deletes in a cluster
- [Issue #94](https://github.com/influxdb/influxdb/issues/94). delete queries
- [Issue #116](https://github.com/influxdb/influxdb/issues/116). Use proper logging
- [Issue #40](https://github.com/influxdb/influxdb/issues/40). Use TOML instead of JSON in the config file
- [Issue #99](https://github.com/influxdb/influxdb/issues/99). Support list series in the query language
- [Issue #149](https://github.com/influxdb/influxdb/issues/149). Cluster admins should be able to perform reads and writes.
- [Issue #108](https://github.com/influxdb/influxdb/issues/108). Querying one point using `time =`
- [Issue #114](https://github.com/influxdb/influxdb/issues/114). Servers should periodically check that they're consistent.
- [Issue #93](https://github.com/influxdb/influxdb/issues/93). Should be able to drop a time series
- [Issue #177](https://github.com/influxdb/influxdb/issues/177). Support drop series in the query language.
- [Issue #184](https://github.com/influxdb/influxdb/issues/184). Implement Raft log compaction.
- [Issue #153](https://github.com/influxdb/influxdb/issues/153). Implement continuous queries

### Bugfixes

- [Issue #90](https://github.com/influxdb/influxdb/issues/90). Group by multiple columns panic
- [Issue #89](https://github.com/influxdb/influxdb/issues/89). 'Group by' combined with 'where' not working
- [Issue #106](https://github.com/influxdb/influxdb/issues/106). Don't panic if we only see one point and can't calculate derivative
- [Issue #105](https://github.com/influxdb/influxdb/issues/105). Panic when using a where clause that reference columns with null values
- [Issue #61](https://github.com/influxdb/influxdb/issues/61). Remove default limits from queries
- [Issue #118](https://github.com/influxdb/influxdb/issues/118). Make column names starting with '_' legal
- [Issue #121](https://github.com/influxdb/influxdb/issues/121). Don't fall back to the cluster admin auth if the db user auth fails
- [Issue #127](https://github.com/influxdb/influxdb/issues/127). Return error on delete queries with where condition that don't have time
- [Issue #117](https://github.com/influxdb/influxdb/issues/117). Fill empty groups with default values
- [Issue #150](https://github.com/influxdb/influxdb/pull/150). Fix parser for when multiple divisions look like a regex.
- [Issue #158](https://github.com/influxdb/influxdb/issues/158). Logged deletes should be stored with the time range if missing.
- [Issue #136](https://github.com/influxdb/influxdb/issues/136). Make sure writes are replicated in order to avoid triggering replays
- [Issue #145](https://github.com/influxdb/influxdb/issues/145). Server fails to join cluster if all starting at same time.
- [Issue #176](https://github.com/influxdb/influxdb/issues/176). Drop database should take effect on all nodes
- [Issue #180](https://github.com/influxdb/influxdb/issues/180). Column names not returned when running multi-node cluster and writing more than one point.
- [Issue #182](https://github.com/influxdb/influxdb/issues/182). Queries with invalid limit clause crash the server

### Deprecated

- deprecate '==' and '!=' in favor of '=' and '<>', respectively
- deprecate `/dbs` (for listing databases) in favor of a more consistent `/db` endpoint
- deprecate `username` field for a more consistent `name` field in `/db/:db/users` and `/cluster_admins`
- deprecate endpoints `/db/:db/admins/:user` in favor of using `/db/:db/users/:user` which should
  be used to update user flags, password, etc.
- Querying for column names that don't exist no longer throws an error.

## v0.3.2

## Features

- [Issue #82](https://github.com/influxdb/influxdb/issues/82). Add endpoint for listing available admin interfaces.
- [Issue #80](https://github.com/influxdb/influxdb/issues/80). Support durations when specifying start and end time
- [Issue #81](https://github.com/influxdb/influxdb/issues/81). Add support for IN

## Bugfixes

- [Issue #75](https://github.com/influxdb/influxdb/issues/75). Don't allow time series names that start with underscore
- [Issue #85](https://github.com/influxdb/influxdb/issues/85). Non-existing columns exist after they have been queried before

## v0.3.0

## Features

- [Issue #51](https://github.com/influxdb/influxdb/issues/51). Implement first and last aggregates
- [Issue #35](https://github.com/influxdb/influxdb/issues/35). Support table aliases in Join Queries
- [Issue #71](https://github.com/influxdb/influxdb/issues/71). Add WillReturnSingleSeries to the Query
- [Issue #61](https://github.com/influxdb/influxdb/issues/61). Limit should default to 10k
- [Issue #59](https://github.com/influxdb/influxdb/issues/59). Add histogram aggregate function

## Bugfixes

- Fix join and merges when the query is a descending order query
- [Issue #57](https://github.com/influxdb/influxdb/issues/57). Don't panic when type of time != float
- [Issue #63](https://github.com/influxdb/influxdb/issues/63). Aggregate queries should not have a sequence_number column

## v0.2.0

### Features

- [Issue #37](https://github.com/influxdb/influxdb/issues/37). Support the negation of the regex matcher !~
- [Issue #47](https://github.com/influxdb/influxdb/issues/47). Spill out query and database detail at the time of bug report

### Bugfixes

- [Issue #36](https://github.com/influxdb/influxdb/issues/36). The regex operator should be =~ not ~=
- [Issue #39](https://github.com/influxdb/influxdb/issues/39). Return proper content types from the http api
- [Issue #42](https://github.com/influxdb/influxdb/issues/42). Make the api consistent with the docs
- [Issue #41](https://github.com/influxdb/influxdb/issues/41). Table/Points not deleted when database is dropped
- [Issue #45](https://github.com/influxdb/influxdb/issues/45). Aggregation shouldn't mess up the order of the points
- [Issue #44](https://github.com/influxdb/influxdb/issues/44). Fix crashes on RHEL 5.9
- [Issue #34](https://github.com/influxdb/influxdb/issues/34). Ascending order always return null for columns that have a null value
- [Issue #55](https://github.com/influxdb/influxdb/issues/55). Limit should limit the points that match the Where clause
- [Issue #53](https://github.com/influxdb/influxdb/issues/53). Writing null values via HTTP API fails

### Deprecated

- Preparing to deprecate `/dbs` (for listing databases) in favor of a more consistent `/db` endpoint
- Preparing to deprecate `username` field for a more consistent `name` field in the `/db/:db/users`
- Preparing to deprecate endpoints `/db/:db/admins/:user` in favor of using `/db/:db/users/:user` which should
  be used to update user flags, password, etc.

## v0.1.0

### Features

- [Issue #29](https://github.com/influxdb/influxdb/issues/29). Semicolon is now optional in queries
- [Issue #31](https://github.com/influxdb/influxdb/issues/31). Support Basic Auth as well as query params for authentication.

### Bugfixes

- Don't allow creating users with empty username
- [Issue #22](https://github.com/influxdb/influxdb/issues/22). Don't set goroot if it was set
- [Issue #25](https://github.com/influxdb/influxdb/issues/25). Fix queries that use the median aggregator
- [Issue #26](https://github.com/influxdb/influxdb/issues/26). Default log and db directories should be in /opt/influxdb/shared/data
- [Issue #27](https://github.com/influxdb/influxdb/issues/27). Group by should not blow up if the one of the columns in group by has null values
- [Issue #30](https://github.com/influxdb/influxdb/issues/30). Column indexes/names getting off somehow
- [Issue #32](https://github.com/influxdb/influxdb/issues/32). Fix many typos in the codebase. Thanks @pborreli

## v0.0.9

#### Features

- Add stddev(...) support
- Better docs, thanks @auxesis and @d-snp.

#### Bugfixes

- Set PYTHONPATH and CC appropriately on mac os x.
- [Issue #18](https://github.com/influxdb/influxdb/issues/18). Fix 386 debian and redhat packages
- [Issue #23](https://github.com/influxdb/influxdb/issues/23). Fix the init scripts on redhat

## v0.0.8

#### Features

- Add a way to reset the root password from the command line.
- Add distinct(..) and derivative(...) support
- Print test coverage if running go1.2

#### Bugfixes

- Fix the default admin site path in the .deb and .rpm packages.
- Fix the configuration filename in the .tar.gz package.

## v0.0.7

#### Features

- include the admin site in the repo to make it easier for newcomers.

## v0.0.6

#### Features

- Add count(distinct(..)) support

#### Bugfixes

- Reuse levigo read/write options.

## v0.0.5

#### Features

- Cache passwords in memory to speed up password verification
- Add MERGE and INNER JOIN support

#### Bugfixes

- All columns should be returned if `select *` was used
- Read/Write benchmarks

## v0.0.2

#### Features

- Add an admin UI
- Deb and RPM packages

#### Bugfixes

- Fix some nil pointer dereferences
- Cleanup the aggregators implementation

## v0.0.1 [2013-10-22]

  * Initial Release
