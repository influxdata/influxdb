## v0.8.0-rc.5 [unreleased]

### Bugfixes

- [Issue #426](https://github.com/influxdb/influxdb/pull/426). Fill should fill the entire time range that is requested

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
