## v2.1.0

### `influx` CLI moved to separate repository

The `influx` CLI has been moved to its [own GitHub repository](https://github.com/influxdata/influx-cli/). Release artifacts
produced by `influxdb` are impacted as follows:

* Release archives (`.tar.gz` and `.zip`) no longer contain the `influx` binary.
* The `influxdb2` package (`.deb` and `.rpm`) no longer contains the `influx` binary. Instead, it declares a recommended
  dependency on the new `influx-cli` package.
* The `quay.io/influxdb/influxdb` image no longer contains the `influx` binary. Users are recommended to migrate to the
  `influxdb` image hosted in DockerHub.

With this change, versions of the `influx` CLI and `influxd` server are not guaranteed to exactly match. Please use
`influxd version` or `curl <your-server-url>/health` when checking the version of the installed/running server.

### Notebooks and Annotations

Support for Notebooks and Annotations is included with this release.

### SQLite Metadata Store

This release adds an embedded SQLite database for storing metadata required by the latest UI features like Notebooks and Annotations.

### Features

1. [19811](https://github.com/influxdata/influxdb/pull/19811): Add Geo graph type to be able to store in Dashboard cells
1. [21218](https://github.com/influxdata/influxdb/pull/21218): Add the properties of a static legend for line graphs and band plots
1. [21367](https://github.com/influxdata/influxdb/pull/21367): List users via the API now supports pagination
1. [21543](https://github.com/influxdata/influxdb/pull/21543): Added `influxd` configuration flag `--sqlite-path` for specifying a user-defined path to the SQLite database file
1. [21543](https://github.com/influxdata/influxdb/pull/21543): Updated `influxd` configuration flag `--store` to work with string values `disk` or `memory`. Memory continues to store metadata in-memory for testing; `disk` will persist metadata to disk via bolt and SQLite
1. [21547](https://github.com/influxdata/influxdb/pull/21547): Allow hiding the tooltip independently of the static legend
1. [21584](https://github.com/influxdata/influxdb/pull/21584): Added the `api/v2/backup/metadata` endpoint for backing up both KV and SQL metadata, and the `api/v2/restore/sql` for restoring SQL metadata
1. [21635](https://github.com/influxdata/influxdb/pull/21635): Port `influxd inspect verify-seriesfile` to 2.x
1. [21621](https://github.com/influxdata/influxdb/pull/21621): Add `storage-wal-max-concurrent-writes` config option to `influxd` to enable tuning memory pressure under heavy write load
1. [21621](https://github.com/influxdata/influxdb/pull/21621): Add `storage-wal-max-write-delay` config option to `influxd` to prevent deadlocks when the WAL is overloaded with concurrent writes
1. [21615](https://github.com/influxdata/influxdb/pull/21615): Ported the `influxd inspect verify-tsm` command from 1.x
1. [21646](https://github.com/influxdata/influxdb/pull/21646): Ported the `influxd inspect verify-tombstone` command from 1.x
1. [21761](https://github.com/influxdata/influxdb/pull/21761): Ported the `influxd inspect dump-tsm` command from 1.x
1. [21788](https://github.com/influxdata/influxdb/pull/21788): Ported the `influxd inspect report-tsi` command from 1.x
1. [21784](https://github.com/influxdata/influxdb/pull/21784): Ported the `influxd inspect dumptsi` command from 1.x
1. [21786](https://github.com/influxdata/influxdb/pull/21786): Ported the `influxd inspect deletetsm` command from 1.x
1. [21888](https://github.com/influxdata/influxdb/pull/21888): Ported the `influxd inspect dump-wal` command from 1.x
1. [21828](https://github.com/influxdata/influxdb/pull/21828): Added the command `influx inspect verify-wal`
1. [21814](https://github.com/influxdata/influxdb/pull/21814): Ported the `influxd inspect report-tsm` command from 1.x
1. [21936](https://github.com/influxdata/influxdb/pull/21936): Ported the `influxd inspect build-tsi` command from 1.x
1. [21938](https://github.com/influxdata/influxdb/pull/21938): Added route to delete individual secret
1. [21972](https://github.com/influxdata/influxdb/pull/21972): Added support for notebooks and annotations
1. [22311](https://github.com/influxdata/influxdb/pull/22311): Add `storage-no-validate-field-size` config to `influxd` to disable enforcement of max field size
1. [22322](https://github.com/influxdata/influxdb/pull/22322): Add support for `merge_hll`, `sum_hll`, and `count_hll` in InfluxQL
1. [22476](https://github.com/influxdata/influxdb/pull/22476): Allow new telegraf input plugins and update toml
1. [22607](https://github.com/influxdata/influxdb/pull/22607): Update push down window logic for location option
1. [22617](https://github.com/influxdata/influxdb/pull/22617): Add `--storage-write-timeout` flag to set write request timeouts
1. [22396](https://github.com/influxdata/influxdb/pull/22396): Show measurement database and retention policy wildcards
1. [22590](https://github.com/influxdata/influxdb/pull/22590): New recovery subcommand allows creating recovery user/token
1. [22629](https://github.com/influxdata/influxdb/pull/22629): Return new operator token during backup overwrite
1. [22635](https://github.com/influxdata/influxdb/pull/22635): update window planner rules for location changes to support fixed offsets
1. [22634](https://github.com/influxdata/influxdb/pull/22634): enable writing to remote hosts via `to()` and `experimental.to()`
1. [22498](https://github.com/influxdata/influxdb/pull/22498): Add Bearer token auth
1. [22669](https://github.com/influxdata/influxdb/pull/22669): Enable new dashboard autorefresh
1. [22674](https://github.com/influxdata/influxdb/pull/22674): list-bucket API supports pagination when filtering by org
1. [22671](https://github.com/influxdata/influxdb/pull/22671): Update flux to `v0.134.0`
1. [22692](https://github.com/influxdata/influxdb/pull/22692): Update UI to `OSS-2.1.0`

### Bug Fixes

1. [21648](https://github.com/influxdata/influxdb/pull/21648): Change static legend's `hide` to `show` to let users decide if they want it
1. [22442](https://github.com/influxdata/influxdb/pull/22442): Detect noninteractive prompt when displaying warning in `buildtsi`
1. [22458](https://github.com/influxdata/influxdb/pull/22458): Ensure files are closed before they are deleted or moved in `deletetsm`
1. [22448](https://github.com/influxdata/influxdb/pull/22448): More expressive errors
1. [22545](https://github.com/influxdata/influxdb/pull/22545): Sync series segment to disk after writing
1. [22604](https://github.com/influxdata/influxdb/pull/22604): Do not allow shard creation to create overlapping shards
1. [22650](https://github.com/influxdata/influxdb/pull/22650): Don't drop shard-group durations when upgrading DBs
