# `influx_inspect`

## Ways to run

### `influx_inspect`
Will print usage for the tool.

### `influx_inspect report`
Displays series meta-data for all shards.  Default location [$HOME/.influxdb]

### `influx_inspect dumptsm`
Dumps low-level details about tsm1 files

#### Flags

##### `-index` bool
Dump raw index data.

`default` = false

#### `-blocks` bool
Dump raw block data.

`default` = false

#### `-all`
Dump all data. Caution: This may print a lot of information.

`default` = false

#### `-filter-key`
Only display index and block data match this key substring.

`default` = ""


### `influx_inspect export`
Exports all tsm files to line protocol.  This output file can be imported via the [influx](https://github.com/influxdata/influxdb/tree/master/importer#running-the-import-command) command.


#### `-datadir` string
Data storage path.

`default` = "$HOME/.influxdb/data"

#### `-waldir` string
WAL storage path.

`default` = "$HOME/.influxdb/wal"

#### `-out` string
Destination file to export to.
In case of export to Parquet, destination should be existing directory.

`default` = "$HOME/.influxdb/export"

#### `-database` string
Database to export.
Mandatory for export to Parquet, optional otherwise (default).

`default` = ""

#### `-retention` string
Retention policy to export.
Mandatory for export to Parquet, optional otherwise (default).

`default` = ""

#### `-measurement` string
Name of the measurement to export.
Mandatory for export to Parquet, optional otherwise (default).

`default` = ""

#### `-start` string (optional)
Optional. The time range to start at.

#### `-end` string (optional)
Optional. The time range to end at.

#### `-compress` bool (optional)
Compress the output.

`default` = false

#### `-parquet` bool (optional)
Export to Parquet.

`default` = false

#### `-chunk-size` int (optional)
Size to partition Parquet files, in bytes.

`default` = 100000000

#### Sample Commands

Export entire database and compress output:
```
influx_inspect export --compress
```

Export specific retention policy:
```
influx_inspect export --database mydb --retention autogen
```

Export specific measurement to Parquet:
```
influx_inspect export --database mydb --retention autogen --measurement cpu --parquet
```

##### Sample Data
This is a sample of what the output will look like.

```
# DDL
CREATE DATABASE MY_DB_NAME
CREATE RETENTION POLICY autogen ON MY_DB_NAME DURATION inf REPLICATION 1

# DML
# CONTEXT-DATABASE:MY_DB_NAME
# CONTEXT-RETENTION-POLICY:autogen
randset value=97.9296104805 1439856000000000000
randset value=25.3849066842 1439856100000000000
```

# Caveats

The system does not have access to the meta store when exporting TSM shards.  As such, it always creates the retention policy with infinite duration and replication factor of 1.
End users may want to change this prior to re-importing if they are importing to a cluster or want a different duration for retention.
