# Converting b1 and bz1 shards to tsm1
`influx_tsm` is a tool for converting b1 and bz1 shards to tsm1 format. Converting shards to tsm1 format results in a very significant reduction in disk usage, and significantly improved write-throughput, when writing data into those shards.

Conversion can be controlled on a database-by-database basis. By default a database is backed up before it is converted, allowing you to roll back any changes. Because of the backup process, ensure the host system has at least as much free disk space as the disk space consumed by the _data_ directory of your InfluxDB system.

The tool automatically ignores tsm1 shards, and can be run idempotently on any database.

Conversion is an offline process, and the InfluxDB system must be stopped during conversion. However the conversion process reads and writes shards directly on disk and should be fast.

## Steps
Follow these steps to perform a conversion.

* Identify the databases you wish to convert. You can convert one or more databases at a time. By default all databases are converted.
* Decide on parallel operation. By default the conversion operation peforms each operation in a serial manner. This minimizes load on the host system performing the conversion, but also takes the most time. If you wish to minimize the time conversion takes, enable parallel mode. Conversion will then perform as many operations as possible in parallel, but the process may place significant load on the host system (CPU, disk, and RAM, usage will all increase).
* Stop all write-traffic to your InfluxDB system.
* Restart the InfluxDB service and wait until all WAL data is flushed to disk -- this has completed when the system responds to queries. This is to ensure all data is present in shards.
* Stop the InfluxDB service. It should not be restarted until conversion is complete.
* Run conversion tool.
* Unless you ran the conversion tool as the same user as that which runs InfluxDB, then you may need to set the correct read-and-write permissions on the new tsm1 directories.
* Restart node and ensure data looks correct.
* If everything looks OK, you may then wish to remove or archive the backed-up databases.
* Restart write traffic.

## Example session
Below is an example session, showing a database being converted.

```
$ mkdir ~/influxdb_backup
$ influx_tsm -backup ~/influxdb_backup -parallel ~/.influxdb/data

b1 and bz1 shard conversion.
-----------------------------------
Data directory is:                  /home/user/.influxdb/data
Backup directory is:                /home/user/influxdb_backup
Databases specified:                all
Database backups enabled:           yes
Parallel mode enabled (GOMAXPROCS): yes (8)


Found 1 shards that will be converted.

Database        Retention       Path                                                    Engine  Size
_internal       monitor         /home/user/.influxdb/data/_internal/monitor/1           bz1     65536

These shards will be converted. Proceed? y/N: y
Conversion starting....
Backing up 1 databases...
2016/01/28 12:23:43.699266 Backup of databse '_internal' started
2016/01/28 12:23:43.699883 Backing up file /home/user/.influxdb/data/_internal/monitor/1
2016/01/28 12:23:43.700052 Database _internal backed up (851.776µs)
2016/01/28 12:23:43.700320 Starting conversion of shard: /home/user/.influxdb/data/_internal/monitor/1
2016/01/28 12:23:43.706276 Conversion of /home/user/.influxdb/data/_internal/monitor/1 successful (6.040148ms)

Summary statistics
========================================
Databases converted:                 1
Shards converted:                    1
TSM files created:                   1
Points read:                         369
Points written:                      369
NaN filtered:                        0
Inf filtered:                        0
Points without fields filtered:      0
Disk usage pre-conversion (bytes):   65536
Disk usage post-conversion (bytes):  11000
Reduction factor:                    83%
Bytes per TSM point:                 29.81
Total conversion time:               7.330443ms

$ # restart node, verify data

$ rm -r ~/influxdb_backup
```

Note that the tool first lists the shards that will be converted, before asking for confirmation. You can abort the conversion process at this step if you just wish to see what would be converted, or if the list of shards does not look correct.

## Rolling back a conversion
After a successful backup (the message `Database XYZ backed up` was logged), you have a duplicate of that database in the _backup_ directory you provided on the command line. If, when checking your data after a successful conversion, you notice things missing or something just isn't right, you can "undo"  the conversion:

- Shut down your node (this is very important)
- Remove the database's directory from the influxdb `data` directory (default: ~/.influxdb/data/XYZ)
- Copy (to really make sure the shard is preserved) the database's directory from the backup directory you created into the `data` directory.

Using the same directories as above, and assuming a database named `stats`:
```
$ rm -r ~/.influxdb/data/stats
$ cp -r ~/influxdb_backup/stats ~/.influxdb/data/
$ # restart influxd node
```
