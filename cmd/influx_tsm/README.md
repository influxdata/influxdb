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
* If everything looks OK, you may then wish to remove or archive the backed-up databases. This is not required for a correctly functioning InfluxDB system, since the backed-up databases will be simply ignored by the system. Backed-up databases are suffixed with the extension `.bak`.
* Restart write traffic.

## Example session
Below is an example session, showing a database being converted.

```
$ influx_tsm -parallel ~/.influxdb/data/

b1 and bz1 shard conversion.
-----------------------------------
Data directory is: /home/bob/.influxdb/data/
Databases specified: all
Parallel mode enabled: yes
Database backups enabled: yes
1 shard(s) detected, 1 non-TSM shards detected.

Database        Retention       Path                                         Engine  Size
_internal       monitor         /home/bob/.influxdb/data/_internal/monitor/1 bz1     262144

These shards will be converted. Proceed? y/N: y
Conversion starting....
Database _internal backed up.
Conversion of /home/bob/.influxdb/data/_internal/monitor/1 successful (27.485037ms)

$ rm -r /home/bob/.influxdb/data/_internal.bak # After confirming converted data looks good.
```
Note that the tool first lists the shards that will be converted, before asking for confirmation. You can abort the conversion process at this step if you just wish to see what would be converted, or if the list of shards does not look correct.

## Rolling back a conversion
If you wish to rollback a conversion check the databases in your _data_ directory. For every backed-up database remove the non-backed up version and then rename the backup so that it no longer has the extention `.bak`. Then restart your InfluxDB system.
