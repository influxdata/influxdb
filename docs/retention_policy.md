# Data Retention Policy in IOx

The data retention policy in IOx belongs to the namespace and is specified at namespace creation time and is infinite if not specified. It can be set or updated via the IOx CLI or via the namespace gRPC API exposed on the IOx Router server. When specifying it from the CLI the unit is hours (and 0 represents infinite), however it is stored in nanoseconds internally and in the gRPC API. To get help for the CLI, pass `--help` to each command such as:

```
influxdb_iox namespace --help
influxdb_iox namespace create --help
influxdb_iox namespace retention --help
```

### Examples

- Create namespace `my_namespace` with 24 hours of data retention

    ```
    influxdb_iox namespace create --retention-hours 24 my_namespace
    ```

- Create namespace `my_namespace_2` with default data retention

    ```
    influxdb_iox namespace create my_namespace_2
    ```
    The retention period of `name_space_2` will be the value of the environment variable `INFLUXDB_IOX_NAMESPACE_RETENTION_HOURS`. If the environment variable is not set, IOx will use its default value which is 0 representing infinite retention.



- Update retention of namespace `my_namespace` to 10 hours
  
    ```
    influxdb_iox namespace retention --retention-hours 10 my_namespace
    ```

- Update retention of namespace `my_namespace` to  infinite
    ```
    influxdb_iox namespace retention --retention-hours 0 my_namespace
    ```

# Retention Period

Data of a row of a table is retained if the value of its `time` field is inside the retention-period of its table's namespace. In other words, the rule to check data inside retention period is  `time >= now - namespace-retention-period`

- At ingestion time, data outside the retention period will be rejected by IOx Routers. Note that if at least one line protocol of the line protocol batch is outside the retention period, its whole batch will get rejected.
- At query time, rows of data outside the retention period will be filtered out by the IOx Querier.

Note that at this moment (Nov 28, 2022), Routers and Queriers cache namespace retention periods at restart and won't refresh them. If the retention period is updated, the rejection in Routers and data filtering in Queriers still use the namespace's old retention period until they are restarted.

# When data is deleted

## Soft Delete
IOx has a background loop to soft-delete data outside its namespace retention period. Soft-delete means if all data of a parquet file is outside the retention period, the parquet file will be marked to be deleted. Files that are marked to be deleted won't be queried by the Queriers. This background task will read the up-to-date namespace retention periods from IOx catalog and always delete appropriate data. However, if the retention period is updated to a lot longer, data that was removed because it is outside its previous and shorter retention period won't be able to recover. 

How often the data is schedule to be soft deleted is configured using `INFLUXDB_IOX_GC_RETENTION_SLEEP_INTERVAL_MINUTES`.

Parquet file whose data is partially outside the retention period will stay active. Queriers will filter outside-retention-period data of the file at query time as described above.

## Hard Delete

IOx Garbage Collector will permanently remove soft deleted files after certain days configured in `INFLUXDB_IOX_GC_PARQUETFILE_CUTOFF`. Note that parquet files are also soft deleted after their data are compacted into a different file by IOx Compactors. The Garbage Collector is designed to remove files without any knowledge of how they were soft deleted, due to compaction or outside retetion.
