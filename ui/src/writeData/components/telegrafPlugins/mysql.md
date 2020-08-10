# MySQL Input Plugin

This plugin gathers the statistic data from MySQL server

* Global statuses
* Global variables
* Slave statuses
* Binlog size
* Process list
* User Statistics
* Info schema auto increment columns
* InnoDB metrics
* Table I/O waits
* Index I/O waits
* Perf Schema table lock waits
* Perf Schema event waits
* Perf Schema events statements
* File events statistics
* Table schema statistics

### Configuration

```toml
[[inputs.mysql]]
  ## specify servers via a url matching:
  ##  [username[:password]@][protocol[(address)]]/[?tls=[true|false|skip-verify|custom]]
  ##  see https://github.com/go-sql-driver/mysql#dsn-data-source-name
  ##  e.g.
  ##    servers = ["user:passwd@tcp(127.0.0.1:3306)/?tls=false"]
  ##    servers = ["user@tcp(127.0.0.1:3306)/?tls=false"]
  #
  ## If no servers are specified, then localhost is used as the host.
  servers = ["tcp(127.0.0.1:3306)/"]

  ## Selects the metric output format.
  ##
  ## This option exists to maintain backwards compatibility, if you have
  ## existing metrics do not set or change this value until you are ready to
  ## migrate to the new format.
  ##
  ## If you do not have existing metrics from this plugin set to the latest
  ## version.
  ##
  ## Telegraf >=1.6: metric_version = 2
  ##           <1.6: metric_version = 1 (or unset)
  metric_version = 2

  ## if the list is empty, then metrics are gathered from all database tables
  # table_schema_databases = []

  ## gather metrics from INFORMATION_SCHEMA.TABLES for databases provided above list
  # gather_table_schema = false

  ## gather thread state counts from INFORMATION_SCHEMA.PROCESSLIST
  # gather_process_list = false

  ## gather user statistics from INFORMATION_SCHEMA.USER_STATISTICS
  # gather_user_statistics = false

  ## gather auto_increment columns and max values from information schema
  # gather_info_schema_auto_inc = false

  ## gather metrics from INFORMATION_SCHEMA.INNODB_METRICS
  # gather_innodb_metrics = false

  ## gather metrics from SHOW SLAVE STATUS command output
  # gather_slave_status = false

  ## gather metrics from SHOW BINARY LOGS command output
  # gather_binary_logs = false

  ## gather metrics from SHOW GLOBAL VARIABLES command output
  # gather_global_variables = true

  ## gather metrics from PERFORMANCE_SCHEMA.TABLE_IO_WAITS_SUMMARY_BY_TABLE
  # gather_table_io_waits = false

  ## gather metrics from PERFORMANCE_SCHEMA.TABLE_LOCK_WAITS
  # gather_table_lock_waits = false

  ## gather metrics from PERFORMANCE_SCHEMA.TABLE_IO_WAITS_SUMMARY_BY_INDEX_USAGE
  # gather_index_io_waits = false

  ## gather metrics from PERFORMANCE_SCHEMA.EVENT_WAITS
  # gather_event_waits = false

  ## gather metrics from PERFORMANCE_SCHEMA.FILE_SUMMARY_BY_EVENT_NAME
  # gather_file_events_stats = false

  ## gather metrics from PERFORMANCE_SCHEMA.EVENTS_STATEMENTS_SUMMARY_BY_DIGEST
  # gather_perf_events_statements = false

  ## the limits for metrics form perf_events_statements
  # perf_events_statements_digest_text_limit = 120
  # perf_events_statements_limit = 250
  # perf_events_statements_time_limit = 86400

  ## Some queries we may want to run less often (such as SHOW GLOBAL VARIABLES)
  ##   example: interval_slow = "30m"
  # interval_slow = ""

  ## Optional TLS Config (will be used if tls=custom parameter specified in server uri)
  # tls_ca = "/etc/telegraf/ca.pem"
  # tls_cert = "/etc/telegraf/cert.pem"
  # tls_key = "/etc/telegraf/key.pem"
  ## Use TLS but skip chain & host verification
  # insecure_skip_verify = false
```

#### Metric Version

When `metric_version = 2`, a variety of field type issues are corrected as well
as naming inconsistencies.  If you have existing data on the original version
enabling this feature will cause a `field type error` when inserted into
InfluxDB due to the change of types.  For this reason, you should keep the
`metric_version` unset until you are ready to migrate to the new format.

If preserving your old data is not required you may wish to drop conflicting
measurements:
```
DROP SERIES from mysql
DROP SERIES from mysql_variables
DROP SERIES from mysql_innodb
```

Otherwise, migration can be performed using the following steps:

1. Duplicate your `mysql` plugin configuration and add a `name_suffix` and
`metric_version = 2`, this will result in collection using both the old and new
style concurrently:
   ```toml
   [[inputs.mysql]]
     servers = ["tcp(127.0.0.1:3306)/"]

   [[inputs.mysql]]
     name_suffix = "_v2"
     metric_version = 2

     servers = ["tcp(127.0.0.1:3306)/"]
   ```

2. Upgrade all affected Telegraf clients to version >=1.6.

   New measurements will be created with the `name_suffix`, for example::
   - `mysql_v2`
   - `mysql_variables_v2`

3. Update charts, alerts, and other supporting code to the new format.
4. You can now remove the old `mysql` plugin configuration and remove old
   measurements.

If you wish to remove the `name_suffix` you may use Kapacitor to copy the
historical data to the default name.  Do this only after retiring the old
measurement name.

1. Use the technique described above to write to multiple locations:
   ```toml
   [[inputs.mysql]]
     servers = ["tcp(127.0.0.1:3306)/"]
     metric_version = 2

   [[inputs.mysql]]
     name_suffix = "_v2"
     metric_version = 2

     servers = ["tcp(127.0.0.1:3306)/"]
   ```
2. Create a TICKScript to copy the historical data:
   ```
   dbrp "telegraf"."autogen"

   batch
       |query('''
           SELECT * FROM "telegraf"."autogen"."mysql_v2"
       ''')
           .period(5m)
           .every(5m)
           |influxDBOut()
                   .database('telegraf')
                   .retentionPolicy('autogen')
                   .measurement('mysql')
   ```
3. Define a task for your script:
   ```sh
   kapacitor define copy-measurement -tick copy-measurement.task
   ```
4. Run the task over the data you would like to migrate:
   ```sh
   kapacitor replay-live batch -start 2018-03-30T20:00:00Z -stop 2018-04-01T12:00:00Z -rec-time -task copy-measurement
   ```
5. Verify copied data and repeat for other measurements.

### Metrics:
* Global statuses - all numeric and boolean values of `SHOW GLOBAL STATUSES`
* Global variables - all numeric and boolean values of `SHOW GLOBAL VARIABLES`
* Slave status - metrics from `SHOW SLAVE STATUS` the metrics are gathered when
the single-source replication is on. If the multi-source replication is set,
then everything works differently, this metric does not work with multi-source
replication.
    * slave_[column name]()
* Binary logs - all metrics including size and count of all binary files.
Requires to be turned on in configuration.
    * binary_size_bytes(int, number)
    * binary_files_count(int, number)
* Process list - connection metrics from processlist for each user. It has the following tags
    * connections(int, number)
* User Statistics - connection metrics from user statistics for each user. It has the following fields
    * access_denied
    * binlog_bytes_written
    * busy_time
    * bytes_received
    * bytes_sent
    * commit_transactions
    * concurrent_connections
    * connected_time
    * cpu_time
    * denied_connections
    * empty_queries
    * hostlost_connections
    * other_commands
    * rollback_transactions
    * rows_fetched
    * rows_updated
    * select_commands
    * server
    * table_rows_read
    * total_connections
    * total_ssl_connections
    * update_commands
    * user
* Perf Table IO waits - total count and time of I/O waits event for each table
and process. It has following fields:
    * table_io_waits_total_fetch(float, number)
    * table_io_waits_total_insert(float, number)
    * table_io_waits_total_update(float, number)
    * table_io_waits_total_delete(float, number)
    * table_io_waits_seconds_total_fetch(float, milliseconds)
    * table_io_waits_seconds_total_insert(float, milliseconds)
    * table_io_waits_seconds_total_update(float, milliseconds)
    * table_io_waits_seconds_total_delete(float, milliseconds)
* Perf index IO waits - total count and time of I/O waits event for each index
and process. It has following fields:
    * index_io_waits_total_fetch(float, number)
    * index_io_waits_seconds_total_fetch(float, milliseconds)
    * index_io_waits_total_insert(float, number)
    * index_io_waits_total_update(float, number)
    * index_io_waits_total_delete(float, number)
    * index_io_waits_seconds_total_insert(float, milliseconds)
    * index_io_waits_seconds_total_update(float, milliseconds)
    * index_io_waits_seconds_total_delete(float, milliseconds)
* Info schema autoincrement statuses - autoincrement fields and max values
for them. It has following fields:
    * auto_increment_column(int, number)
    * auto_increment_column_max(int, number)
* InnoDB metrics - all metrics of information_schema.INNODB_METRICS with a status "enabled"
* Perf table lock waits - gathers total number and time for SQL and external
lock waits events for each table and operation. It has following fields.
The unit of fields varies by the tags.
    * read_normal(float, number/milliseconds)
    * read_with_shared_locks(float, number/milliseconds)
    * read_high_priority(float, number/milliseconds)
    * read_no_insert(float, number/milliseconds)
    * write_normal(float, number/milliseconds)
    * write_allow_write(float, number/milliseconds)
    * write_concurrent_insert(float, number/milliseconds)
    * write_low_priority(float, number/milliseconds)
    * read(float, number/milliseconds)
    * write(float, number/milliseconds)
* Perf events waits - gathers total time and number of event waits
    * events_waits_total(float, number)
    * events_waits_seconds_total(float, milliseconds)
* Perf file events statuses - gathers file events statuses
    * file_events_total(float,number)
    * file_events_seconds_total(float, milliseconds)
    * file_events_bytes_total(float, bytes)
* Perf events statements - gathers attributes of each event
    * events_statements_total(float, number)
    * events_statements_seconds_total(float, millieconds)
    * events_statements_errors_total(float, number)
    * events_statements_warnings_total(float, number)
    * events_statements_rows_affected_total(float, number)
    * events_statements_rows_sent_total(float, number)
    * events_statements_rows_examined_total(float, number)
    * events_statements_tmp_tables_total(float, number)
    * events_statements_tmp_disk_tables_total(float, number)
    * events_statements_sort_merge_passes_totals(float, number)
    * events_statements_sort_rows_total(float, number)
    * events_statements_no_index_used_total(float, number)
* Table schema - gathers statistics of each schema. It has following measurements
    * info_schema_table_rows(float, number)
    * info_schema_table_size_data_length(float, number)
    * info_schema_table_size_index_length(float, number)
    * info_schema_table_size_data_free(float, number)
    * info_schema_table_version(float, number)

## Tags
* All measurements has following tags
    * server (the host name from which the metrics are gathered)
* Process list measurement has following tags
    * user (username for whom the metrics are gathered)
* User Statistics measurement has following tags
    * user (username for whom the metrics are gathered)
* Perf table IO waits measurement has following tags
    * schema
    * name (object name for event or process)
* Perf index IO waits has following tags
    * schema
    * name
    * index
* Info schema autoincrement statuses has following tags
    * schema
    * table
    * column
* Perf table lock waits has following tags
    * schema
    * table
    * sql_lock_waits_total(fields including this tag have numeric unit)
    * external_lock_waits_total(fields including this tag have numeric unit)
    * sql_lock_waits_seconds_total(fields including this tag have millisecond unit)
    * external_lock_waits_seconds_total(fields including this tag have millisecond unit)
* Perf events statements has following tags
    * event_name
* Perf file events statuses has following tags
    * event_name
    * mode
* Perf file events statements has following tags
    * schema
    * digest
    * digest_text
* Table schema has following tags
    * schema
    * table
    * component
    * type
    * engine
    * row_format
    * create_options
