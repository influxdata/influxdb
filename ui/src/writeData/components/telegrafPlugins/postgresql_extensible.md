# PostgreSQL plugin

This postgresql plugin provides metrics for your postgres database. It has been
designed to parse SQL queries in the plugin section of your `telegraf.conf`.

The example below has two queries are specified, with the following parameters:

* The SQL query itself
* The minimum PostgreSQL version supported (the numeric display visible in pg_settings)
* A boolean to define if the query has to be run against some specific database (defined in the `databases` variable of the plugin section)
* The name of the measurement
* A list of the columns to be defined as tags

```
[[inputs.postgresql_extensible]]
  # specify address via a url matching:
  # postgres://[pqgotest[:password]]@host:port[/dbname]?sslmode=...
  # or a simple string:
  #   host=localhost port=5432 user=pqgotest password=... sslmode=... dbname=app_production
  #
  # All connection parameters are optional.
  # Without the dbname parameter, the driver will default to a database
  # with the same name as the user. This dbname is just for instantiating a
  # connection with the server and doesn't restrict the databases we are trying
  # to grab metrics for.
  #
  address = "host=localhost user=postgres sslmode=disable"
  # A list of databases to pull metrics about. If not specified, metrics for all
  # databases are gathered.
  # databases = ["app_production", "testing"]
  #
  # Define the toml config where the sql queries are stored
  # New queries can be added, if the withdbname is set to true and there is no
  # databases defined in the 'databases field', the sql query is ended by a 'is
  # not null' in order to make the query succeed.
  # Be careful that the sqlquery must contain the where clause with a part of
  # the filtering, the plugin will add a 'IN (dbname list)' clause if the
  # withdbname is set to true
  # Example :
  # The sqlquery : "SELECT * FROM pg_stat_database where datname" become
  # "SELECT * FROM pg_stat_database where datname IN ('postgres', 'pgbench')"
  # because the databases variable was set to ['postgres', 'pgbench' ] and the
  # withdbname was true.
  # Be careful that if the withdbname is set to false you don't have to define
  # the where clause (aka with the dbname)
  #
  # The script option can be used to specify the .sql file path.
  # If script and sqlquery options specified at same time, sqlquery will be used
  #
  # the tagvalue field is used to define custom tags (separated by comas).
  # the query is expected to return columns which match the names of the
  # defined tags. The values in these columns must be of a string-type,
  # a number-type or a blob-type.
  #
  # Structure :
  # [[inputs.postgresql_extensible.query]]
  #   sqlquery string
  #   version string
  #   withdbname boolean
  #   tagvalue string (coma separated)
  [[inputs.postgresql_extensible.query]]
    sqlquery="SELECT * FROM pg_stat_database where datname"
    version=901
    withdbname=false
    tagvalue=""
  [[inputs.postgresql_extensible.query]]
    script="your_sql-filepath.sql"
    version=901
    withdbname=false
    tagvalue=""
```

The system can be easily extended using homemade metrics collection tools or
using postgresql extensions ([pg_stat_statements](http://www.postgresql.org/docs/current/static/pgstatstatements.html), [pg_proctab](https://github.com/markwkm/pg_proctab) or [powa](http://dalibo.github.io/powa/))

# Sample Queries :
- telegraf.conf postgresql_extensible queries (assuming that you have configured
 correctly your connection)
```
[[inputs.postgresql_extensible.query]]
  sqlquery="SELECT * FROM pg_stat_database"
  version=901
  withdbname=false
  tagvalue=""
[[inputs.postgresql_extensible.query]]
  sqlquery="SELECT * FROM pg_stat_bgwriter"
  version=901
  withdbname=false
  tagvalue=""
[[inputs.postgresql_extensible.query]]
  sqlquery="select * from sessions"
  version=901
  withdbname=false
  tagvalue="db,username,state"
[[inputs.postgresql_extensible.query]]
  sqlquery="select setting as max_connections from pg_settings where \
  name='max_connections'"
  version=801
  withdbname=false
  tagvalue=""
[[inputs.postgresql_extensible.query]]
  sqlquery="select * from pg_stat_kcache"
  version=901
  withdbname=false
  tagvalue=""
[[inputs.postgresql_extensible.query]]
  sqlquery="select setting as shared_buffers from pg_settings where \
  name='shared_buffers'"
  version=801
  withdbname=false
  tagvalue=""
[[inputs.postgresql_extensible.query]]
  sqlquery="SELECT db, count( distinct blocking_pid ) AS num_blocking_sessions,\
  count( distinct blocked_pid) AS num_blocked_sessions FROM \
  public.blocking_procs group by db"
  version=901
  withdbname=false
  tagvalue="db"
[[inputs.postgresql_extensible.query]]
  sqlquery="""
    SELECT type, (enabled || '') AS enabled, COUNT(*)
      FROM application_users
      GROUP BY type, enabled
  """
  version=901
  withdbname=false
  tagvalue="type,enabled"
```

# Postgresql Side
postgresql.conf :
```
shared_preload_libraries = 'pg_stat_statements,pg_stat_kcache'
```

Please follow the requirements to setup those extensions.

In the database (can be a specific monitoring db)
```
create extension pg_stat_statements;
create extension pg_stat_kcache;
create extension pg_proctab;
```
(assuming that the extension is installed on the OS Layer)

 - pg_stat_kcache is available on the postgresql.org yum repo
 - pg_proctab is available at : https://github.com/markwkm/pg_proctab

 ## Views
 - Blocking sessions
```sql
CREATE OR REPLACE VIEW public.blocking_procs AS
 SELECT a.datname AS db,
    kl.pid AS blocking_pid,
    ka.usename AS blocking_user,
    ka.query AS blocking_query,
    bl.pid AS blocked_pid,
    a.usename AS blocked_user,
    a.query AS blocked_query,
    to_char(age(now(), a.query_start), 'HH24h:MIm:SSs'::text) AS age
   FROM pg_locks bl
     JOIN pg_stat_activity a ON bl.pid = a.pid
     JOIN pg_locks kl ON bl.locktype = kl.locktype AND NOT bl.database IS
     DISTINCT FROM kl.database AND NOT bl.relation IS DISTINCT FROM kl.relation
     AND NOT bl.page IS DISTINCT FROM kl.page AND NOT bl.tuple IS DISTINCT FROM
     kl.tuple AND NOT bl.virtualxid IS DISTINCT FROM kl.virtualxid AND NOT
     bl.transactionid IS DISTINCT FROM kl.transactionid AND NOT bl.classid IS
     DISTINCT FROM kl.classid AND NOT bl.objid IS DISTINCT FROM kl.objid AND
      NOT bl.objsubid IS DISTINCT FROM kl.objsubid AND bl.pid <> kl.pid
     JOIN pg_stat_activity ka ON kl.pid = ka.pid
  WHERE kl.granted AND NOT bl.granted
  ORDER BY a.query_start;
```
  - Sessions Statistics
```sql
CREATE OR REPLACE VIEW public.sessions AS
 WITH proctab AS (
         SELECT pg_proctab.pid,
                CASE
                    WHEN pg_proctab.state::text = 'R'::bpchar::text
                      THEN 'running'::text
                    WHEN pg_proctab.state::text = 'D'::bpchar::text
                      THEN 'sleep-io'::text
                    WHEN pg_proctab.state::text = 'S'::bpchar::text
                      THEN 'sleep-waiting'::text
                    WHEN pg_proctab.state::text = 'Z'::bpchar::text
                      THEN 'zombie'::text
                    WHEN pg_proctab.state::text = 'T'::bpchar::text
                      THEN 'stopped'::text
                    ELSE NULL::text
                END AS proc_state,
            pg_proctab.ppid,
            pg_proctab.utime,
            pg_proctab.stime,
            pg_proctab.vsize,
            pg_proctab.rss,
            pg_proctab.processor,
            pg_proctab.rchar,
            pg_proctab.wchar,
            pg_proctab.syscr,
            pg_proctab.syscw,
            pg_proctab.reads,
            pg_proctab.writes,
            pg_proctab.cwrites
           FROM pg_proctab() pg_proctab(pid, comm, fullcomm, state, ppid, pgrp,
             session, tty_nr, tpgid, flags, minflt, cminflt, majflt, cmajflt,
             utime, stime, cutime, cstime, priority, nice, num_threads,
             itrealvalue, starttime, vsize, rss, exit_signal, processor,
             rt_priority, policy, delayacct_blkio_ticks, uid, username, rchar,
             wchar, syscr, syscw, reads, writes, cwrites)
        ), stat_activity AS (
         SELECT pg_stat_activity.datname,
            pg_stat_activity.pid,
            pg_stat_activity.usename,
                CASE
                    WHEN pg_stat_activity.query IS NULL THEN 'no query'::text
                    WHEN pg_stat_activity.query IS NOT NULL AND
                    pg_stat_activity.state = 'idle'::text THEN 'no query'::text
                    ELSE regexp_replace(pg_stat_activity.query, '[\n\r]+'::text,
                       ' '::text, 'g'::text)
                END AS query
           FROM pg_stat_activity
        )
 SELECT stat.datname::name AS db,
    stat.usename::name AS username,
    stat.pid,
    proc.proc_state::text AS state,
('"'::text || stat.query) || '"'::text AS query,
    (proc.utime/1000)::bigint AS session_usertime,
    (proc.stime/1000)::bigint AS session_systemtime,
    proc.vsize AS session_virtual_memory_size,
    proc.rss AS session_resident_memory_size,
    proc.processor AS session_processor_number,
    proc.rchar AS session_bytes_read,
    proc.rchar-proc.reads AS session_logical_bytes_read,
    proc.wchar AS session_bytes_written,
    proc.wchar-proc.writes AS session_logical_bytes_writes,
    proc.syscr AS session_read_io,
    proc.syscw AS session_write_io,
    proc.reads AS session_physical_reads,
    proc.writes AS session_physical_writes,
    proc.cwrites AS session_cancel_writes
   FROM proctab proc,
    stat_activity stat
  WHERE proc.pid = stat.pid;
```
