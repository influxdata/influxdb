# PostgreSQL plugin

This postgresql plugin provides metrics for your postgres database. It currently works with postgres versions 8.1+. It uses data from the built in _pg_stat_database_ and pg_stat_bgwriter views. The metrics recorded depend on your version of postgres. See table:
```
pg version      9.2+   9.1   8.3-9.0   8.1-8.2   7.4-8.0(unsupported)
---             ---    ---   -------   -------   -------
datid            x      x       x         x
datname          x      x       x         x
numbackends      x      x       x         x         x
xact_commit      x      x       x         x         x
xact_rollback    x      x       x         x         x
blks_read        x      x       x         x         x
blks_hit         x      x       x         x         x
tup_returned     x      x       x
tup_fetched      x      x       x
tup_inserted     x      x       x
tup_updated      x      x       x
tup_deleted      x      x       x
conflicts        x      x
temp_files       x
temp_bytes       x
deadlocks        x
blk_read_time    x
blk_write_time   x
stats_reset*     x      x
```

_* value ignored and therefore not recorded._


More information about the meaning of these metrics can be found in the [PostgreSQL Documentation](http://www.postgresql.org/docs/9.2/static/monitoring-stats.html#PG-STAT-DATABASE-VIEW)

## Configuration
Specify address via a postgresql connection string:

  `host=localhost port=5432 user=telegraf database=telegraf`

Or via an url matching:

  `postgres://[pqgotest[:password]]@host:port[/dbname]?sslmode=[disable|verify-ca|verify-full]`

All connection parameters are optional. Without the dbname parameter, the driver will default to a database with the same name as the user. This dbname is just for instantiating a connection with the server and doesn't restrict the databases we are trying to grab metrics for.

A  list of databases to explicitly ignore.  If not specified, metrics for all databases are gathered.  Do NOT use with the 'databases' option.

  `ignored_databases = ["postgres", "template0", "template1"]`

A list of databases to pull metrics about. If not specified, metrics for all databases are gathered.  Do NOT use with the 'ignored_databases' option.

  `databases = ["app_production", "testing"]`

### TLS Configuration

Add the `sslkey`, `sslcert` and `sslrootcert` options to your DSN:
```
host=localhost user=pgotest dbname=app_production sslmode=require sslkey=/etc/telegraf/key.pem sslcert=/etc/telegraf/cert.pem sslrootcert=/etc/telegraf/ca.pem
```

### Configuration example
```
[[inputs.postgresql]]
  address = "postgres://telegraf@localhost/someDB"
  ignored_databases = ["template0", "template1"]
```
