# PgBouncer Input Plugin

The `pgbouncer` plugin provides metrics for your PgBouncer load balancer.

More information about the meaning of these metrics can be found in the
[PgBouncer Documentation](https://pgbouncer.github.io/usage.html).

- PgBouncer minimum tested version: 1.5

### Configuration example

```toml
[[inputs.pgbouncer]]
  ## specify address via a url matching:
  ##   postgres://[pqgotest[:password]]@host:port[/dbname]\
  ##       ?sslmode=[disable|verify-ca|verify-full]
  ## or a simple string:
  ##   host=localhost port=5432 user=pqgotest password=... sslmode=... dbname=app_production
  ##
  ## All connection parameters are optional.
  ##
  address = "host=localhost user=pgbouncer sslmode=disable"
```

#### `address`

Specify address via a postgresql connection string:

  `host=/run/postgresql port=6432 user=telegraf database=pgbouncer`

Or via an url matching:

  `postgres://[pqgotest[:password]]@host:port[/dbname]?sslmode=[disable|verify-ca|verify-full]`

All connection parameters are optional.

Without the dbname parameter, the driver will default to a database with the same name as the user.
This dbname is just for instantiating a connection with the server and doesn't restrict the databases we are trying to grab metrics for.

### Metrics

- pgbouncer
  - tags:
    - db
    - server
  - fields:
    - avg_query_count
    - avg_query_time
    - avg_wait_time
    - avg_xact_count
    - avg_xact_time
    - total_query_count
    - total_query_time
    - total_received
    - total_sent
    - total_wait_time
    - total_xact_count
    - total_xact_time

+ pgbouncer_pools
  - tags:
    - db
    - pool_mode
    - server
    - user
  - fields:
    - cl_active
    - cl_waiting
    - maxwait
    - maxwait_us
    - sv_active
    - sv_idle
    - sv_login
    - sv_tested
    - sv_used

### Example Output

```
pgbouncer,db=pgbouncer,server=host\=debian-buster-postgres\ user\=dbn\ port\=6432\ dbname\=pgbouncer\  avg_query_count=0i,avg_query_time=0i,avg_wait_time=0i,avg_xact_count=0i,avg_xact_time=0i,total_query_count=26i,total_query_time=0i,total_received=0i,total_sent=0i,total_wait_time=0i,total_xact_count=26i,total_xact_time=0i 1581569936000000000
pgbouncer_pools,db=pgbouncer,pool_mode=statement,server=host\=debian-buster-postgres\ user\=dbn\ port\=6432\ dbname\=pgbouncer\ ,user=pgbouncer cl_active=1i,cl_waiting=0i,maxwait=0i,maxwait_us=0i,sv_active=0i,sv_idle=0i,sv_login=0i,sv_tested=0i,sv_used=0i 1581569936000000000
```
