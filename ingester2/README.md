# ingester2

## Quick run

Set-up empty catalog db:

```bash
mkdir -p /tmp/iox/{wal,obj}

createdb iox_shared
./target/debug/influxdb_iox catalog setup --catalog-dsn postgres:///iox_shared

# there has to exist one "topic", see https://github.com/influxdata/influxdb_iox/issues/6420
psql 'dbname=iox_shared options=-csearch_path=public,iox_catalog' -c "insert into topic (name) values ('iox-shared')"
```


Run ingester2:

```bash
INFLUXDB_IOX_RPC_MODE=2 ./target/debug/influxdb_iox run ingester2 --api-bind=127.0.0.1:8081 --grpc-bind=127.0.0.1:8042 --wal-directory /tmp/iox/wal  --catalog-dsn postgres:///iox_shared --object-store=file --data-dir=/tmp/iox/obj -v 
```

Run router2:

```bash
INFLUXDB_IOX_RPC_MODE=2 ./target/debug/influxdb_iox run router2 --api-bind=127.0.0.1:8080 --grpc-bind=127.0.0.1:8085 --ingester-addresses=127.0.0.1:8042 --catalog-dsn postgres:///iox_shared -v
```

Run querier:

```bash
INFLUXDB_IOX_RPC_MODE=2 ./target/debug/influxdb_iox run querier --ingester-addresses=http://127.0.0.1:8042 --api-bind 127.0.0.1:8083 --grpc-bind 127.0.0.1:8082 --catalog-dsn postgres:///iox_shared --object-store=file --data-dir=/tmp/iox/obj -v
```

