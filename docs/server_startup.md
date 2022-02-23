# Server Startup

An IOx node can be started from the command line:

```shell
influxdb_iox run database
```

See help (via `influxdb_iox run database --help`) for arguments.


## Server ID
Before the server can do anything useful, it needs to have a server ID. There are multiple ways of doing so:

- **CLI Argument:** Pass `--server-id` to the `run` command.
- **Environment Variable:** Set `INFLUXDB_IOX_ID` before starting the server.
- **gRPC:** Use the `UpdateServerId` gRPC call to set the server ID.


## Server Init Process
Once the server ID is know, the server will use the registered object store credentials to load all previously known
database. If there are any errors during the object store inspection (e.g. due to wrong credentials, IO errors,
connectivity errors) the server will be in an error state. The error will be logged and can also be inspected via the
`GetServerStatus` gRPC interface.

You can use the the CLI to wait for the server to be intialized:

```shell
influxdb_iox server wait-server-initalized ...
```


## Database Init Process
For every database that the server has found, it will:

1. load the serialized rules
2. load the preserved catalog
3. start the database

If there is an error during any of these steps, it will be logged and exposed via the `GetServerStatus` gRPC interface.


## Database Recovery
Some database errors can be recovered.

### Preserved Catalog -- Wiping
The preserved catalog can be wiped. For this, the database has either be unknown (hence it will be some kind of garbage
collection / clean up) or the database has to be in an error state. Either use the `WipePreservedCatalog` gRPC interface
or the CLI:

```shell
influxdb_iox database recover wipe ...
```

Once the catalog is wiped, the server will retry to initialize the database. Process will be logged. If the database
init process is successful, the error status within the `GetServerStatus` gRPC response will be cleared.

### Preserved Catalog -- Rebuild
The preserved catalog can be rebuilt from parquet files. Either use the `RebuildPreservedCatalog` gRPC interface
or the CLI:

```shell
influxdb_iox database recover rebuild ...
```

Once the catalog is rebuilt, the server will retry to initialize the database. If the database
init process is successful, the error status within the `GetServerStatus` gRPC response will be cleared.

## Creating a Database from Parquet Files

It is possible to copy parquet files from one IOx server to another and then have IOx reimport them.

### Create a new empty database to receive the data

Create a new database and note the UUID reported (`4fc2236c-7ab8-4200-83c7-f29cd0c2385f` in the example below):

```shell
influxdb_iox database create imported_db

Created database imported_db
4fc2236c-7ab8-4200-83c7-f29cd0c2385f
```

### Shutdown Database

Shutdown the database so we can safely make changes to its data files, but leave the server process running

```
influxdb_iox database shutdown imported_db
```

You can check it has shutdown correctly with

```
$ cargo run -- database list --detailed
    Finished dev [unoptimized + debuginfo] target(s) in 0.17s
     Running `target/debug/influxdb_iox database list --detailed`
+-------------+--------------------------------------+-------------+--------+
| Name        | UUID                                 | State       | Error  |
+-------------+--------------------------------------+-------------+--------+
| imported_db | 4fc2236c-7ab8-4200-83c7-f29cd0c2385f | Shutdown    | <none> |
+-------------+--------------------------------------+-------------+--------+
```

### Copy parquet files into the new database

IOx stores parquet files in `<db_uuid>/data/<table_name>/<partition>`,
and the imported data files must be in the same structure.

For example, if you are running IOx with data directory of `~/.influxdb_iox` the data
for a database with UUID `4fc2236c-7ab8-4200-83c7-f29cd0c2385f` will be found in
`~/.influxdb_iox/dbs/4fc2236c-7ab8-4200-83c7-f29cd0c2385f/data/`

Copy the parquet files you want to import into `<db_uuid>/data`. For example if `my_awesome_table` is a directory that looks like

```
my_awesome_table
  --> '2021-11-30 00:00:00'
    --> c32c5591-8b2d-4ca1-9a34-edc2515a338b.parquet
    --> 82da6914-c16e-459e-8a5f-61c7669073d2.parquet
  --> '2021-11-30 00:01:00'
    --> a8d07291-0546-4fa7-ae93-c2f888c0611a.parquet
```

Copy that directory structure into the database's catalog:

```shell
mkdir -p ~/.influxdb_iox/dbs/4fc2236c-7ab8-4200-83c7-f29cd0c2385f/data
cp -R 'my_awesome_table' ~/.influxdb_iox/dbs/4fc2236c-7ab8-4200-83c7-f29cd0c2385f/data/
```

### Option 1: Copy Catalog

If you wish to construct an exact replica of a remote database, you may wish to copy the upstream catalog. 

To do this, ensure you have copied all the parquet files in the step above, then navigate on your local machine to the `transactions` directory. 

This should contain a single directory with a single `.txn` file

```
├── 4fc2236c-7ab8-4200-83c7-f29cd0c2385f
│ └── transactions
│     └── 00000000000000000000
│         └── 581c2682-a2b2-4d17-918c-65972b702216.txn
```

Delete this file, and copy the contents of `transactions` from the remote server.

You can then start the database up with

```
influxdb_iox database restart imported_db
```

You should then be able to see the chunks

```
influxdb_iox database query imported_db "select * from system.chunks"
```

### Option 2: Rebuild Catalog from Parquet Files

If you did not copy the `transactions` directory as described above, you will need to rebuild the catalog from the parquet files 

To do this run:

```shell
./target/debug/influxdb_iox  database recover rebuild imported_db --force
{
  "operation": {
    "name": "0",
    "metadata": {
      "typeUrl": "type.googleapis.com/influxdata.iox.management.v1.OperationMetadata",
      "value": "GAEgAaIBDQoLaW1wb3J0ZWRfZGI="
    }
  },
  "metadata": {
    "totalCount": "1",
    "pendingCount": "1",
    "rebuildPreservedCatalog": {
      "dbName": "imported_db"
    }
  }
```

_You can omit the `--force` option if either the remote or local server is using static configuration provided by `--config-file`_

You can check on the status with `./target/debug/influxdb_iox operation list`

When the `rebuildPreservedCatalog` operation has completed, the newly imported tables should be visible, with a command such as

```shell
./target/debug/influxdb_iox  database query imported_db 'show tables'
+---------------+--------------------+---------------------------------+------------+
| table_catalog | table_schema       | table_name                      | table_type |
+---------------+--------------------+---------------------------------+------------+
| public        | iox                | My Awesome Imported Table Name  | BASE TABLE |
| public        | system             | chunks                          | BASE TABLE |
| public        | system             | columns                         | BASE TABLE |
| public        | system             | chunk_columns                   | BASE TABLE |
| public        | system             | operations                      | BASE TABLE |
| public        | system             | persistence_windows             | BASE TABLE |
| public        | information_schema | tables                          | VIEW       |
| public        | information_schema | columns                         | VIEW       |
+---------------+--------------------+---------------------------------+------------+
```
