# Server Startup

An IOx node can be started from the command line:

```shell
influxdb_iox run
```

See help (via `influxdb_iox run --help`) for arguments.


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
influxdb_iox database catalog wipe ...
```

Once the catalog is wiped, the server will retry to initialize the database. Process will be logged. If the database
init process is successful, the error status within the `GetServerStatus` gRPC response will be cleared.
