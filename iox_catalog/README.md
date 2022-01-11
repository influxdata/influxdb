# IOx Catalog
This crate contains the code for the IOx Catalog. This includes the definitions of namespaces, their tables, 
the columns of those tables and their types, what Parquet files are in object storage and delete tombstones. 
There's also some configuration information that the overal distributed system uses for operation.

To run this crate's tests you'll need Postgres installed and running locally. You'll also need to set the 
`DATABASE_URL` environment variable so that sqlx will be able to connect to your local DB. For example with 
user and password filled in:

```
DATABASE_URL=postgres://<postgres user>:<postgres password>@localhost/iox_shared
```

You'll then need to create the database and run the migrations. You can do this via the sqlx command line.

```
cargo install sqlx-cli
sqlx database create
sqlx migrate run
```

This will set up the database based on the files in `./migrations` in this crate. SQLx also creates a table 
to keep track of which migrations have been run.