# InfluxDB Edge

> [!NOTE]
> On 2023-09-21 this repo changed the default branch from master to main. At the same time, we moved all InfluxDB 2.x development into the main-2.x branch. If you relied on the 2.x codebase in the former master branch, update your tooling to point to main-2.x, which is the new home for any future InfluxDB 2.x development. This branch (main) is now the default branch for this repo and is for development of InfluxDB 3.x.
>
> For now, this means that InfluxDB 3.0 and its upstream dependencies are the focus of our open source efforts. We continue to support both the 1.x and 2.x versions of InfluxDB for our customers, but our new development efforts are now focused on 3.x. The remainder of this readme has more details on 3.0 and what you can expect.

InfluxDB is an open source time series database written in Rust, using Apache Arrow, Apache Parquet, and Apache DataFusion as its foundational building blocks. This latest version (3.x) of InfluxDB focuses on providing a real-time buffer for observational data of all kinds (metrics, events, logs, traces, etc.) that is queryable via SQL or InfluxQL, and persisted in bulk to object storage as Parquet files, which other third-party systems can then use. It is able to run either with a write ahead log or completely off object storage if the write ahead log is disabled (in this mode of operation there is a potential window of data loss for any data buffered that has not yet been persisted to object store).

The open source project runs as a standalone system in a single process. If you're looking for a clustered, distributed time series database with a bunch of enterprise security features, we have a commercial offering available as a managed hosted service or as on-premise software designed to run inside Kubernetes. The distributed version also includes functionality to reorganize the files in object storage for optimal query performance. In the future, we intend to have a commercial version of the single server software that adds fine-grained security, federated query capabilities, file reorganization for query optimization and deletes, and integration with other systems.

## Project Status

Currently this project is under active prototype development without documentation or official builds. This README will be updated with getting started details and links to docs when the time comes.

## Roadmap

The scope of this open source InfluxDB 3.0 is different from either InfluxDB 1.x or 2.x. This may change over time, but for now here are the basics of what we have planned:

* InfluxDB 1.x and 2.x HTTP write API (supporting Line Protocol)
* InfluxDB 1.x HTTP query API (InfluxQL)
* Flight SQL (query API using SQL)
* InfluxQL over Flight
* Data migration tooling for InfluxDB 1.x & 2.x to 3.0
* InfluxDB 3.0 HTTP write API (a new way to write data with a more expressive data model than 1.x or 2.x)
* InfluxDB 3.0 HTTP query API (send InfluxQL or SQL queries as an HTTP GET and get back JSON lines, CSV, or pretty print response)
* Persist event stream (subscribe to the Parquet file persist events, useful for downstream clients to pick up files from object store)
* Embedded VM (either Python, Javascript, WASM, or some combination thereof)
  * Individual queries
  * Triggers on write
  * On persist (run whatever is being persisted through script)
  * On schedule
* Bearer token authentication (all or nothing, token is set at startup through env variable, more fine-grained security is outside the scope of the open source effort)

What this means is that InfluxDB 3.0 can be pointed to as though it is an InfluxDB 1.x server with most of the functionality present. For InfluxDB 2.x users that primarily interact with the database through the InfluxQL query capability, they will also be able to use this database in a similar way. Version 3.0 will not be implementing the rest of the 2.x API natively, although there could be separate processes that could be added on at some later date that would provide that functionality.

## Flux

Flux is the custom scripting and query language we developed as part of our effort on InfluxDB 2.0. While we will continue to support Flux for our customers, it is noticeably absent from the description of InfluxDB 3.0. Written in Go, we built Flux hoping it would get broad adoption and empower users to do things with the database that were previously impossible. While we delivered a powerful new way to work with time series data, many users found Flux to be an adoption blocker for the database.

We spent years of developer effort on Flux starting in 2018 with a small team of developers. However, the size of the effort, including creating a new language, VM, query planner, parser, optimizer and execution engine, was significant. We ultimately weren’t able to devote the kind of attention we would have liked to more language features, tooling, and overall usability and developer experience. We worked constantly on performance, but because we were building everything from scratch, all the effort was solely on the shoulders of our small team. We think this ultimately kept us from working on the kinds of usability improvements that would have helped Flux get broader adoption.

For InfluxDB 3.0 we adopted Apache Arrow DataFusion, an existing query parser, planner, and executor as our core engine. That was in mid-2020, and over the course of the last three years, there have been significant contributions from an active and growing community. While we remain major contributors to the project, it is continuously getting feature enhancements and performance improvements from a worldwide pool of developers. Our efforts on the Flux implementation would simply not be able to keep pace with the much larger group of DataFusion developers.

With InfluxDB 3.0 being a ground-up rewrite of the database in a new language (from Go to Rust), we weren’t able to bring the Flux implementation along. For InfluxQL we were able to support it natively by writing a language parser in Rust and then converting InfluxQL queries into logical plans that our new native query engine, Apache Arrow DataFusion, can understand and process. We also had to add new capabilities to the query engine to support some of the time series queries that InfluxQL enables. This is an effort that took a little over a year and is still ongoing. This approach means that the contributions to DataFusion become improvements to InfluxQL as well given it is the underlying engine.

Initially, our plan to support Flux in 3.0 was to do so through a lower level API that the database would provide. In our Cloud2 product, Flux processes connect to the InfluxDB 1 & 2 TSM storage engine through a gRPC API. We built support for this in InfluxDB 3.0 and started testing with mirrored production workloads. We quickly found that this interface performed poorly and had unforeseen bugs, eliminating it as a viable option for Flux users to bring their scripts over to 3.0. This is due to the API being designed around the TSM storage engine’s very specific format, which the 3.0 engine is unable to serve up as quickly.

We’ll continue to support Flux for our users and customers. But given Flux is a scripting language in addition to being a query language, planner, optimizer, and execution engine, a Rust-native version of it is likely out of reach. And because the surface area of the language is so large, such an effort would be unlikely to yield a version that is compatible enough to run existing Flux queries without modification or rewrites, which would eliminate the point of the effort to begin with.

For Flux to have a path forward, we believe the best plan is to update the core engine so that it can use Flight SQL to talk to InfluxDB 3.0. This would make an architecture where independent processes that serve the InfluxDB 2.x query API (i.e. Flux) would be able to convert whatever portion of a Flux script that is a query into a SQL query that gets sent to the InfluxDB 3.0 process with the result being post-processed by the Flux engine.

This is likely not a small effort as the Flux engine is built around InfluxDB 2.0's TSM storage engine and the representation of all data as individual time series. InfluxDB 3.0 doesn't keep a concept of series so the SQL query would either have to do a bunch of work to return individual series, or the Flux engine would do work with the resulting query response to construct the series. For the moment, we’re focused on improvements to the core SQL and (and by extension InfluxQL) query engine and experience both in InfluxDB 3.0 and DataFusion.

We may come back to this effort in the future, but we don’t want to stop the community from self-organizing an effort to bring Flux forward. The Flux runtime and language exists as permissively licensed open source [here](https://github.com/InfluxCommunity/flux). We've also created a community fork of Flux where the community can self-organize and move development forward without requiring our code review process. There are already a few community members working on this potential path forward. If you're interested in helping with this effort, please speak up on this tracked issue.

We realize that Flux still has an enthusiastic, if small, user base and we’d like to figure out the best path forward for these users. For now, with our limited resources, we think focusing our efforts on improvements to Apache Arrow DataFusion and InfluxDB 3.0’s usage of it is the best way to serve our users that are willing to convert to either InfluxQL or SQL. In the meantime, we’ll continue to maintain Flux with security and critical fixes for our users and customers.

## Install

> [!NOTE]
> InfluxDB Edge is pre-release and in active development. Build artifacts are not yet generally available and official installation instructions will be forthcoming as InfluxDB Edge approaches release. For the time being, a Dockerfile is provided and can be adapted or used for inspiration by intrepid users.

## Getting Started

> [!NOTE]
> InfluxDB Edge is pre-release and in active development. These usage instructions are only designed for a very quick start and subject to change.

The following assumes that `influxdb3` exists in your current directory. Please adjust as necessary for your environment. Help can be seen with `influxdb3 --help`.

### Starting the server

`influxdb3` has many different configuration options, which may be controlled via command line arguments or environment variable. See `influxdb3 serve --help` for details.

Eg, to start the server with a local filesystem object store (default listens on port 8181 with no authentication):
```
$ export INFLUXDB_IOX_OBJECT_STORE=file
$ export INFLUXDB_IOX_DB_DIR=/path/to/influxdb3
$ influxdb3 serve
2024-05-10T15:54:24.446764Z  INFO influxdb3::commands::serve: InfluxDB3 Edge server starting git_hash=v2.5.0-14032-g1b7cd1976d65bc7121df7212cb234fca5c5fa899 version=0.1.0 uuid=3c0e0e61-ae4e-46cb-bba3-9ccd7b5feecd num_cpus=20 build_malloc_conf=
2024-05-10T15:54:24.446947Z  INFO clap_blocks::object_store: Object Store db_dir="/path/to/influxdb3" object_store_type="Directory"
2024-05-10T15:54:24.447035Z  INFO influxdb3::commands::serve: Creating shared query executor num_threads=20
...
```

### Interacting with the server
The `influxdb3` binary is also used as a client.

#### Writing
```
# write help
$ influxdb3 write --help
...

# create some points in a file for the client to write from using line protocol
$ cat > ./file.lp <<EOM
mymeas,mytag1=sometag value=0.54 $(date +%s%N)
mymeas,mytag1=sometag value=0.55 $(date +%s%N)
EOM

# perform the write
$ influxdb3 write --dbname mydb -f ./file.lp
success
```

See the [InfluxDB documentation](https://docs.influxdata.com/influxdb/cloud-dedicated/reference/syntax/line-protocol/) for details on line protocol.

`curl` can also be used with the `/api/v3/write_lp` API (subject to change):
```
$ export URL="http://localhost:8181"
$ curl -s -X POST "$URL/api/v3/write_lp?db=mydb" --data-binary @file.lp
```

#### Querying
```
# query help
$ influxdb3 query --help
...

# perform a query with SQL
$ influxdb3 query --dbname mydb "SELECT * from mymeas"
+------------------------------------------------------------------+---------+-------------------------------+-------+
| _series_id                                                       | mytag1  | time                          | value |
+------------------------------------------------------------------+---------+-------------------------------+-------+
| 05a045398acb0754a6551b74ca09d180d7e452d72b4f196da665490a6dd9e788 | sometag | 2024-05-09T21:08:52.227359715 | 0.54  |
| 05a045398acb0754a6551b74ca09d180d7e452d72b4f196da665490a6dd9e788 | sometag | 2024-05-09T21:08:52.229494943 | 0.55  |
+------------------------------------------------------------------+---------+-------------------------------+-------+

# perform a query with InfluxQL
$ influxdb3 query --lang influxql --dbname mydb "SELECT * from mymeas"
+------------------+-------------------------------+------------------------------------------------------------------+---------+-------+
| iox::measurement | time                          | _series_id                                                       | mytag1  | value |
+------------------+-------------------------------+------------------------------------------------------------------+---------+-------+
| mymeas           | 2024-05-09T21:08:52.227359715 | 05a045398acb0754a6551b74ca09d180d7e452d72b4f196da665490a6dd9e788 | sometag | 0.54  |
| mymeas           | 2024-05-09T21:08:52.229494943 | 05a045398acb0754a6551b74ca09d180d7e452d72b4f196da665490a6dd9e788 | sometag | 0.55  |
+------------------+-------------------------------+------------------------------------------------------------------+---------+-------+
```

`curl` can also be used with the `/api/v3/query_sql` and `/api/v3/query_influxql` APIs (subject to change):
```
$ export URL="http://localhost:8181"
$ curl -s -H "Content-Type: application/json" \
    -X POST "$URL/api/v3/query_sql" \
    --data-binary '{"db": "mydb", "q": "SELECT * from mymeas"}' | jq
[
  {
    "_series_id": "05a045398acb0754a6551b74ca09d180d7e452d72b4f196da665490a6dd9e788",
    "mytag1": "sometag",
    "time": "2024-05-09T21:08:52.227359715",
    "value": 0.54
  },
  {
    "_series_id": "05a045398acb0754a6551b74ca09d180d7e452d72b4f196da665490a6dd9e788",
    "mytag1": "sometag",
    "time": "2024-05-09T21:08:52.229494943",
    "value": 0.55
  }
]
```

By default, `application/json` is returned. This can be adjusted via the `format` key which can be one of `json` (`application/json`; default), `pretty` (`application/plain`) or `parquet` (`application/vnd.apache.parquet`). Eg, `influxql` with `pretty` format:
```
$ curl -s -H "Content-Type: application/json" \
    -X POST "$URL/api/v3/query_influxql" \
    --data-binary '{"db": "mydb", "q": "SELECT * from mymeas", "format": "pretty"}'
+------------------+-------------------------------+------------------------------------------------------------------+---------+-------+
| iox::measurement | time                          | _series_id                                                       | mytag1  | value |
+------------------+-------------------------------+------------------------------------------------------------------+---------+-------+
| mymeas           | 2024-05-09T21:08:52.227359715 | 05a045398acb0754a6551b74ca09d180d7e452d72b4f196da665490a6dd9e788 | sometag | 0.54  |
| mymeas           | 2024-05-09T21:08:52.229494943 | 05a045398acb0754a6551b74ca09d180d7e452d72b4f196da665490a6dd9e788 | sometag | 0.55  |
+------------------+-------------------------------+------------------------------------------------------------------+---------+-------+
```

### Enabling HTTP authorization
By default, `influxdb3` allows all HTTP requests and supports authorization via a single "all or nothing" authorization token. When the server is started with `--bearer-token` (or `INFLUXDB3_BEARER_TOKEN=...` in its environment), the client then needs to provide an `Authorization: Bearer <token>` header with each HTTP request. The server will authorize the request by calculating the SHA512 of the token and check that it matches the value specified with `--bearer-token/INFLUXDB3_BEARER_TOKEN`. The `influxdb create token` command can be used to help with this. Eg:
```
$ influxdb3 create token
Token: apiv3_<token>
Hashed Token: <sha512 of raw token>

Start the server with `influxdb3 serve --bearer-token "<sha512 of raw token>"`

HTTP requests require the following header: "Authorization: Bearer apiv3_<token>"
This will grant you access to every HTTP endpoint or deny it otherwise
```

When the server is started in this way, all HTTP requests must provide the correct authorization header. Eg:
```
# write
$  export TOKEN="apiv3_<token>"
$ influxdb3 w --dbname mydb --token "$TOKEN" -f ./file.lp
success

# perform a query with SQL using influxdb3 client
$ influxdb3 q --dbname mydb --token "$TOKEN" "SELECT * from mymeas"

# perform a query with SQL using curl
$ export URL="http://localhost:8181"
$ curl -s -H "Content-Type: application/json" \
    -H "Authorization: Bearer $TOKEN" \
    -X POST "$URL/api/v3/query_sql" \
    --data-binary '{"db": "mydb", "q": "SELECT * from mymeas", "format": "pretty"}'
```
