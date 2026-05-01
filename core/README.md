# `oss/core`

`oss/core` contains the lower-level shared crates shared across:

- InfluxDB 3 Core
- InfluxDB 3 Enterprise
- InfluxDB 3 IOx

These crates provide common building blocks such as query execution, schema and data
types, HTTP and gRPC utilities, observability helpers, and supporting Arrow and Parquet
infrastructure.


Examples of crates in this directory include:

- query and planner crates such as `iox_query`, `iox_query_influxql`, and `query_functions`
- shared data model crates such as `data_types`, `schema`, and `mutable_batch`
- service and protocol crates such as `generated_types`, `service_grpc_flight`, and `iox_http`
- common runtime and observability crates such as `executor`, `metric`, `trace`, and `trogging`

As a rule of thumb, code belongs in `core` when it is product-agnostic infrastructure.
