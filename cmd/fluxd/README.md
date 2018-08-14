# Flux

This process provides an implementation of the idpe.Query interface via the network.

# Exposed Metrics

The `fluxd` process exposes a Prometheus endpoint on port `8093` by default.

The following metrics are exposed:

| Metric Name                                 | Prometheus Type | Labels                     | Description                                       |
| -------------                               | --------------- | ---------------            | ---------------                                   |
| `http_api_requests_total`                   | counter         | handler,method,path,status | Number of requests received on the server         |
| `http_api_requests_duration_seconds`        | histogram       | handler,method,path,status | Histogram of times spent on all http requests     |
| `query_control_all_active`                  | gauge           | org                        | Number of active queries in any state             |
| `query_control_all_duration_seconds`        | histogram       | org                        | Histogram of total time spent in all query states |
| `query_control_compiling_active`            | gauge           | org                        | Number of queries actively compiling              |
| `query_control_compiling_duration_seconds`  | histogram       | org                        | Histogram of times spent compiling queries        |
| `query_control_queueing_active`             | gauge           | org                        | Number of queries actively queueing               |
| `query_control_queueing_duration_seconds`   | histogram       | org                        | Histogram of times spent queueing queries         |
| `query_control_requeueing_active`           | gauge           | org                        | Number of queries actively requeueing             |
| `query_control_requeueing_duration_seconds` | histogram       | org                        | Histogram of times spent requeueing queries       |
| `query_control_planning_active`             | gauge           | org                        | Number of queries actively planning               |
| `query_control_planning_duration_seconds`   | histogram       | org                        | Histogram of times spent planning queries         |
| `query_control_executing_active`            | gauge           | org                        | Number of queries actively executing              |
| `query_control_executing_duration_seconds`  | histogram       | org                        | Histogram of times spent executing queries        |

For the `http_api` metrics the `handler` label is `query` for this process.
