# Transpilerd

Transpilerd is daemon that can execute queries from various source languages by transpiling the query and tranforming the result.

# Exposed Metrics

The `transpilerd` process exposes a Prometheus endpoint on port `8098` by default.

The following metrics are exposed:

| Metric Name                                 | Prometheus Type | Labels                     | Description
| -------------                               | --------------- | ---------------            | ---------------                               |
| `http_api_requests_total`                   | counter         | handler,method,path,status | Number of requests received on the server     |
| `http_api_requests_duration_seconds`        | histogram       | handler,method,path,status | Histogram of times spent on all http requests |

