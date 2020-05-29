## Telemetry Server

Telemetry server accepts pushed prometheus metrics where it
logs them to stdout.

Telemetry server is very similar to prometheus pushgateway, but,
has stores that are configurable rather than just a /metrics
endpoint.
