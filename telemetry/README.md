## Telemetry Data

Telemetry is first collected by retrieving prometheus data from a Gatherer.
Next, the collected data is filtered by matching a subset of prometheus families.
Finally, the data is transmitted to a prometheus push gateway handler.

The handler enriches the metrics with the timestamp when the data is
received.
