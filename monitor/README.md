# System Monitoring
_System Monitoring_ means all statistical and diagnostic information made availabe to the user of InfluxDB system, about the system itself. Its purpose is to assist with troubleshooting and performance analysis.

## Supported Commands

 * `SHOW STATS`
 * `SHOW DIAGNOSTICS`

If statistical information is also written to an InfluxDB system, the data will also be queryable by the InfluxQL query language.

## Statistics vs. Diagnostics
A distinction between _statistics_ and _diagnostics_ is made for the purposes of monitoring. Generally a statistical quality is something that is being counted, and for which it makes sense to store for historical analysis. Diagnostic information is not necessarily numerical, and may not make sense to store.

An example of statistical information would be the number of points received over UDP, or the number of queries executed. Examples of diagnostic information would be a list of current Graphite TCP connections, the version of InfluxDB, or the uptime of the process.

## Design and Implementation

A new module named `monitor` supports all statistics and diagnostic functionality. This includes:

 * Allowing other modules to register statistics and diagnostics information, allowing it to be accessed on demand by the `monitor` module.
 * Serving the statistics and diagnostic information to the user, in response to commands such as `SHOW DIAGNOSTICS`.
 * Expose standard Go runtime information such as garbage collection statistics.
 * Make all collected expvar data via HTTP, for collection by 3rd-party tools.
 * Writing the statistical information to an InfluxDB system, for historical analysis. This may be the same system generating the statistical information, but it does not have to be. Information is written used the Line Protocol.

To register with `monitor`, a module must implement the following interface:

```
type Client interface {
    Statistics() (map[string]interface{}, error)
    Diagnostics() (map[string]interface{}, error)
}
```

The module then calls `Register(name string, tags map[string]string, client Client)`. `name` is the Measurement name that will be associated with the statistics. `tags` will be the tags, though an empty map is acceptable. `client` is the module which implements the `Client` interface.

### expvar
Statistical information is gathered by each package using [expvar](https://golang.org/pkg/expvar). Each package registers a map using its package name.

Due to the nature of `expvar`, statistical information is reset to its initial state when a server is restarted.

## Configuration
The `monitor` module will allow the following configuration:

 * Whether to write statistical and diagnostic information to an InfluxDB system. This is enabled by default.
 * The name of the database to where this information should be written. Defaults to `_internal`. The information is written to the default retention policy for the given database.
 * The name of the retention policy, along with full configuration control of the retention policy.
 * The address and port of the InfluxDB system. This will default to the system generating the data.
 * The rate at which this information should be written. The maximum rate will be once a second.
