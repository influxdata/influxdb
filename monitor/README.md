# System Monitoring
_System Monitoring_ means all statistical and diagnostic information made availabe to the user of InfluxDB system, about the system itself. Its purpose is to assist with troubleshooting and performance analysis.

## Supported Commands

 * `SHOW STATS`
 * `SHOW DIAGNOSTICS`

## Statistics vs. Diagnostics
A distinction between _statistics_ and _diagnostics_ is made for the purposes of monitoring. Generally a statistical quality is something that is being counted, and for which it makes sense to store for historical analysis. Diagnostic information is not necessarily numerical, and may not make sense to store. An example of statistical information would be the number of points received over UDP, or the number of queries executed. Examples of diagnostic information would be a list of current Graphite TCP connections, the version of InfluxDB, and the uptime of the process.

## Design and Implementation

A new module named `monitor` supports all statistics and diagnostic functionality. This includes:

 * Allowing other modules to register statistics and diagnotics information, allowing it to be accessed on demand by the `monitor` module.
 * Serving the statistics and diagnostic information to the user, in response to commands such as `SHOW DIAGNOSTICS`.
 * Expose standard Go runtime information such as garbage collection statistics.
 * Writing the statistical information to an InfluxDB system, for historical analysis. This may be the same system generating the statistical information, but it does not have to be.

To register with the `monitor` service, a module must implement the following interface:

```
import "expvar"

type Client interface {
    Statistics() (expvar.Map, error)
    Diagnostics() (map[string]interface{}, error)
}
```

### expvar
Statistical information is gathered by each package using [expvar](https://golang.org/pkg/expvar). To prevent namespace collision during registration with `expvar`, each module registers using the format `influxdb_<module>`. For example the `influxdb/tsdb` package will register with `expvar` using the key `influxdb_tsdb`.

Due to the nature of `expvar`, statistical information is reset to its initial state when a server is restarted,

## Configuration
The `monitor` module will allow the following configuration:

 * Whether to write statistical and diagnostic information to an InfluxDB system. This is enabled by default.
 * The name of the database to where this information should be written. Defaults to `_internal`. The information is written to the default retention policy for the given database.
 * The address and port of the InfluxDB system. This will default to the system generating the data.
 * The rate at which this information should be written. The maximum rate will be once-per-minute.
