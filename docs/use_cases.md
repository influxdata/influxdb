# Use Cases

Chronograf works with the other components of the TICK stack to provide a user interface for monitoring and alerting on your infrastructure.
This document describes common setup use cases for Chronograf.

## Use Case 1: Setup for Monitoring Several Servers

Suppose you want to use Chronograf to monitor several servers.
This section describes a simple setup for monitoring CPU, disk, and memory usage on three servers.

### Architecture Overview

![Setup Diagram](https://github.com/influxdata/chronograf/blob/use-case/docs/images/set-up-diagram.png)

Each of the three servers has its own [Telegraf](https://github.com/influxdata/telegraf) instance.
Those instances are configured to collect CPU, disk, and memory data using Telegraf's [system stats](https://github.com/influxdata/telegraf/tree/master/plugins/inputs/system) input plugin.
Each Telegraf instance is also configured to send those data to a single [InfluxDB](https://github.com/influxdata/influxdb) instance.
When Telegraf sends data to InfluxDB, it automatically [tags](https://docs.influxdata.com/influxdb/latest/concepts/glossary/#tag) those data with the relevant server's hostname.

The single InfluxDB instance is connected to Chronograf.
Chronograf uses the `host` tag in the Telegraf data to populate the [HOST LIST](https://github.com/influxdata/chronograf/blob/master/docs/GETTING_STARTED.md#host-list) page and provide other hostname-specific information in the user interface.

### Setup Description

To start out, we [install and start](https://github.com/influxdata/chronograf/blob/master/docs/INSTALLATION.md#influxdb-setup) InfluxDB on a separate server.
We recommend installing InfluxDB on its own [machine](https://docs.influxdata.com/influxdb/latest/guides/hardware_sizing/) for performance purposes.
InfluxDB's default [configuration](https://docs.influxdata.com/influxdb/latest/administration/config/) doesn't require any adjustments for this particular use case.

Next, we [install](https://www.influxdata.com/downloads/) Telegraf on each server that we want to monitor.
Before starting the three Telegraf services we need to make some edits to Telegraf's [configuration file](https://github.com/influxdata/telegraf/blob/master/docs/CONFIGURATION.md) (`/etc/telegraf/telegraf.conf`).
First, we configure each instance to use the system stats plugin to collect CPU, disk, and memory data.
The system stats plugin is actually enabled by default so there's no additional work to do here.
We just double check that `[[inputs.cpu]]`, `[[inputs.disk]]`, and `[[inputs.mem]]` are uncommented in the `INPUT PLUGINS` section of Telegraf's configuration file:

```
###############################################################################
#                            INPUT PLUGINS                                    #
###############################################################################

# Read metrics about cpu usage
[[inputs.cpu]] #✅
  ## Whether to report per-cpu stats or not
  percpu = true
  ## Whether to report total system cpu stats or not
  totalcpu = true
  ## If true, collect raw CPU time metrics.
  collect_cpu_time = false


# Read metrics about disk usage by mount point
[[inputs.disk]] #✅
  ## By default, telegraf gather stats for all mountpoints.
  ## Setting mountpoints will restrict the stats to the specified mountpoints.
  # mount_points = ["/"]

  ## Ignore some mountpoints by filesystem type. For example (dev)tmpfs (usually
  ## present on /run, /var/run, /dev/shm or /dev).
  ignore_fs = ["tmpfs", "devtmpfs"]

[...]

# Read metrics about memory usage
[[inputs.mem]] #✅
  # no configuration
```

Our next edit to Telegraf's configuration file ensures that each Telegraf instance sends data to our single InfluxDB instance.
To do this, we edit the `urls` setting in the `OUTPUT PLUGINS` section to point to the IP of our InfluxDB instance:

```
###############################################################################
#                            OUTPUT PLUGINS                                   #
###############################################################################

# Configuration for influxdb server to send metrics to
[[outputs.influxdb]]
  ## The full HTTP or UDP endpoint URL for your InfluxDB instance.
  ## Multiple urls can be specified as part of the same cluster,
  ## this means that only ONE of the urls will be written to each interval.
  # urls = ["udp://localhost:8089"] # UDP endpoint example
  urls = ["http://<InfluxDB-IP>:8086"] # 💥 Edit here!💥
  ## The target database for metrics (telegraf will create it if not exists).
  database = "telegraf" # required
```
Now that we've configured our inputs and outputs, we [start](https://github.com/influxdata/chronograf/blob/master/docs/INSTALLATION.md#2-start-telegraf) the Telegraf service on all three servers.
Telegraf begins by creating a database in InfluxDB called `telegraf` (that name is configurable), and Telegraf starts writing system stats data to that database.
Note that Telegraf automatically creates a `host` [tag](https://docs.influxdata.com/influxdb/latest/concepts/glossary/#tag) that records the hostname of the server that sent the data.
Here's a sample of some CPU usage data in InfluxDB:

```
name: cpu
time                   usage_idle          host <--- Telegraf's auto-generated tag
----                   ----------          ----
2016-11-29T22:41:00Z   99.70000000000253   server-01
2016-11-29T22:41:00Z   99.79959919839698   server-02
2016-11-29T22:41:00Z   98.1037924151472    server-03
2016-11-29T22:41:10Z   99.60000000000036   server-01
2016-11-29T22:41:10Z   99.49698189131892   server-02
2016-11-29T22:41:10Z   99.6996996996977    server-03
2016-11-29T22:41:20Z   98.89889889889365   server-01
2016-11-29T22:41:20Z   99.40119760479097   server-02
2016-11-29T22:41:20Z   99.60039960039995   server-03
```

Finally, we [install and start](https://github.com/influxdata/chronograf/blob/master/docs/INSTALLATION.md#chronograf-setup) Chronograf.
Once we [connect](https://github.com/influxdata/chronograf/blob/master/docs/INSTALLATION.md#3-connect-to-chronograf) Chronograf to our InfluxDB
instance, Chronograf uses Telegraf's `host` tag to populate the [HOST LIST](https://github.com/influxdata/chronograf/blob/master/docs/GETTING_STARTED.md#host-list) page:

![Host List](https://github.com/influxdata/chronograf/blob/use-case/docs/images/host-list-usecase.png)

The system stats dashboard template shows the CPU, Disk, and Memory metrics for the selected hostname:

![Dashboard Template](https://github.com/influxdata/chronograf/blob/use-case/docs/images/template-dashboard-usecase.png)

Finally, you can create queries in the [Data Explorer](https://github.com/influxdata/chronograf/blob/master/docs/GETTING_STARTED.md#data-explorer) that graph results per hostname:

![Dashboard Template](https://github.com/influxdata/chronograf/blob/use-case/docs/images/group-by-usecase.png)

## Use Case 2: Setup the TICK Stack in a Kubernetes Instance

Check out our 20-minute [webinar](https://vimeo.com/193632831) for how to spin up the TICK Stack in a Kubernetes instance.
