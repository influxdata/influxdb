# Getting Started Guide

Chronograf is the a user interface that makes owning the monitoring and alerting for your infastructure easy to setup and maintain. Chronograf also is the inteface that ties together the other three components of the InfluxData TICK stack. In order to get the most of out Chronograf you will need to set up all four packages of the TICK stack. This guide will help you get each going as quickly as possible so you can begin monitoring with as little configuration and code as possible. This guide will point you directly to the downloads for each package involved and will give sample configurations for each. If you would like to read more about each you can follow the links here.

[Telegraf](https://github.com/influxdata/telegraf)
[Telegraf Documentation](https://docs.influxdata.com/telegraf/v1.1)

[InfluxDB](https://github.com/influxdata/influxdb)
[InfluxDB Documentation](https://docs.influxdata.com/influxdb/v1.0)

[Chronograf](https://github.com/influxdata/chronograf)
[Chronograf Documentation](https://docs.influxdata.com/chronograf/v1.1)

[Kapacitor](https://github.com/influxdata/kapacitor)
[Kapactior Documentation](https://docs.influxdata.com/kapacitor/v1.1)

## Operating system support
Chronograf as well of the other components of the TICK stack are supported on a large number of operating systems and hardware architectues. This guide will walk you through getting set up on a Ubuntu 16.04 installation. Most of the document will be applicable to most flavors of Linux, check our [downloads](https://www.influxdata.com/downloads/) page for links to the binaries of your choice.

## InfluxDB setup

First download the deb package for InfluxDB 1.1.0rc1 `wget https://dl.influxdata.com/influxdb/releases/influxdb_1.1.0~rc1_amd64.deb` then run the dpkg installer `sudo dpkg -i influxdb_1.1.0~rc1_amd64.deb`. The default configuration for InfluxDB is fine for this guide so now just start the database `sudo systemctl start influxdb`. Once this is done you can test that its working by running the `SHOW DATABASES` command using curl `curl "http://localhost:8086/query?q=show+databases"`. This should return an InfluxDB object that will probably only contain the `_internal` database, it should look like:
```json
{"results":[{"series":[{"name":"databases","columns":["name"],"values":[["_internal"]]}]}]}
```
So far so good, you don't need to create your own database, the other components will do that for you.

## Kapacitor setup 

Next we are going to set up the data processing platform Kapacitor. Kapacitor is resonsible for creating and sending alerts in Chronograf. You will first need to download and install the binaries `wget https://dl.influxdata.com/kapacitor/releases/kapacitor_1.1.0~rc2_amd64.deb` and `sudo dpkg -i kapacitor_1.1.0~rc2_amd64.deb`. Once the package is installed you need to start the process `sudo systemctl start kapacitor`. You can test that this has worked by checking the `task` list of Kapacitor `kapacitor list tasks` this should return an empty list of tasks, so you will probably just see the header `ID                            Type      Status    Executing Databases and Retention Policies`. If there was a problem you will see an error message like `Get http://localhost:9092/kapacitor/v1/tasks?dot-view=attributes&fields=type&fields=status&fields=executing&fields=dbrps&limit=100&offset=0&pattern=&replay-id=&script-format=formatted: dial tcp [::1]:9092: getsockopt: connection refused`.

## Telegraf setup

Now you need to set up Telegraf. Telegraf is the meterics gathering agent and thus would normally be installed on your servers and woudl point the output to the machine you put InfluxDB on, however, for this example we will monoitor this machine. Each application you want to monitor will need to have a Telegraf input configured for it. At the end of this guide we will provide sample configurations for additional applications. This guide will walk you through setting up basic system stats. First download, install and start the binary `wget https://dl.influxdata.com/telegraf/releases/telegraf_1.1.0~rc1_amd64.deb` then `sudo dpkg -i telegraf_1.1.0~rc1_amd64.deb` and `sudo systemctl start telegraf`. This should create a configuration file at `/etc/telegraf/telegraf.conf`. The default configuration file should give us the system stats, but lets check out the file to be sure. We are interested in the `outputs.influxdb` and the configured `inputs`. The outputs should look like:
```
[[outputs.influxdb]]
  ## The full HTTP or UDP endpoint URL for your InfluxDB instance.
  ## Multiple urls can be specified as part of the same cluster,
  ## this means that only ONE of the urls will be written to each interval.
  # urls = ["udp://localhost:8089"] # UDP endpoint example
  urls = ["http://localhost:8086"] # required
  ## The target database for metrics (telegraf will create it if not exists).
  database = "telegraf" # required

  ## Retention policy to write to. Empty string writes to the default rp.
  retention_policy = ""
  ## Write consistency (clusters only), can be: "any", "one", "quorum", "all"
  write_consistency = "any"

  ## Write timeout (for the InfluxDB client), formatted as a string.
  ## If not provided, will default to 5s. 0s means no timeout (not recommended).
  timeout = "5s"
  # username = "telegraf"
  # password = "metricsmetricsmetricsmetrics"
  ## Set the user agent for HTTP POSTs (can be useful for log differentiation)
  # user_agent = "telegraf"
  ## Set UDP payload size, defaults to InfluxDB UDP Client default (512 bytes)
  # udp_payload = 512
```
These defaults should work, so now to check the inputs, search the configuration file until you find `inputs.cpu`, the following should be setup.
```
# Read metrics about cpu usage
[[inputs.cpu]]
  ## Whether to report per-cpu stats or not
  percpu = true
  ## Whether to report total system cpu stats or not
  totalcpu = true
  ## If true, collect raw CPU time metrics.
  collect_cpu_time = false


# Read metrics about disk usage by mount point
[[inputs.disk]]
  ## By default, telegraf gather stats for all mountpoints.
  ## Setting mountpoints will restrict the stats to the specified mountpoints.
  # mount_points = ["/"]

  ## Ignore some mountpoints by filesystem type. For example (dev)tmpfs (usually
  ## present on /run, /var/run, /dev/shm or /dev).
  ignore_fs = ["tmpfs", "devtmpfs"]


# Read metrics about disk IO by device
[[inputs.diskio]]
  ## By default, telegraf will gather stats for all devices including
  ## disk partitions.
  ## Setting devices will restrict the stats to the specified devices.
  # devices = ["sda", "sdb"]
  ## Uncomment the following line if you need disk serial numbers.
  # skip_serial_number = false


# Get kernel statistics from /proc/stat
[[inputs.kernel]]
  # no configuration


# Read metrics about memory usage
[[inputs.mem]]
  # no configuration


# Get the number of processes and group them by status
[[inputs.processes]]
  # no configuration


# Read metrics about swap memory usage
[[inputs.swap]]
  # no configuration


# Read metrics about system load & uptime
[[inputs.system]]
  # no configuration
```
If this looks like your configuration then we can run a quick test to ensure the data is coming in. `curl "http://localhost:8086/query?q=select+*+from+telegraf..cpu"` if telegraf is setup properly you should see a lot of json data, if the output is empty than something has gone wrong.

## Chronograf
Now that we are collecting and storing data we can install Chronograf to view that data. Download, install and run `https://dl.influxdata.com/chronograf/nightlies/chronograf_nightly_amd64.deb` and `sudo systemctl start chronograf`.

If everything worked we should be able to connect to Chronograf and configure it. Point your web browser to `http://localhost:8888` or the ip of the server you are working on if you are not running on localhost. You should see a Welcome to Chronograf page that asks for the InfluxDB information. Enter the hostname or IP of the machine you configured InfluxDB on. You can name this connection anything you want. Since we used the default InfluxDB configuration you should not need to add a username and password.

Once this is done click connect. You should see the host list, with the machines hostname in the list. This machine should be configured with the system level stats, so it should show `system` in the apps list. If you click the hostname it should take you to a series of system level graphs about the host.

The next step is to set up Kapacitor. If you hover over the last menu item in the left navigation menu you will see a "Source" option. Under that it should like "Kapacitor". Click this option and enter in the ip or hostname of the Kapacitor instace (the default port for kapacitor is 9092). You can name it whatever you wish. If kapacitor is successfully connected to you will see a list of [alert endpoints](https://docs.influxdata.com/kapacitor/v1.0/nodes/alert_node/) you can use to send alerts to when a rule is triggered. You can configure those now or come back later.

Now that you are up and running try out the `Data Explore` to view your schema and build graphs, or go to the `Alert Rules` to create and load new alerting rules.
