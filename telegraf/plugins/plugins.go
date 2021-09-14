package plugins

import (
	"encoding/json"
	"fmt"
	"sort"
)

// Plugin defines a Telegraf plugin.
type Plugin struct {
	Type        string `json:"type,omitempty"`        // Type of the plugin.
	Name        string `json:"name,omitempty"`        // Name of the plugin.
	Description string `json:"description,omitempty"` // Description of the plugin.
	Config      string `json:"config,omitempty"`      // Config contains the toml config of the plugin.
}

// TelegrafPlugins defines a Telegraf version's collection of plugins.
type TelegrafPlugins struct {
	Version string   `json:"version,omitempty"` // Version of telegraf plugins are for.
	OS      string   `json:"os,omitempty"`      // OS the plugins apply to.
	Plugins []Plugin `json:"plugins,omitempty"` // Plugins this version of telegraf supports.
}

// ListAvailablePlugins lists available plugins based on type.
func ListAvailablePlugins(t string) (*TelegrafPlugins, error) {
	switch t {
	case "input":
		return AvailableInputs()
	case "output":
		return AvailableOutputs()
	case "processor":
		return AvailableProcessors()
	case "aggregator":
		return AvailableAggregators()
	case "bundle":
		return AvailableBundles()
	default:
		return nil, fmt.Errorf("unknown plugin type '%s'", t)
	}
}

// GetPlugin returns the plugin's sample config, if available.
func GetPlugin(t, name string) (*Plugin, bool) {
	var p *TelegrafPlugins
	var err error

	switch t {
	case "input":
		p, err = AvailableInputs()

	case "output":
		p, err = AvailableOutputs()

	case "processor":
		p, err = AvailableProcessors()

	case "aggregator":
		p, err = AvailableAggregators()

	case "bundle":
		p, err = AvailableBundles()

	default:
		return nil, false
	}

	if err != nil {
		return nil, false
	}

	return p.findPluginByName(name)
}

// findPluginByName returns a plugin named "name". This should only be run on
// TelegrafPlugins containing the same type of plugin.
func (t *TelegrafPlugins) findPluginByName(name string) (*Plugin, bool) {
	for i := range t.Plugins {
		if t.Plugins[i].Name == name {
			return &t.Plugins[i], true
		}
	}

	return nil, false
}

// AvailablePlugins returns the base list of available plugins.
func AvailablePlugins() (*TelegrafPlugins, error) {
	all := &TelegrafPlugins{}

	t, err := AvailableInputs()
	if err != nil {
		return nil, err
	}
	all.Version = t.Version
	all.Plugins = append(all.Plugins, t.Plugins...)

	t, err = AvailableOutputs()
	if err != nil {
		return nil, err
	}
	all.Plugins = append(all.Plugins, t.Plugins...)

	t, err = AvailableProcessors()
	if err != nil {
		return nil, err
	}
	all.Plugins = append(all.Plugins, t.Plugins...)

	t, err = AvailableAggregators()
	if err != nil {
		return nil, err
	}
	all.Plugins = append(all.Plugins, t.Plugins...)

	return all, nil
}

func sortPlugins(t *TelegrafPlugins) *TelegrafPlugins {
	sort.Slice(t.Plugins, func(i, j int) bool {
		return t.Plugins[i].Name < t.Plugins[j].Name
	})

	return t
}

// AvailableInputs returns the base list of available input plugins.
func AvailableInputs() (*TelegrafPlugins, error) {
	t := &TelegrafPlugins{}
	err := json.Unmarshal([]byte(availableInputs), t)
	if err != nil {
		return nil, err
	}
	return sortPlugins(t), nil
}

// AvailableOutputs returns the base list of available output plugins.
func AvailableOutputs() (*TelegrafPlugins, error) {
	t := &TelegrafPlugins{}
	err := json.Unmarshal([]byte(availableOutputs), t)
	if err != nil {
		return nil, err
	}
	return sortPlugins(t), nil
}

// AvailableProcessors returns the base list of available processor plugins.
func AvailableProcessors() (*TelegrafPlugins, error) {
	t := &TelegrafPlugins{}
	err := json.Unmarshal([]byte(availableProcessors), t)
	if err != nil {
		return nil, err
	}
	return sortPlugins(t), nil
}

// AvailableAggregators returns the base list of available aggregator plugins.
func AvailableAggregators() (*TelegrafPlugins, error) {
	t := &TelegrafPlugins{}
	err := json.Unmarshal([]byte(availableAggregators), t)
	if err != nil {
		return nil, err
	}
	return sortPlugins(t), nil
}

// AvailableBundles returns the base list of available bundled plugins.
func AvailableBundles() (*TelegrafPlugins, error) {
	return &TelegrafPlugins{
		Version: "1.13.0",
		OS:      "unix",
		Plugins: []Plugin{
			{
				Type:        "bundle",
				Name:        "System Bundle",
				Description: "Collection of system related inputs",
				Config: "" +
					"# Read metrics about cpu usage\n[[inputs.cpu]]\n  # alias=\"cpu\"\n  ## Whether to report per-cpu stats or not\n  percpu = true\n  ## Whether to report total system cpu stats or not\n  totalcpu = true\n  ## If true, collect raw CPU time metrics.\n  collect_cpu_time = false\n  ## If true, compute and report the sum of all non-idle CPU states.\n  report_active = false\n" +
					"# Read metrics about swap memory usage\n[[inputs.swap]]\n  # alias=\"swap\"\n" +
					"# Read metrics about disk usage by mount point\n[[inputs.disk]]\n  # alias=\"disk\"\n  ## By default stats will be gathered for all mount points.\n  ## Set mount_points will restrict the stats to only the specified mount points.\n  # mount_points = [\"/\"]\n\n  ## Ignore mount points by filesystem type.\n  ignore_fs = [\"tmpfs\", \"devtmpfs\", \"devfs\", \"iso9660\", \"overlay\", \"aufs\", \"squashfs\"]\n" +
					"# Read metrics about memory usage\n[[inputs.mem]]\n  # alias=\"mem\"\n",
			},
		},
	}, nil
}

// AgentConfig contains the default agent config.
var AgentConfig = `# Configuration for telegraf agent
[agent]
  ## Default data collection interval for all inputs
  interval = "10s"
  ## Rounds collection interval to 'interval'
  ## ie, if interval="10s" then always collect on :00, :10, :20, etc.
  round_interval = true

  ## Telegraf will send metrics to outputs in batches of at most
  ## metric_batch_size metrics.
  ## This controls the size of writes that Telegraf sends to output plugins.
  metric_batch_size = 1000

  ## Maximum number of unwritten metrics per output.  Increasing this value
  ## allows for longer periods of output downtime without dropping metrics at the
  ## cost of higher maximum memory usage.
  metric_buffer_limit = 10000

  ## Collection jitter is used to jitter the collection by a random amount.
  ## Each plugin will sleep for a random time within jitter before collecting.
  ## This can be used to avoid many plugins querying things like sysfs at the
  ## same time, which can have a measurable effect on the system.
  collection_jitter = "0s"

  ## Default flushing interval for all outputs. Maximum flush_interval will be
  ## flush_interval + flush_jitter
  flush_interval = "10s"
  ## Jitter the flush interval by a random amount. This is primarily to avoid
  ## large write spikes for users running a large number of telegraf instances.
  ## ie, a jitter of 5s and interval 10s means flushes will happen every 10-15s
  flush_jitter = "0s"

  ## By default or when set to "0s", precision will be set to the same
  ## timestamp order as the collection interval, with the maximum being 1s.
  ##   ie, when interval = "10s", precision will be "1s"
  ##       when interval = "250ms", precision will be "1ms"
  ## Precision will NOT be used for service inputs. It is up to each individual
  ## service input to set the timestamp at the appropriate precision.
  ## Valid time units are "ns", "us" (or "µs"), "ms", "s".
  precision = ""

  ## Log at debug level.
  # debug = false
  ## Log only error level messages.
  # quiet = false

  ## Log target controls the destination for logs and can be one of "file",
  ## "stderr" or, on Windows, "eventlog".  When set to "file", the output file
  ## is determined by the "logfile" setting.
  # logtarget = "file"

  ## Name of the file to be logged to when using the "file" logtarget.  If set to
  ## the empty string then logs are written to stderr.
  # logfile = ""

  ## The logfile will be rotated after the time interval specified.  When set
  ## to 0 no time based rotation is performed.  Logs are rotated only when
  ## written to, if there is no log activity rotation may be delayed.
  # logfile_rotation_interval = "0d"

  ## The logfile will be rotated when it becomes larger than the specified
  ## size.  When set to 0 no size based rotation is performed.
  # logfile_rotation_max_size = "0MB"

  ## Maximum number of rotated archives to keep, any older logs are deleted.
  ## If set to -1, no archives are removed.
  # logfile_rotation_max_archives = 5

  ## Pick a timezone to use when logging or type 'local' for local time.
  ## Example: America/Chicago
  # log_with_timezone = ""

  ## Override default hostname, if empty use os.Hostname()
  hostname = ""
  ## If set to true, do no set the "host" tag in the telegraf agent.
  omit_hostname = false
`

var availableInputs = `{
  "version": "1.13.0",
  "os": "linux",
  "plugins": [
    {
      "type": "input",
      "name": "tcp_listener",
      "description": "Generic TCP listener",
      "config": "# Generic TCP listener\n[[inputs.tcp_listener]]\n  # alias=\"tcp_listener\"\n  # DEPRECATED: the TCP listener plugin has been deprecated in favor of the\n  # socket_listener plugin\n  # see https://github.com/influxdata/telegraf/tree/master/plugins/inputs/socket_listener\n\n"
    },
    {
      "type": "input",
      "name": "kernel",
      "description": "Get kernel statistics from /proc/stat",
      "config": "# Get kernel statistics from /proc/stat\n[[inputs.kernel]]\n  # alias=\"kernel\"\n"
    },
    {
      "type": "input",
      "name": "powerdns",
      "description": "Read metrics from one or many PowerDNS servers",
      "config": "# Read metrics from one or many PowerDNS servers\n[[inputs.powerdns]]\n  # alias=\"powerdns\"\n  ## An array of sockets to gather stats about.\n  ## Specify a path to unix socket.\n  unix_sockets = [\"/var/run/pdns.controlsocket\"]\n\n"
    },
    {
      "type": "input",
      "name": "processes",
      "description": "Get the number of processes and group them by status",
      "config": "# Get the number of processes and group them by status\n[[inputs.processes]]\n  # alias=\"processes\"\n"
    },
    {
      "type": "input",
      "name": "snmp_legacy",
      "description": "DEPRECATED! PLEASE USE inputs.snmp INSTEAD.",
      "config": "# DEPRECATED! PLEASE USE inputs.snmp INSTEAD.\n[[inputs.snmp_legacy]]\n  # alias=\"snmp_legacy\"\n  ## Use 'oids.txt' file to translate oids to names\n  ## To generate 'oids.txt' you need to run:\n  ##   snmptranslate -m all -Tz -On | sed -e 's/\"//g' \u003e /tmp/oids.txt\n  ## Or if you have an other MIB folder with custom MIBs\n  ##   snmptranslate -M /mycustommibfolder -Tz -On -m all | sed -e 's/\"//g' \u003e oids.txt\n  snmptranslate_file = \"/tmp/oids.txt\"\n  [[inputs.snmp.host]]\n    address = \"192.168.2.2:161\"\n    # SNMP community\n    community = \"public\" # default public\n    # SNMP version (1, 2 or 3)\n    # Version 3 not supported yet\n    version = 2 # default 2\n    # SNMP response timeout\n    timeout = 2.0 # default 2.0\n    # SNMP request retries\n    retries = 2 # default 2\n    # Which get/bulk do you want to collect for this host\n    collect = [\"mybulk\", \"sysservices\", \"sysdescr\"]\n    # Simple list of OIDs to get, in addition to \"collect\"\n    get_oids = []\n\n  [[inputs.snmp.host]]\n    address = \"192.168.2.3:161\"\n    community = \"public\"\n    version = 2\n    timeout = 2.0\n    retries = 2\n    collect = [\"mybulk\"]\n    get_oids = [\n        \"ifNumber\",\n        \".1.3.6.1.2.1.1.3.0\",\n    ]\n\n  [[inputs.snmp.get]]\n    name = \"ifnumber\"\n    oid = \"ifNumber\"\n\n  [[inputs.snmp.get]]\n    name = \"interface_speed\"\n    oid = \"ifSpeed\"\n    instance = \"0\"\n\n  [[inputs.snmp.get]]\n    name = \"sysuptime\"\n    oid = \".1.3.6.1.2.1.1.3.0\"\n    unit = \"second\"\n\n  [[inputs.snmp.bulk]]\n    name = \"mybulk\"\n    max_repetition = 127\n    oid = \".1.3.6.1.2.1.1\"\n\n  [[inputs.snmp.bulk]]\n    name = \"ifoutoctets\"\n    max_repetition = 127\n    oid = \"ifOutOctets\"\n\n  [[inputs.snmp.host]]\n    address = \"192.168.2.13:161\"\n    #address = \"127.0.0.1:161\"\n    community = \"public\"\n    version = 2\n    timeout = 2.0\n    retries = 2\n    #collect = [\"mybulk\", \"sysservices\", \"sysdescr\", \"systype\"]\n    collect = [\"sysuptime\" ]\n    [[inputs.snmp.host.table]]\n      name = \"iftable3\"\n      include_instances = [\"enp5s0\", \"eth1\"]\n\n  # SNMP TABLEs\n  # table without mapping neither subtables\n  [[inputs.snmp.table]]\n    name = \"iftable1\"\n    oid = \".1.3.6.1.2.1.31.1.1.1\"\n\n  # table without mapping but with subtables\n  [[inputs.snmp.table]]\n    name = \"iftable2\"\n    oid = \".1.3.6.1.2.1.31.1.1.1\"\n    sub_tables = [\".1.3.6.1.2.1.2.2.1.13\"]\n\n  # table with mapping but without subtables\n  [[inputs.snmp.table]]\n    name = \"iftable3\"\n    oid = \".1.3.6.1.2.1.31.1.1.1\"\n    # if empty. get all instances\n    mapping_table = \".1.3.6.1.2.1.31.1.1.1.1\"\n    # if empty, get all subtables\n\n  # table with both mapping and subtables\n  [[inputs.snmp.table]]\n    name = \"iftable4\"\n    oid = \".1.3.6.1.2.1.31.1.1.1\"\n    # if empty get all instances\n    mapping_table = \".1.3.6.1.2.1.31.1.1.1.1\"\n    # if empty get all subtables\n    # sub_tables could be not \"real subtables\"\n    sub_tables=[\".1.3.6.1.2.1.2.2.1.13\", \"bytes_recv\", \"bytes_send\"]\n\n"
    },
    {
      "type": "input",
      "name": "statsd",
      "description": "Statsd UDP/TCP Server",
      "config": "# Statsd UDP/TCP Server\n[[inputs.statsd]]\n  # alias=\"statsd\"\n  ## Protocol, must be \"tcp\", \"udp\", \"udp4\" or \"udp6\" (default=udp)\n  protocol = \"udp\"\n\n  ## MaxTCPConnection - applicable when protocol is set to tcp (default=250)\n  max_tcp_connections = 250\n\n  ## Enable TCP keep alive probes (default=false)\n  tcp_keep_alive = false\n\n  ## Specifies the keep-alive period for an active network connection.\n  ## Only applies to TCP sockets and will be ignored if tcp_keep_alive is false.\n  ## Defaults to the OS configuration.\n  # tcp_keep_alive_period = \"2h\"\n\n  ## Address and port to host UDP listener on\n  service_address = \":8125\"\n\n  ## The following configuration options control when telegraf clears it's cache\n  ## of previous values. If set to false, then telegraf will only clear it's\n  ## cache when the daemon is restarted.\n  ## Reset gauges every interval (default=true)\n  delete_gauges = true\n  ## Reset counters every interval (default=true)\n  delete_counters = true\n  ## Reset sets every interval (default=true)\n  delete_sets = true\n  ## Reset timings \u0026 histograms every interval (default=true)\n  delete_timings = true\n\n  ## Percentiles to calculate for timing \u0026 histogram stats\n  percentiles = [50.0, 90.0, 99.0, 99.9, 99.95, 100.0]\n\n  ## separator to use between elements of a statsd metric\n  metric_separator = \"_\"\n\n  ## Parses tags in the datadog statsd format\n  ## http://docs.datadoghq.com/guides/dogstatsd/\n  parse_data_dog_tags = false\n\n  ## Parses datadog extensions to the statsd format\n  datadog_extensions = false\n\n  ## Statsd data translation templates, more info can be read here:\n  ## https://github.com/influxdata/telegraf/blob/master/docs/TEMPLATE_PATTERN.md\n  # templates = [\n  #     \"cpu.* measurement*\"\n  # ]\n\n  ## Number of UDP messages allowed to queue up, once filled,\n  ## the statsd server will start dropping packets\n  allowed_pending_messages = 10000\n\n  ## Number of timing/histogram values to track per-measurement in the\n  ## calculation of percentiles. Raising this limit increases the accuracy\n  ## of percentiles but also increases the memory usage and cpu time.\n  percentile_limit = 1000\n\n"
    },
    {
      "type": "input",
      "name": "bcache",
      "description": "Read metrics of bcache from stats_total and dirty_data",
      "config": "# Read metrics of bcache from stats_total and dirty_data\n[[inputs.bcache]]\n  # alias=\"bcache\"\n  ## Bcache sets path\n  ## If not specified, then default is:\n  bcachePath = \"/sys/fs/bcache\"\n\n  ## By default, telegraf gather stats for all bcache devices\n  ## Setting devices will restrict the stats to the specified\n  ## bcache devices.\n  bcacheDevs = [\"bcache0\"]\n\n"
    },
    {
      "type": "input",
      "name": "mesos",
      "description": "Telegraf plugin for gathering metrics from N Mesos masters",
      "config": "# Telegraf plugin for gathering metrics from N Mesos masters\n[[inputs.mesos]]\n  # alias=\"mesos\"\n  ## Timeout, in ms.\n  timeout = 100\n\n  ## A list of Mesos masters.\n  masters = [\"http://localhost:5050\"]\n\n  ## Master metrics groups to be collected, by default, all enabled.\n  master_collections = [\n    \"resources\",\n    \"master\",\n    \"system\",\n    \"agents\",\n    \"frameworks\",\n    \"framework_offers\",\n    \"tasks\",\n    \"messages\",\n    \"evqueue\",\n    \"registrar\",\n    \"allocator\",\n  ]\n\n  ## A list of Mesos slaves, default is []\n  # slaves = []\n\n  ## Slave metrics groups to be collected, by default, all enabled.\n  # slave_collections = [\n  #   \"resources\",\n  #   \"agent\",\n  #   \"system\",\n  #   \"executors\",\n  #   \"tasks\",\n  #   \"messages\",\n  # ]\n\n  ## Optional TLS Config\n  # tls_ca = \"/etc/telegraf/ca.pem\"\n  # tls_cert = \"/etc/telegraf/cert.pem\"\n  # tls_key = \"/etc/telegraf/key.pem\"\n  ## Use TLS but skip chain \u0026 host verification\n  # insecure_skip_verify = false\n\n"
    },
    {
      "type": "input",
      "name": "pf",
      "description": "Gather counters from PF",
      "config": "# Gather counters from PF\n[[inputs.pf]]\n  # alias=\"pf\"\n  ## PF require root access on most systems.\n  ## Setting 'use_sudo' to true will make use of sudo to run pfctl.\n  ## Users must configure sudo to allow telegraf user to run pfctl with no password.\n  ## pfctl can be restricted to only list command \"pfctl -s info\".\n  use_sudo = false\n\n"
    },
    {
      "type": "input",
      "name": "webhooks",
      "description": "A Webhooks Event collector",
      "config": "# A Webhooks Event collector\n[[inputs.webhooks]]\n  # alias=\"webhooks\"\n  ## Address and port to host Webhook listener on\n  service_address = \":1619\"\n\n  [inputs.webhooks.filestack]\n    path = \"/filestack\"\n\n  [inputs.webhooks.github]\n    path = \"/github\"\n    # secret = \"\"\n\n  [inputs.webhooks.mandrill]\n    path = \"/mandrill\"\n\n  [inputs.webhooks.rollbar]\n    path = \"/rollbar\"\n\n  [inputs.webhooks.papertrail]\n    path = \"/papertrail\"\n\n  [inputs.webhooks.particle]\n    path = \"/particle\"\n\n"
    },
    {
      "type": "input",
      "name": "http_listener_v2",
      "description": "Generic HTTP write listener",
      "config": "# Generic HTTP write listener\n[[inputs.http_listener_v2]]\n  # alias=\"http_listener_v2\"\n  ## Address and port to host HTTP listener on\n  service_address = \":8080\"\n\n  ## Path to listen to.\n  # path = \"/telegraf\"\n\n  ## HTTP methods to accept.\n  # methods = [\"POST\", \"PUT\"]\n\n  ## maximum duration before timing out read of the request\n  # read_timeout = \"10s\"\n  ## maximum duration before timing out write of the response\n  # write_timeout = \"10s\"\n\n  ## Maximum allowed http request body size in bytes.\n  ## 0 means to use the default of 524,288,00 bytes (500 mebibytes)\n  # max_body_size = \"500MB\"\n\n  ## Part of the request to consume.  Available options are \"body\" and\n  ## \"query\".\n  # data_source = \"body\"\n\n  ## Set one or more allowed client CA certificate file names to\n  ## enable mutually authenticated TLS connections\n  # tls_allowed_cacerts = [\"/etc/telegraf/clientca.pem\"]\n\n  ## Add service certificate and key\n  # tls_cert = \"/etc/telegraf/cert.pem\"\n  # tls_key = \"/etc/telegraf/key.pem\"\n\n  ## Optional username and password to accept for HTTP basic authentication.\n  ## You probably want to make sure you have TLS configured above for this.\n  # basic_username = \"foobar\"\n  # basic_password = \"barfoo\"\n\n  ## Data format to consume.\n  ## Each data format has its own unique set of configuration options, read\n  ## more about them here:\n  ## https://github.com/influxdata/telegraf/blob/master/docs/DATA_FORMATS_INPUT.md\n  data_format = \"influx\"\n\n"
    },
    {
      "type": "input",
      "name": "http_listener",
      "description": "Influx HTTP write listener",
      "config": "# Influx HTTP write listener\n[[inputs.http_listener]]\n  # alias=\"http_listener\"\n  ## Address and port to host HTTP listener on\n  service_address = \":8186\"\n\n  ## maximum duration before timing out read of the request\n  read_timeout = \"10s\"\n  ## maximum duration before timing out write of the response\n  write_timeout = \"10s\"\n\n  ## Maximum allowed http request body size in bytes.\n  ## 0 means to use the default of 524,288,000 bytes (500 mebibytes)\n  max_body_size = \"500MiB\"\n\n  ## Maximum line size allowed to be sent in bytes.\n  ## 0 means to use the default of 65536 bytes (64 kibibytes)\n  max_line_size = \"64KiB\"\n  \n\n  ## Optional tag name used to store the database. \n  ## If the write has a database in the query string then it will be kept in this tag name.\n  ## This tag can be used in downstream outputs.\n  ## The default value of nothing means it will be off and the database will not be recorded.\n  # database_tag = \"\"\n\n  ## Set one or more allowed client CA certificate file names to\n  ## enable mutually authenticated TLS connections\n  tls_allowed_cacerts = [\"/etc/telegraf/clientca.pem\"]\n\n  ## Add service certificate and key\n  tls_cert = \"/etc/telegraf/cert.pem\"\n  tls_key = \"/etc/telegraf/key.pem\"\n\n  ## Optional username and password to accept for HTTP basic authentication.\n  ## You probably want to make sure you have TLS configured above for this.\n  # basic_username = \"foobar\"\n  # basic_password = \"barfoo\"\n\n"
    },
    {
      "type": "input",
      "name": "sysstat",
      "description": "Sysstat metrics collector",
      "config": "# Sysstat metrics collector\n[[inputs.sysstat]]\n  # alias=\"sysstat\"\n  ## Path to the sadc command.\n  #\n  ## Common Defaults:\n  ##   Debian/Ubuntu: /usr/lib/sysstat/sadc\n  ##   Arch:          /usr/lib/sa/sadc\n  ##   RHEL/CentOS:   /usr/lib64/sa/sadc\n  sadc_path = \"/usr/lib/sa/sadc\" # required\n\n  ## Path to the sadf command, if it is not in PATH\n  # sadf_path = \"/usr/bin/sadf\"\n\n  ## Activities is a list of activities, that are passed as argument to the\n  ## sadc collector utility (e.g: DISK, SNMP etc...)\n  ## The more activities that are added, the more data is collected.\n  # activities = [\"DISK\"]\n\n  ## Group metrics to measurements.\n  ##\n  ## If group is false each metric will be prefixed with a description\n  ## and represents itself a measurement.\n  ##\n  ## If Group is true, corresponding metrics are grouped to a single measurement.\n  # group = true\n\n  ## Options for the sadf command. The values on the left represent the sadf\n  ## options and the values on the right their description (which are used for\n  ## grouping and prefixing metrics).\n  ##\n  ## Run 'sar -h' or 'man sar' to find out the supported options for your\n  ## sysstat version.\n  [inputs.sysstat.options]\n    -C = \"cpu\"\n    -B = \"paging\"\n    -b = \"io\"\n    -d = \"disk\"             # requires DISK activity\n    \"-n ALL\" = \"network\"\n    \"-P ALL\" = \"per_cpu\"\n    -q = \"queue\"\n    -R = \"mem\"\n    -r = \"mem_util\"\n    -S = \"swap_util\"\n    -u = \"cpu_util\"\n    -v = \"inode\"\n    -W = \"swap\"\n    -w = \"task\"\n  #  -H = \"hugepages\"        # only available for newer linux distributions\n  #  \"-I ALL\" = \"interrupts\" # requires INT activity\n\n  ## Device tags can be used to add additional tags for devices.\n  ## For example the configuration below adds a tag vg with value rootvg for\n  ## all metrics with sda devices.\n  # [[inputs.sysstat.device_tags.sda]]\n  #  vg = \"rootvg\"\n\n"
    },
    {
      "type": "input",
      "name": "systemd_units",
      "description": "Gather systemd units state",
      "config": "# Gather systemd units state\n[[inputs.systemd_units]]\n  # alias=\"systemd_units\"\n  ## Set timeout for systemctl execution\n  # timeout = \"1s\"\n  #\n  ## Filter for a specific unit type, default is \"service\", other possible\n  ## values are \"socket\", \"target\", \"device\", \"mount\", \"automount\", \"swap\",\n  ## \"timer\", \"path\", \"slice\" and \"scope \":\n  # unittype = \"service\"\n\n"
    },
    {
      "type": "input",
      "name": "temp",
      "description": "Read metrics about temperature",
      "config": "# Read metrics about temperature\n[[inputs.temp]]\n  # alias=\"temp\"\n"
    },
    {
      "type": "input",
      "name": "cgroup",
      "description": "Read specific statistics per cgroup",
      "config": "# Read specific statistics per cgroup\n[[inputs.cgroup]]\n  # alias=\"cgroup\"\n  ## Directories in which to look for files, globs are supported.\n  ## Consider restricting paths to the set of cgroups you really\n  ## want to monitor if you have a large number of cgroups, to avoid\n  ## any cardinality issues.\n  # paths = [\n  #   \"/cgroup/memory\",\n  #   \"/cgroup/memory/child1\",\n  #   \"/cgroup/memory/child2/*\",\n  # ]\n  ## cgroup stat fields, as file names, globs are supported.\n  ## these file names are appended to each path from above.\n  # files = [\"memory.*usage*\", \"memory.limit_in_bytes\"]\n\n"
    },
    {
      "type": "input",
      "name": "mysql",
      "description": "Read metrics from one or many mysql servers",
      "config": "# Read metrics from one or many mysql servers\n[[inputs.mysql]]\n  # alias=\"mysql\"\n  ## specify servers via a url matching:\n  ##  [username[:password]@][protocol[(address)]]/[?tls=[true|false|skip-verify|custom]]\n  ##  see https://github.com/go-sql-driver/mysql#dsn-data-source-name\n  ##  e.g.\n  ##    servers = [\"user:passwd@tcp(127.0.0.1:3306)/?tls=false\"]\n  ##    servers = [\"user@tcp(127.0.0.1:3306)/?tls=false\"]\n  #\n  ## If no servers are specified, then localhost is used as the host.\n  servers = [\"tcp(127.0.0.1:3306)/\"]\n\n  ## Selects the metric output format.\n  ##\n  ## This option exists to maintain backwards compatibility, if you have\n  ## existing metrics do not set or change this value until you are ready to\n  ## migrate to the new format.\n  ##\n  ## If you do not have existing metrics from this plugin set to the latest\n  ## version.\n  ##\n  ## Telegraf \u003e=1.6: metric_version = 2\n  ##           \u003c1.6: metric_version = 1 (or unset)\n  metric_version = 2\n\n  ## if the list is empty, then metrics are gathered from all databasee tables\n  # table_schema_databases = []\n\n  ## gather metrics from INFORMATION_SCHEMA.TABLES for databases provided above list\n  # gather_table_schema = false\n\n  ## gather thread state counts from INFORMATION_SCHEMA.PROCESSLIST\n  # gather_process_list = false\n\n  ## gather user statistics from INFORMATION_SCHEMA.USER_STATISTICS\n  # gather_user_statistics = false\n\n  ## gather auto_increment columns and max values from information schema\n  # gather_info_schema_auto_inc = false\n\n  ## gather metrics from INFORMATION_SCHEMA.INNODB_METRICS\n  # gather_innodb_metrics = false\n\n  ## gather metrics from SHOW SLAVE STATUS command output\n  # gather_slave_status = false\n\n  ## gather metrics from SHOW BINARY LOGS command output\n  # gather_binary_logs = false\n\n  ## gather metrics from PERFORMANCE_SCHEMA.TABLE_IO_WAITS_SUMMARY_BY_TABLE\n  # gather_table_io_waits = false\n\n  ## gather metrics from PERFORMANCE_SCHEMA.TABLE_LOCK_WAITS\n  # gather_table_lock_waits = false\n\n  ## gather metrics from PERFORMANCE_SCHEMA.TABLE_IO_WAITS_SUMMARY_BY_INDEX_USAGE\n  # gather_index_io_waits = false\n\n  ## gather metrics from PERFORMANCE_SCHEMA.EVENT_WAITS\n  # gather_event_waits = false\n\n  ## gather metrics from PERFORMANCE_SCHEMA.FILE_SUMMARY_BY_EVENT_NAME\n  # gather_file_events_stats = false\n\n  ## gather metrics from PERFORMANCE_SCHEMA.EVENTS_STATEMENTS_SUMMARY_BY_DIGEST\n  # gather_perf_events_statements = false\n\n  ## the limits for metrics form perf_events_statements\n  # perf_events_statements_digest_text_limit = 120\n  # perf_events_statements_limit = 250\n  # perf_events_statements_time_limit = 86400\n\n  ## Some queries we may want to run less often (such as SHOW GLOBAL VARIABLES)\n  ##   example: interval_slow = \"30m\"\n  # interval_slow = \"\"\n\n  ## Optional TLS Config (will be used if tls=custom parameter specified in server uri)\n  # tls_ca = \"/etc/telegraf/ca.pem\"\n  # tls_cert = \"/etc/telegraf/cert.pem\"\n  # tls_key = \"/etc/telegraf/key.pem\"\n  ## Use TLS but skip chain \u0026 host verification\n  # insecure_skip_verify = false\n\n"
    },
    {
      "type": "input",
      "name": "redis",
      "description": "Read metrics from one or many redis servers",
      "config": "# Read metrics from one or many redis servers\n[[inputs.redis]]\n  # alias=\"redis\"\n  ## specify servers via a url matching:\n  ##  [protocol://][:password]@address[:port]\n  ##  e.g.\n  ##    tcp://localhost:6379\n  ##    tcp://:password@192.168.99.100\n  ##    unix:///var/run/redis.sock\n  ##\n  ## If no servers are specified, then localhost is used as the host.\n  ## If no port is specified, 6379 is used\n  servers = [\"tcp://localhost:6379\"]\n\n  ## specify server password\n  # password = \"s#cr@t%\"\n\n  ## Optional TLS Config\n  # tls_ca = \"/etc/telegraf/ca.pem\"\n  # tls_cert = \"/etc/telegraf/cert.pem\"\n  # tls_key = \"/etc/telegraf/key.pem\"\n  ## Use TLS but skip chain \u0026 host verification\n  # insecure_skip_verify = true\n\n"
    },
    {
      "type": "input",
      "name": "couchbase",
      "description": "Read metrics from one or many couchbase clusters",
      "config": "# Read metrics from one or many couchbase clusters\n[[inputs.couchbase]]\n  # alias=\"couchbase\"\n  ## specify servers via a url matching:\n  ##  [protocol://][:password]@address[:port]\n  ##  e.g.\n  ##    http://couchbase-0.example.com/\n  ##    http://admin:secret@couchbase-0.example.com:8091/\n  ##\n  ## If no servers are specified, then localhost is used as the host.\n  ## If no protocol is specified, HTTP is used.\n  ## If no port is specified, 8091 is used.\n  servers = [\"http://localhost:8091\"]\n\n"
    },
    {
      "type": "input",
      "name": "file",
      "description": "Reload and gather from file[s] on telegraf's interval.",
      "config": "# Reload and gather from file[s] on telegraf's interval.\n[[inputs.file]]\n  # alias=\"file\"\n  ## Files to parse each interval.\n  ## These accept standard unix glob matching rules, but with the addition of\n  ## ** as a \"super asterisk\". ie:\n  ##   /var/log/**.log     -\u003e recursively find all .log files in /var/log\n  ##   /var/log/*/*.log    -\u003e find all .log files with a parent dir in /var/log\n  ##   /var/log/apache.log -\u003e only read the apache log file\n  files = [\"/var/log/apache/access.log\"]\n\n  ## The dataformat to be read from files\n  ## Each data format has its own unique set of configuration options, read\n  ## more about them here:\n  ## https://github.com/influxdata/telegraf/blob/master/docs/DATA_FORMATS_INPUT.md\n  data_format = \"influx\"\n\n  ## Name a tag containing the name of the file the data was parsed from.  Leave empty\n  ## to disable.\n  # file_tag = \"\"\n\n"
    },
    {
      "type": "input",
      "name": "kube_inventory",
      "description": "Read metrics from the Kubernetes api",
      "config": "# Read metrics from the Kubernetes api\n[[inputs.kube_inventory]]\n  # alias=\"kube_inventory\"\n  ## URL for the Kubernetes API\n  url = \"https://127.0.0.1\"\n\n  ## Namespace to use. Set to \"\" to use all namespaces.\n  # namespace = \"default\"\n\n  ## Use bearer token for authorization. ('bearer_token' takes priority)\n  ## If both of these are empty, we'll use the default serviceaccount:\n  ## at: /run/secrets/kubernetes.io/serviceaccount/token\n  # bearer_token = \"/path/to/bearer/token\"\n  ## OR\n  # bearer_token_string = \"abc_123\"\n\n  ## Set response_timeout (default 5 seconds)\n  # response_timeout = \"5s\"\n\n  ## Optional Resources to exclude from gathering\n  ## Leave them with blank with try to gather everything available.\n  ## Values can be - \"daemonsets\", deployments\", \"endpoints\", \"ingress\", \"nodes\",\n  ## \"persistentvolumes\", \"persistentvolumeclaims\", \"pods\", \"services\", \"statefulsets\"\n  # resource_exclude = [ \"deployments\", \"nodes\", \"statefulsets\" ]\n\n  ## Optional Resources to include when gathering\n  ## Overrides resource_exclude if both set.\n  # resource_include = [ \"deployments\", \"nodes\", \"statefulsets\" ]\n\n  ## Optional TLS Config\n  # tls_ca = \"/path/to/cafile\"\n  # tls_cert = \"/path/to/certfile\"\n  # tls_key = \"/path/to/keyfile\"\n  ## Use TLS but skip chain \u0026 host verification\n  # insecure_skip_verify = false\n\n"
    },
    {
      "type": "input",
      "name": "neptune_apex",
      "description": "Neptune Apex data collector",
      "config": "# Neptune Apex data collector\n[[inputs.neptune_apex]]\n  # alias=\"neptune_apex\"\n  ## The Neptune Apex plugin reads the publicly available status.xml data from a local Apex.\n  ## Measurements will be logged under \"apex\".\n\n  ## The base URL of the local Apex(es). If you specify more than one server, they will\n  ## be differentiated by the \"source\" tag.\n  servers = [\n    \"http://apex.local\",\n  ]\n\n  ## The response_timeout specifies how long to wait for a reply from the Apex.\n  #response_timeout = \"5s\"\n\n"
    },
    {
      "type": "input",
      "name": "openntpd",
      "description": "Get standard NTP query metrics from OpenNTPD.",
      "config": "# Get standard NTP query metrics from OpenNTPD.\n[[inputs.openntpd]]\n  # alias=\"openntpd\"\n  ## Run ntpctl binary with sudo.\n  # use_sudo = false\n\n  ## Location of the ntpctl binary.\n  # binary = \"/usr/sbin/ntpctl\"\n\n  ## Maximum time the ntpctl binary is allowed to run.\n  # timeout = \"5ms\"\n  \n"
    },
    {
      "type": "input",
      "name": "ipset",
      "description": "Gather packets and bytes counters from Linux ipsets",
      "config": "# Gather packets and bytes counters from Linux ipsets\n[[inputs.ipset]]\n  # alias=\"ipset\"\n  ## By default, we only show sets which have already matched at least 1 packet.\n  ## set include_unmatched_sets = true to gather them all.\n  include_unmatched_sets = false\n  ## Adjust your sudo settings appropriately if using this option (\"sudo ipset save\")\n  use_sudo = false\n  ## The default timeout of 1s for ipset execution can be overridden here:\n  # timeout = \"1s\"\n\n"
    },
    {
      "type": "input",
      "name": "tengine",
      "description": "Read Tengine's basic status information (ngx_http_reqstat_module)",
      "config": "# Read Tengine's basic status information (ngx_http_reqstat_module)\n[[inputs.tengine]]\n  # alias=\"tengine\"\n  # An array of Tengine reqstat module URI to gather stats.\n  urls = [\"http://127.0.0.1/us\"]\n\n  # HTTP response timeout (default: 5s)\n  # response_timeout = \"5s\"\n\n  ## Optional TLS Config\n  # tls_ca = \"/etc/telegraf/ca.pem\"\n  # tls_cert = \"/etc/telegraf/cert.cer\"\n  # tls_key = \"/etc/telegraf/key.key\"\n  ## Use TLS but skip chain \u0026 host verification\n  # insecure_skip_verify = false\n\n"
    },
    {
      "type": "input",
      "name": "vsphere",
      "description": "Read metrics from VMware vCenter",
      "config": "# Read metrics from VMware vCenter\n[[inputs.vsphere]]\n  # alias=\"vsphere\"\n  ## List of vCenter URLs to be monitored. These three lines must be uncommented\n  ## and edited for the plugin to work.\n  vcenters = [ \"https://vcenter.local/sdk\" ]\n  username = \"user@corp.local\"\n  password = \"secret\"\n\n  ## VMs\n  ## Typical VM metrics (if omitted or empty, all metrics are collected)\n  vm_metric_include = [\n    \"cpu.demand.average\",\n    \"cpu.idle.summation\",\n    \"cpu.latency.average\",\n    \"cpu.readiness.average\",\n    \"cpu.ready.summation\",\n    \"cpu.run.summation\",\n    \"cpu.usagemhz.average\",\n    \"cpu.used.summation\",\n    \"cpu.wait.summation\",\n    \"mem.active.average\",\n    \"mem.granted.average\",\n    \"mem.latency.average\",\n    \"mem.swapin.average\",\n    \"mem.swapinRate.average\",\n    \"mem.swapout.average\",\n    \"mem.swapoutRate.average\",\n    \"mem.usage.average\",\n    \"mem.vmmemctl.average\",\n    \"net.bytesRx.average\",\n    \"net.bytesTx.average\",\n    \"net.droppedRx.summation\",\n    \"net.droppedTx.summation\",\n    \"net.usage.average\",\n    \"power.power.average\",\n    \"virtualDisk.numberReadAveraged.average\",\n    \"virtualDisk.numberWriteAveraged.average\",\n    \"virtualDisk.read.average\",\n    \"virtualDisk.readOIO.latest\",\n    \"virtualDisk.throughput.usage.average\",\n    \"virtualDisk.totalReadLatency.average\",\n    \"virtualDisk.totalWriteLatency.average\",\n    \"virtualDisk.write.average\",\n    \"virtualDisk.writeOIO.latest\",\n    \"sys.uptime.latest\",\n  ]\n  # vm_metric_exclude = [] ## Nothing is excluded by default\n  # vm_instances = true ## true by default\n\n  ## Hosts\n  ## Typical host metrics (if omitted or empty, all metrics are collected)\n  host_metric_include = [\n    \"cpu.coreUtilization.average\",\n    \"cpu.costop.summation\",\n    \"cpu.demand.average\",\n    \"cpu.idle.summation\",\n    \"cpu.latency.average\",\n    \"cpu.readiness.average\",\n    \"cpu.ready.summation\",\n    \"cpu.swapwait.summation\",\n    \"cpu.usage.average\",\n    \"cpu.usagemhz.average\",\n    \"cpu.used.summation\",\n    \"cpu.utilization.average\",\n    \"cpu.wait.summation\",\n    \"disk.deviceReadLatency.average\",\n    \"disk.deviceWriteLatency.average\",\n    \"disk.kernelReadLatency.average\",\n    \"disk.kernelWriteLatency.average\",\n    \"disk.numberReadAveraged.average\",\n    \"disk.numberWriteAveraged.average\",\n    \"disk.read.average\",\n    \"disk.totalReadLatency.average\",\n    \"disk.totalWriteLatency.average\",\n    \"disk.write.average\",\n    \"mem.active.average\",\n    \"mem.latency.average\",\n    \"mem.state.latest\",\n    \"mem.swapin.average\",\n    \"mem.swapinRate.average\",\n    \"mem.swapout.average\",\n    \"mem.swapoutRate.average\",\n    \"mem.totalCapacity.average\",\n    \"mem.usage.average\",\n    \"mem.vmmemctl.average\",\n    \"net.bytesRx.average\",\n    \"net.bytesTx.average\",\n    \"net.droppedRx.summation\",\n    \"net.droppedTx.summation\",\n    \"net.errorsRx.summation\",\n    \"net.errorsTx.summation\",\n    \"net.usage.average\",\n    \"power.power.average\",\n    \"storageAdapter.numberReadAveraged.average\",\n    \"storageAdapter.numberWriteAveraged.average\",\n    \"storageAdapter.read.average\",\n    \"storageAdapter.write.average\",\n    \"sys.uptime.latest\",\n  ]\n  ## Collect IP addresses? Valid values are \"ipv4\" and \"ipv6\"\n  # ip_addresses = [\"ipv6\", \"ipv4\" ]\n  # host_metric_exclude = [] ## Nothing excluded by default\n  # host_instances = true ## true by default\n\n  ## Clusters\n  # cluster_metric_include = [] ## if omitted or empty, all metrics are collected\n  # cluster_metric_exclude = [] ## Nothing excluded by default\n  # cluster_instances = false ## false by default\n\n  ## Datastores\n  # datastore_metric_include = [] ## if omitted or empty, all metrics are collected\n  # datastore_metric_exclude = [] ## Nothing excluded by default\n  # datastore_instances = false ## false by default for Datastores only\n\n  ## Datacenters\n  datacenter_metric_include = [] ## if omitted or empty, all metrics are collected\n  datacenter_metric_exclude = [ \"*\" ] ## Datacenters are not collected by default.\n  # datacenter_instances = false ## false by default for Datastores only\n\n  ## Plugin Settings  \n  ## separator character to use for measurement and field names (default: \"_\")\n  # separator = \"_\"\n\n  ## number of objects to retreive per query for realtime resources (vms and hosts)\n  ## set to 64 for vCenter 5.5 and 6.0 (default: 256)\n  # max_query_objects = 256\n\n  ## number of metrics to retreive per query for non-realtime resources (clusters and datastores)\n  ## set to 64 for vCenter 5.5 and 6.0 (default: 256)\n  # max_query_metrics = 256\n\n  ## number of go routines to use for collection and discovery of objects and metrics\n  # collect_concurrency = 1\n  # discover_concurrency = 1\n\n  ## whether or not to force discovery of new objects on initial gather call before collecting metrics\n  ## when true for large environments this may cause errors for time elapsed while collecting metrics\n  ## when false (default) the first collection cycle may result in no or limited metrics while objects are discovered\n  # force_discover_on_init = false\n\n  ## the interval before (re)discovering objects subject to metrics collection (default: 300s)\n  # object_discovery_interval = \"300s\"\n\n  ## timeout applies to any of the api request made to vcenter\n  # timeout = \"60s\"\n\n  ## When set to true, all samples are sent as integers. This makes the output\n  ## data types backwards compatible with Telegraf 1.9 or lower. Normally all\n  ## samples from vCenter, with the exception of percentages, are integer\n  ## values, but under some conditions, some averaging takes place internally in\n  ## the plugin. Setting this flag to \"false\" will send values as floats to\n  ## preserve the full precision when averaging takes place.\n  # use_int_samples = true\n\n  ## Custom attributes from vCenter can be very useful for queries in order to slice the\n  ## metrics along different dimension and for forming ad-hoc relationships. They are disabled\n  ## by default, since they can add a considerable amount of tags to the resulting metrics. To\n  ## enable, simply set custom_attribute_exlude to [] (empty set) and use custom_attribute_include\n  ## to select the attributes you want to include.\n  # custom_attribute_include = []\n  # custom_attribute_exclude = [\"*\"] \n\n  ## Optional SSL Config\n  # ssl_ca = \"/path/to/cafile\"\n  # ssl_cert = \"/path/to/certfile\"\n  # ssl_key = \"/path/to/keyfile\"\n  ## Use SSL but skip chain \u0026 host verification\n  # insecure_skip_verify = false\n\n"
    },
    {
      "type": "input",
      "name": "aurora",
      "description": "Gather metrics from Apache Aurora schedulers",
      "config": "# Gather metrics from Apache Aurora schedulers\n[[inputs.aurora]]\n  # alias=\"aurora\"\n  ## Schedulers are the base addresses of your Aurora Schedulers\n  schedulers = [\"http://127.0.0.1:8081\"]\n\n  ## Set of role types to collect metrics from.\n  ##\n  ## The scheduler roles are checked each interval by contacting the\n  ## scheduler nodes; zookeeper is not contacted.\n  # roles = [\"leader\", \"follower\"]\n\n  ## Timeout is the max time for total network operations.\n  # timeout = \"5s\"\n\n  ## Username and password are sent using HTTP Basic Auth.\n  # username = \"username\"\n  # password = \"pa$$word\"\n\n  ## Optional TLS Config\n  # tls_ca = \"/etc/telegraf/ca.pem\"\n  # tls_cert = \"/etc/telegraf/cert.pem\"\n  # tls_key = \"/etc/telegraf/key.pem\"\n  ## Use TLS but skip chain \u0026 host verification\n  # insecure_skip_verify = false\n\n"
    },
    {
      "type": "input",
      "name": "burrow",
      "description": "Collect Kafka topics and consumers status from Burrow HTTP API.",
      "config": "# Collect Kafka topics and consumers status from Burrow HTTP API.\n[[inputs.burrow]]\n  # alias=\"burrow\"\n  ## Burrow API endpoints in format \"schema://host:port\".\n  ## Default is \"http://localhost:8000\".\n  servers = [\"http://localhost:8000\"]\n\n  ## Override Burrow API prefix.\n  ## Useful when Burrow is behind reverse-proxy.\n  # api_prefix = \"/v3/kafka\"\n\n  ## Maximum time to receive response.\n  # response_timeout = \"5s\"\n\n  ## Limit per-server concurrent connections.\n  ## Useful in case of large number of topics or consumer groups.\n  # concurrent_connections = 20\n\n  ## Filter clusters, default is no filtering.\n  ## Values can be specified as glob patterns.\n  # clusters_include = []\n  # clusters_exclude = []\n\n  ## Filter consumer groups, default is no filtering.\n  ## Values can be specified as glob patterns.\n  # groups_include = []\n  # groups_exclude = []\n\n  ## Filter topics, default is no filtering.\n  ## Values can be specified as glob patterns.\n  # topics_include = []\n  # topics_exclude = []\n\n  ## Credentials for basic HTTP authentication.\n  # username = \"\"\n  # password = \"\"\n\n  ## Optional SSL config\n  # ssl_ca = \"/etc/telegraf/ca.pem\"\n  # ssl_cert = \"/etc/telegraf/cert.pem\"\n  # ssl_key = \"/etc/telegraf/key.pem\"\n  # insecure_skip_verify = false\n\n"
    },
    {
      "type": "input",
      "name": "consul",
      "description": "Gather health check statuses from services registered in Consul",
      "config": "# Gather health check statuses from services registered in Consul\n[[inputs.consul]]\n  # alias=\"consul\"\n  ## Consul server address\n  # address = \"localhost\"\n\n  ## URI scheme for the Consul server, one of \"http\", \"https\"\n  # scheme = \"http\"\n\n  ## ACL token used in every request\n  # token = \"\"\n\n  ## HTTP Basic Authentication username and password.\n  # username = \"\"\n  # password = \"\"\n\n  ## Data center to query the health checks from\n  # datacenter = \"\"\n\n  ## Optional TLS Config\n  # tls_ca = \"/etc/telegraf/ca.pem\"\n  # tls_cert = \"/etc/telegraf/cert.pem\"\n  # tls_key = \"/etc/telegraf/key.pem\"\n  ## Use TLS but skip chain \u0026 host verification\n  # insecure_skip_verify = true\n\n  ## Consul checks' tag splitting\n  # When tags are formatted like \"key:value\" with \":\" as a delimiter then\n  # they will be splitted and reported as proper key:value in Telegraf\n  # tag_delimiter = \":\"\n\n"
    },
    {
      "type": "input",
      "name": "dovecot",
      "description": "Read statistics from one or many dovecot servers",
      "config": "# Read statistics from one or many dovecot servers\n[[inputs.dovecot]]\n  # alias=\"dovecot\"\n  ## specify dovecot servers via an address:port list\n  ##  e.g.\n  ##    localhost:24242\n  ##\n  ## If no servers are specified, then localhost is used as the host.\n  servers = [\"localhost:24242\"]\n\n  ## Type is one of \"user\", \"domain\", \"ip\", or \"global\"\n  type = \"global\"\n\n  ## Wildcard matches like \"*.com\". An empty string \"\" is same as \"*\"\n  ## If type = \"ip\" filters should be \u003cIP/network\u003e\n  filters = [\"\"]\n\n"
    },
    {
      "type": "input",
      "name": "fireboard",
      "description": "Read real time temps from fireboard.io servers",
      "config": "# Read real time temps from fireboard.io servers\n[[inputs.fireboard]]\n  # alias=\"fireboard\"\n  ## Specify auth token for your account\n  auth_token = \"invalidAuthToken\"\n  ## You can override the fireboard server URL if necessary\n  # url = https://fireboard.io/api/v1/devices.json\n  ## You can set a different http_timeout if you need to\n  ## You should set a string using an number and time indicator\n  ## for example \"12s\" for 12 seconds.\n  # http_timeout = \"4s\"\n\n"
    },
    {
      "type": "input",
      "name": "ecs",
      "description": "Read metrics about docker containers from Fargate/ECS v2 meta endpoints.",
      "config": "# Read metrics about docker containers from Fargate/ECS v2 meta endpoints.\n[[inputs.ecs]]\n  # alias=\"ecs\"\n  ## ECS metadata url\n  # endpoint_url = \"http://169.254.170.2\"\n\n  ## Containers to include and exclude. Globs accepted.\n  ## Note that an empty array for both will include all containers\n  # container_name_include = []\n  # container_name_exclude = []\n\n  ## Container states to include and exclude. Globs accepted.\n  ## When empty only containers in the \"RUNNING\" state will be captured.\n  ## Possible values are \"NONE\", \"PULLED\", \"CREATED\", \"RUNNING\",\n  ## \"RESOURCES_PROVISIONED\", \"STOPPED\".\n  # container_status_include = []\n  # container_status_exclude = []\n\n  ## ecs labels to include and exclude as tags.  Globs accepted.\n  ## Note that an empty array for both will include all labels as tags\n  ecs_label_include = [ \"com.amazonaws.ecs.*\" ]\n  ecs_label_exclude = []\n\n  ## Timeout for queries.\n  # timeout = \"5s\"\n\n"
    },
    {
      "type": "input",
      "name": "icinga2",
      "description": "Gather Icinga2 status",
      "config": "# Gather Icinga2 status\n[[inputs.icinga2]]\n  # alias=\"icinga2\"\n  ## Required Icinga2 server address\n  # server = \"https://localhost:5665\"\n  \n  ## Required Icinga2 object type (\"services\" or \"hosts\")\n  # object_type = \"services\"\n\n  ## Credentials for basic HTTP authentication\n  # username = \"admin\"\n  # password = \"admin\"\n\n  ## Maximum time to receive response.\n  # response_timeout = \"5s\"\n\n  ## Optional TLS Config\n  # tls_ca = \"/etc/telegraf/ca.pem\"\n  # tls_cert = \"/etc/telegraf/cert.pem\"\n  # tls_key = \"/etc/telegraf/key.pem\"\n  ## Use TLS but skip chain \u0026 host verification\n  # insecure_skip_verify = true\n  \n"
    },
    {
      "type": "input",
      "name": "diskio",
      "description": "Read metrics about disk IO by device",
      "config": "# Read metrics about disk IO by device\n[[inputs.diskio]]\n  # alias=\"diskio\"\n  ## By default, telegraf will gather stats for all devices including\n  ## disk partitions.\n  ## Setting devices will restrict the stats to the specified devices.\n  # devices = [\"sda\", \"sdb\", \"vd*\"]\n  ## Uncomment the following line if you need disk serial numbers.\n  # skip_serial_number = false\n  #\n  ## On systems which support it, device metadata can be added in the form of\n  ## tags.\n  ## Currently only Linux is supported via udev properties. You can view\n  ## available properties for a device by running:\n  ## 'udevadm info -q property -n /dev/sda'\n  ## Note: Most, but not all, udev properties can be accessed this way. Properties\n  ## that are currently inaccessible include DEVTYPE, DEVNAME, and DEVPATH.\n  # device_tags = [\"ID_FS_TYPE\", \"ID_FS_USAGE\"]\n  #\n  ## Using the same metadata source as device_tags, you can also customize the\n  ## name of the device via templates.\n  ## The 'name_templates' parameter is a list of templates to try and apply to\n  ## the device. The template may contain variables in the form of '$PROPERTY' or\n  ## '${PROPERTY}'. The first template which does not contain any variables not\n  ## present for the device is used as the device name tag.\n  ## The typical use case is for LVM volumes, to get the VG/LV name instead of\n  ## the near-meaningless DM-0 name.\n  # name_templates = [\"$ID_FS_LABEL\",\"$DM_VG_NAME/$DM_LV_NAME\"]\n\n"
    },
    {
      "type": "input",
      "name": "http",
      "description": "Read formatted metrics from one or more HTTP endpoints",
      "config": "# Read formatted metrics from one or more HTTP endpoints\n[[inputs.http]]\n  # alias=\"http\"\n  ## One or more URLs from which to read formatted metrics\n  urls = [\n    \"http://localhost/metrics\"\n  ]\n\n  ## HTTP method\n  # method = \"GET\"\n\n  ## Optional HTTP headers\n  # headers = {\"X-Special-Header\" = \"Special-Value\"}\n\n  ## Optional HTTP Basic Auth Credentials\n  # username = \"username\"\n  # password = \"pa$$word\"\n\n  ## HTTP entity-body to send with POST/PUT requests.\n  # body = \"\"\n\n  ## HTTP Content-Encoding for write request body, can be set to \"gzip\" to\n  ## compress body or \"identity\" to apply no encoding.\n  # content_encoding = \"identity\"\n\n  ## Optional TLS Config\n  # tls_ca = \"/etc/telegraf/ca.pem\"\n  # tls_cert = \"/etc/telegraf/cert.pem\"\n  # tls_key = \"/etc/telegraf/key.pem\"\n  ## Use TLS but skip chain \u0026 host verification\n  # insecure_skip_verify = false\n\n  ## Amount of time allowed to complete the HTTP request\n  # timeout = \"5s\"\n\n  ## List of success status codes\n  # success_status_codes = [200]\n\n  ## Data format to consume.\n  ## Each data format has its own unique set of configuration options, read\n  ## more about them here:\n  ## https://github.com/influxdata/telegraf/blob/master/docs/DATA_FORMATS_INPUT.md\n  # data_format = \"influx\"\n\n"
    },
    {
      "type": "input",
      "name": "uwsgi",
      "description": "Read uWSGI metrics.",
      "config": "# Read uWSGI metrics.\n[[inputs.uwsgi]]\n  # alias=\"uwsgi\"\n  ## List with urls of uWSGI Stats servers. URL must match pattern:\n  ## scheme://address[:port]\n  ##\n  ## For example:\n  ## servers = [\"tcp://localhost:5050\", \"http://localhost:1717\", \"unix:///tmp/statsock\"]\n  servers = [\"tcp://127.0.0.1:1717\"]\n\n  ## General connection timout\n  # timeout = \"5s\"\n\n"
    },
    {
      "type": "input",
      "name": "chrony",
      "description": "Get standard chrony metrics, requires chronyc executable.",
      "config": "# Get standard chrony metrics, requires chronyc executable.\n[[inputs.chrony]]\n  # alias=\"chrony\"\n  ## If true, chronyc tries to perform a DNS lookup for the time server.\n  # dns_lookup = false\n  \n"
    },
    {
      "type": "input",
      "name": "elasticsearch",
      "description": "Read stats from one or more Elasticsearch servers or clusters",
      "config": "# Read stats from one or more Elasticsearch servers or clusters\n[[inputs.elasticsearch]]\n  # alias=\"elasticsearch\"\n  ## specify a list of one or more Elasticsearch servers\n  # you can add username and password to your url to use basic authentication:\n  # servers = [\"http://user:pass@localhost:9200\"]\n  servers = [\"http://localhost:9200\"]\n\n  ## Timeout for HTTP requests to the elastic search server(s)\n  http_timeout = \"5s\"\n\n  ## When local is true (the default), the node will read only its own stats.\n  ## Set local to false when you want to read the node stats from all nodes\n  ## of the cluster.\n  local = true\n\n  ## Set cluster_health to true when you want to also obtain cluster health stats\n  cluster_health = false\n\n  ## Adjust cluster_health_level when you want to also obtain detailed health stats\n  ## The options are\n  ##  - indices (default)\n  ##  - cluster\n  # cluster_health_level = \"indices\"\n\n  ## Set cluster_stats to true when you want to also obtain cluster stats.\n  cluster_stats = false\n\n  ## Only gather cluster_stats from the master node. To work this require local = true\n  cluster_stats_only_from_master = true\n\n  ## Indices to collect; can be one or more indices names or _all\n  indices_include = [\"_all\"]\n\n  ## One of \"shards\", \"cluster\", \"indices\"\n  indices_level = \"shards\"\n\n  ## node_stats is a list of sub-stats that you want to have gathered. Valid options\n  ## are \"indices\", \"os\", \"process\", \"jvm\", \"thread_pool\", \"fs\", \"transport\", \"http\",\n  ## \"breaker\". Per default, all stats are gathered.\n  # node_stats = [\"jvm\", \"http\"]\n\n  ## HTTP Basic Authentication username and password.\n  # username = \"\"\n  # password = \"\"\n\n  ## Optional TLS Config\n  # tls_ca = \"/etc/telegraf/ca.pem\"\n  # tls_cert = \"/etc/telegraf/cert.pem\"\n  # tls_key = \"/etc/telegraf/key.pem\"\n  ## Use TLS but skip chain \u0026 host verification\n  # insecure_skip_verify = false\n\n"
    },
    {
      "type": "input",
      "name": "kafka_consumer",
      "description": "Read metrics from Kafka topics",
      "config": "# Read metrics from Kafka topics\n[[inputs.kafka_consumer]]\n  # alias=\"kafka_consumer\"\n  ## Kafka brokers.\n  brokers = [\"localhost:9092\"]\n\n  ## Topics to consume.\n  topics = [\"telegraf\"]\n\n  ## When set this tag will be added to all metrics with the topic as the value.\n  # topic_tag = \"\"\n\n  ## Optional Client id\n  # client_id = \"Telegraf\"\n\n  ## Set the minimal supported Kafka version.  Setting this enables the use of new\n  ## Kafka features and APIs.  Must be 0.10.2.0 or greater.\n  ##   ex: version = \"1.1.0\"\n  # version = \"\"\n\n  ## Optional TLS Config\n  # tls_ca = \"/etc/telegraf/ca.pem\"\n  # tls_cert = \"/etc/telegraf/cert.pem\"\n  # tls_key = \"/etc/telegraf/key.pem\"\n  ## Use TLS but skip chain \u0026 host verification\n  # insecure_skip_verify = false\n\n  ## Optional SASL Config\n  # sasl_username = \"kafka\"\n  # sasl_password = \"secret\"\n\n  ## Name of the consumer group.\n  # consumer_group = \"telegraf_metrics_consumers\"\n\n  ## Initial offset position; one of \"oldest\" or \"newest\".\n  # offset = \"oldest\"\n\n  ## Consumer group partition assignment strategy; one of \"range\", \"roundrobin\" or \"sticky\".\n  # balance_strategy = \"range\"\n\n  ## Maximum length of a message to consume, in bytes (default 0/unlimited);\n  ## larger messages are dropped\n  max_message_len = 1000000\n\n  ## Maximum messages to read from the broker that have not been written by an\n  ## output.  For best throughput set based on the number of metrics within\n  ## each message and the size of the output's metric_batch_size.\n  ##\n  ## For example, if each message from the queue contains 10 metrics and the\n  ## output metric_batch_size is 1000, setting this to 100 will ensure that a\n  ## full batch is collected and the write is triggered immediately without\n  ## waiting until the next flush_interval.\n  # max_undelivered_messages = 1000\n\n  ## Data format to consume.\n  ## Each data format has its own unique set of configuration options, read\n  ## more about them here:\n  ## https://github.com/influxdata/telegraf/blob/master/docs/DATA_FORMATS_INPUT.md\n  data_format = \"influx\"\n\n"
    },
    {
      "type": "input",
      "name": "tail",
      "description": "Stream a log file, like the tail -f command",
      "config": "# Stream a log file, like the tail -f command\n[[inputs.tail]]\n  # alias=\"tail\"\n  ## files to tail.\n  ## These accept standard unix glob matching rules, but with the addition of\n  ## ** as a \"super asterisk\". ie:\n  ##   \"/var/log/**.log\"  -\u003e recursively find all .log files in /var/log\n  ##   \"/var/log/*/*.log\" -\u003e find all .log files with a parent dir in /var/log\n  ##   \"/var/log/apache.log\" -\u003e just tail the apache log file\n  ##\n  ## See https://github.com/gobwas/glob for more examples\n  ##\n  files = [\"/var/mymetrics.out\"]\n  ## Read file from beginning.\n  from_beginning = false\n  ## Whether file is a named pipe\n  pipe = false\n\n  ## Method used to watch for file updates.  Can be either \"inotify\" or \"poll\".\n  # watch_method = \"inotify\"\n\n  ## Data format to consume.\n  ## Each data format has its own unique set of configuration options, read\n  ## more about them here:\n  ## https://github.com/influxdata/telegraf/blob/master/docs/DATA_FORMATS_INPUT.md\n  data_format = \"influx\"\n\n"
    },
    {
      "type": "input",
      "name": "udp_listener",
      "description": "Generic UDP listener",
      "config": "# Generic UDP listener\n[[inputs.udp_listener]]\n  # alias=\"udp_listener\"\n  # DEPRECATED: the TCP listener plugin has been deprecated in favor of the\n  # socket_listener plugin\n  # see https://github.com/influxdata/telegraf/tree/master/plugins/inputs/socket_listener\n\n"
    },
    {
      "type": "input",
      "name": "beanstalkd",
      "description": "Collects Beanstalkd server and tubes stats",
      "config": "# Collects Beanstalkd server and tubes stats\n[[inputs.beanstalkd]]\n  # alias=\"beanstalkd\"\n  ## Server to collect data from\n  server = \"localhost:11300\"\n\n  ## List of tubes to gather stats about.\n  ## If no tubes specified then data gathered for each tube on server reported by list-tubes command\n  tubes = [\"notifications\"]\n\n"
    },
    {
      "type": "input",
      "name": "github",
      "description": "Gather repository information from GitHub hosted repositories.",
      "config": "# Gather repository information from GitHub hosted repositories.\n[[inputs.github]]\n  # alias=\"github\"\n  ## List of repositories to monitor.\n  repositories = [\n\t  \"influxdata/telegraf\",\n\t  \"influxdata/influxdb\"\n  ]\n\n  ## Github API access token.  Unauthenticated requests are limited to 60 per hour.\n  # access_token = \"\"\n\n  ## Github API enterprise url. Github Enterprise accounts must specify their base url.\n  # enterprise_base_url = \"\"\n\n  ## Timeout for HTTP requests.\n  # http_timeout = \"5s\"\n\n"
    },
    {
      "type": "input",
      "name": "logparser",
      "description": "Stream and parse log file(s).",
      "config": "# Stream and parse log file(s).\n[[inputs.logparser]]\n  # alias=\"logparser\"\n  ## Log files to parse.\n  ## These accept standard unix glob matching rules, but with the addition of\n  ## ** as a \"super asterisk\". ie:\n  ##   /var/log/**.log     -\u003e recursively find all .log files in /var/log\n  ##   /var/log/*/*.log    -\u003e find all .log files with a parent dir in /var/log\n  ##   /var/log/apache.log -\u003e only tail the apache log file\n  files = [\"/var/log/apache/access.log\"]\n\n  ## Read files that currently exist from the beginning. Files that are created\n  ## while telegraf is running (and that match the \"files\" globs) will always\n  ## be read from the beginning.\n  from_beginning = false\n\n  ## Method used to watch for file updates.  Can be either \"inotify\" or \"poll\".\n  # watch_method = \"inotify\"\n\n  ## Parse logstash-style \"grok\" patterns:\n  [inputs.logparser.grok]\n    ## This is a list of patterns to check the given log file(s) for.\n    ## Note that adding patterns here increases processing time. The most\n    ## efficient configuration is to have one pattern per logparser.\n    ## Other common built-in patterns are:\n    ##   %{COMMON_LOG_FORMAT}   (plain apache \u0026 nginx access logs)\n    ##   %{COMBINED_LOG_FORMAT} (access logs + referrer \u0026 agent)\n    patterns = [\"%{COMBINED_LOG_FORMAT}\"]\n\n    ## Name of the outputted measurement name.\n    measurement = \"apache_access_log\"\n\n    ## Full path(s) to custom pattern files.\n    custom_pattern_files = []\n\n    ## Custom patterns can also be defined here. Put one pattern per line.\n    custom_patterns = '''\n    '''\n\n    ## Timezone allows you to provide an override for timestamps that\n    ## don't already include an offset\n    ## e.g. 04/06/2016 12:41:45 data one two 5.43µs\n    ##\n    ## Default: \"\" which renders UTC\n    ## Options are as follows:\n    ##   1. Local             -- interpret based on machine localtime\n    ##   2. \"Canada/Eastern\"  -- Unix TZ values like those found in https://en.wikipedia.org/wiki/List_of_tz_database_time_zones\n    ##   3. UTC               -- or blank/unspecified, will return timestamp in UTC\n    # timezone = \"Canada/Eastern\"\n\n\t## When set to \"disable\", timestamp will not incremented if there is a\n\t## duplicate.\n    # unique_timestamp = \"auto\"\n\n"
    },
    {
      "type": "input",
      "name": "tomcat",
      "description": "Gather metrics from the Tomcat server status page.",
      "config": "# Gather metrics from the Tomcat server status page.\n[[inputs.tomcat]]\n  # alias=\"tomcat\"\n  ## URL of the Tomcat server status\n  # url = \"http://127.0.0.1:8080/manager/status/all?XML=true\"\n\n  ## HTTP Basic Auth Credentials\n  # username = \"tomcat\"\n  # password = \"s3cret\"\n\n  ## Request timeout\n  # timeout = \"5s\"\n\n  ## Optional TLS Config\n  # tls_ca = \"/etc/telegraf/ca.pem\"\n  # tls_cert = \"/etc/telegraf/cert.pem\"\n  # tls_key = \"/etc/telegraf/key.pem\"\n  ## Use TLS but skip chain \u0026 host verification\n  # insecure_skip_verify = false\n\n"
    },
    {
      "type": "input",
      "name": "twemproxy",
      "description": "Read Twemproxy stats data",
      "config": "# Read Twemproxy stats data\n[[inputs.twemproxy]]\n  # alias=\"twemproxy\"\n  ## Twemproxy stats address and port (no scheme)\n  addr = \"localhost:22222\"\n  ## Monitor pool name\n  pools = [\"redis_pool\", \"mc_pool\"]\n\n"
    },
    {
      "type": "input",
      "name": "influxdb_listener",
      "description": "Influx HTTP write listener",
      "config": "# Influx HTTP write listener\n[[inputs.influxdb_listener]]\n  # alias=\"influxdb_listener\"\n  ## Address and port to host HTTP listener on\n  service_address = \":8186\"\n\n  ## maximum duration before timing out read of the request\n  read_timeout = \"10s\"\n  ## maximum duration before timing out write of the response\n  write_timeout = \"10s\"\n\n  ## Maximum allowed http request body size in bytes.\n  ## 0 means to use the default of 524,288,000 bytes (500 mebibytes)\n  max_body_size = \"500MiB\"\n\n  ## Maximum line size allowed to be sent in bytes.\n  ## 0 means to use the default of 65536 bytes (64 kibibytes)\n  max_line_size = \"64KiB\"\n  \n\n  ## Optional tag name used to store the database. \n  ## If the write has a database in the query string then it will be kept in this tag name.\n  ## This tag can be used in downstream outputs.\n  ## The default value of nothing means it will be off and the database will not be recorded.\n  # database_tag = \"\"\n\n  ## Set one or more allowed client CA certificate file names to\n  ## enable mutually authenticated TLS connections\n  tls_allowed_cacerts = [\"/etc/telegraf/clientca.pem\"]\n\n  ## Add service certificate and key\n  tls_cert = \"/etc/telegraf/cert.pem\"\n  tls_key = \"/etc/telegraf/key.pem\"\n\n  ## Optional username and password to accept for HTTP basic authentication.\n  ## You probably want to make sure you have TLS configured above for this.\n  # basic_username = \"foobar\"\n  # basic_password = \"barfoo\"\n\n"
    },
    {
      "type": "input",
      "name": "jti_openconfig_telemetry",
      "description": "Read JTI OpenConfig Telemetry from listed sensors",
      "config": "# Read JTI OpenConfig Telemetry from listed sensors\n[[inputs.jti_openconfig_telemetry]]\n  # alias=\"jti_openconfig_telemetry\"\n  ## List of device addresses to collect telemetry from\n  servers = [\"localhost:1883\"]\n\n  ## Authentication details. Username and password are must if device expects\n  ## authentication. Client ID must be unique when connecting from multiple instances\n  ## of telegraf to the same device\n  username = \"user\"\n  password = \"pass\"\n  client_id = \"telegraf\"\n\n  ## Frequency to get data\n  sample_frequency = \"1000ms\"\n\n  ## Sensors to subscribe for\n  ## A identifier for each sensor can be provided in path by separating with space\n  ## Else sensor path will be used as identifier\n  ## When identifier is used, we can provide a list of space separated sensors.\n  ## A single subscription will be created with all these sensors and data will\n  ## be saved to measurement with this identifier name\n  sensors = [\n   \"/interfaces/\",\n   \"collection /components/ /lldp\",\n  ]\n\n  ## We allow specifying sensor group level reporting rate. To do this, specify the\n  ## reporting rate in Duration at the beginning of sensor paths / collection\n  ## name. For entries without reporting rate, we use configured sample frequency\n  sensors = [\n   \"1000ms customReporting /interfaces /lldp\",\n   \"2000ms collection /components\",\n   \"/interfaces\",\n  ]\n\n  ## Optional TLS Config\n  # enable_tls = true\n  # tls_ca = \"/etc/telegraf/ca.pem\"\n  # tls_cert = \"/etc/telegraf/cert.pem\"\n  # tls_key = \"/etc/telegraf/key.pem\"\n  ## Use TLS but skip chain \u0026 host verification\n  # insecure_skip_verify = false\n\n  ## Delay between retry attempts of failed RPC calls or streams. Defaults to 1000ms.\n  ## Failed streams/calls will not be retried if 0 is provided\n  retry_delay = \"1000ms\"\n\n  ## To treat all string values as tags, set this to true\n  str_as_tags = false\n\n"
    },
    {
      "type": "input",
      "name": "kinesis_consumer",
      "description": "Configuration for the AWS Kinesis input.",
      "config": "# Configuration for the AWS Kinesis input.\n[[inputs.kinesis_consumer]]\n  # alias=\"kinesis_consumer\"\n  ## Amazon REGION of kinesis endpoint.\n  region = \"ap-southeast-2\"\n\n  ## Amazon Credentials\n  ## Credentials are loaded in the following order\n  ## 1) Assumed credentials via STS if role_arn is specified\n  ## 2) explicit credentials from 'access_key' and 'secret_key'\n  ## 3) shared profile from 'profile'\n  ## 4) environment variables\n  ## 5) shared credentials file\n  ## 6) EC2 Instance Profile\n  # access_key = \"\"\n  # secret_key = \"\"\n  # token = \"\"\n  # role_arn = \"\"\n  # profile = \"\"\n  # shared_credential_file = \"\"\n\n  ## Endpoint to make request against, the correct endpoint is automatically\n  ## determined and this option should only be set if you wish to override the\n  ## default.\n  ##   ex: endpoint_url = \"http://localhost:8000\"\n  # endpoint_url = \"\"\n\n  ## Kinesis StreamName must exist prior to starting telegraf.\n  streamname = \"StreamName\"\n\n  ## Shard iterator type (only 'TRIM_HORIZON' and 'LATEST' currently supported)\n  # shard_iterator_type = \"TRIM_HORIZON\"\n\n  ## Maximum messages to read from the broker that have not been written by an\n  ## output.  For best throughput set based on the number of metrics within\n  ## each message and the size of the output's metric_batch_size.\n  ##\n  ## For example, if each message from the queue contains 10 metrics and the\n  ## output metric_batch_size is 1000, setting this to 100 will ensure that a\n  ## full batch is collected and the write is triggered immediately without\n  ## waiting until the next flush_interval.\n  # max_undelivered_messages = 1000\n\n  ## Data format to consume.\n  ## Each data format has its own unique set of configuration options, read\n  ## more about them here:\n  ## https://github.com/influxdata/telegraf/blob/master/docs/DATA_FORMATS_INPUT.md\n  data_format = \"influx\"\n\n  ## Optional\n  ## Configuration for a dynamodb checkpoint\n  [inputs.kinesis_consumer.checkpoint_dynamodb]\n\t## unique name for this consumer\n\tapp_name = \"default\"\n\ttable_name = \"default\"\n\n"
    },
    {
      "type": "input",
      "name": "pgbouncer",
      "description": "Read metrics from one or many pgbouncer servers",
      "config": "# Read metrics from one or many pgbouncer servers\n[[inputs.pgbouncer]]\n  # alias=\"pgbouncer\"\n  ## specify address via a url matching:\n  ##   postgres://[pqgotest[:password]]@localhost[/dbname]\\\n  ##       ?sslmode=[disable|verify-ca|verify-full]\n  ## or a simple string:\n  ##   host=localhost user=pqotest password=... sslmode=... dbname=app_production\n  ##\n  ## All connection parameters are optional.\n  ##\n  address = \"host=localhost user=pgbouncer sslmode=disable\"\n\n"
    },
    {
      "type": "input",
      "name": "internal",
      "description": "Collect statistics about itself",
      "config": "# Collect statistics about itself\n[[inputs.internal]]\n  # alias=\"internal\"\n  ## If true, collect telegraf memory stats.\n  # collect_memstats = true\n\n"
    },
    {
      "type": "input",
      "name": "mcrouter",
      "description": "Read metrics from one or many mcrouter servers",
      "config": "# Read metrics from one or many mcrouter servers\n[[inputs.mcrouter]]\n  # alias=\"mcrouter\"\n  ## An array of address to gather stats about. Specify an ip or hostname\n  ## with port. ie tcp://localhost:11211, tcp://10.0.0.1:11211, etc.\n\tservers = [\"tcp://localhost:11211\", \"unix:///var/run/mcrouter.sock\"]\n\n\t## Timeout for metric collections from all servers.  Minimum timeout is \"1s\".\n  # timeout = \"5s\"\n\n"
    },
    {
      "type": "input",
      "name": "postgresql_extensible",
      "description": "Read metrics from one or many postgresql servers",
      "config": "# Read metrics from one or many postgresql servers\n[[inputs.postgresql_extensible]]\n  # alias=\"postgresql_extensible\"\n  ## specify address via a url matching:\n  ##   postgres://[pqgotest[:password]]@localhost[/dbname]\\\n  ##       ?sslmode=[disable|verify-ca|verify-full]\n  ## or a simple string:\n  ##   host=localhost user=pqotest password=... sslmode=... dbname=app_production\n  #\n  ## All connection parameters are optional.  #\n  ## Without the dbname parameter, the driver will default to a database\n  ## with the same name as the user. This dbname is just for instantiating a\n  ## connection with the server and doesn't restrict the databases we are trying\n  ## to grab metrics for.\n  #\n  address = \"host=localhost user=postgres sslmode=disable\"\n\n  ## connection configuration.\n  ## maxlifetime - specify the maximum lifetime of a connection.\n  ## default is forever (0s)\n  max_lifetime = \"0s\"\n\n  ## A list of databases to pull metrics about. If not specified, metrics for all\n  ## databases are gathered.\n  ## databases = [\"app_production\", \"testing\"]\n  #\n  ## A custom name for the database that will be used as the \"server\" tag in the\n  ## measurement output. If not specified, a default one generated from\n  ## the connection address is used.\n  # outputaddress = \"db01\"\n  #\n  ## Define the toml config where the sql queries are stored\n  ## New queries can be added, if the withdbname is set to true and there is no\n  ## databases defined in the 'databases field', the sql query is ended by a\n  ## 'is not null' in order to make the query succeed.\n  ## Example :\n  ## The sqlquery : \"SELECT * FROM pg_stat_database where datname\" become\n  ## \"SELECT * FROM pg_stat_database where datname IN ('postgres', 'pgbench')\"\n  ## because the databases variable was set to ['postgres', 'pgbench' ] and the\n  ## withdbname was true. Be careful that if the withdbname is set to false you\n  ## don't have to define the where clause (aka with the dbname) the tagvalue\n  ## field is used to define custom tags (separated by commas)\n  ## The optional \"measurement\" value can be used to override the default\n  ## output measurement name (\"postgresql\").\n  ##\n  ## The script option can be used to specify the .sql file path.\n  ## If script and sqlquery options specified at same time, sqlquery will be used \n  ##\n  ## Structure :\n  ## [[inputs.postgresql_extensible.query]]\n  ##   sqlquery string\n  ##   version string\n  ##   withdbname boolean\n  ##   tagvalue string (comma separated)\n  ##   measurement string\n  [[inputs.postgresql_extensible.query]]\n    sqlquery=\"SELECT * FROM pg_stat_database\"\n    version=901\n    withdbname=false\n    tagvalue=\"\"\n    measurement=\"\"\n  [[inputs.postgresql_extensible.query]]\n    sqlquery=\"SELECT * FROM pg_stat_bgwriter\"\n    version=901\n    withdbname=false\n    tagvalue=\"postgresql.stats\"\n\n"
    },
    {
      "type": "input",
      "name": "varnish",
      "description": "A plugin to collect stats from Varnish HTTP Cache",
      "config": "# A plugin to collect stats from Varnish HTTP Cache\n[[inputs.varnish]]\n  # alias=\"varnish\"\n  ## If running as a restricted user you can prepend sudo for additional access:\n  #use_sudo = false\n\n  ## The default location of the varnishstat binary can be overridden with:\n  binary = \"/usr/bin/varnishstat\"\n\n  ## By default, telegraf gather stats for 3 metric points.\n  ## Setting stats will override the defaults shown below.\n  ## Glob matching can be used, ie, stats = [\"MAIN.*\"]\n  ## stats may also be set to [\"*\"], which will collect all stats\n  stats = [\"MAIN.cache_hit\", \"MAIN.cache_miss\", \"MAIN.uptime\"]\n\n  ## Optional name for the varnish instance (or working directory) to query\n  ## Usually appened after -n in varnish cli\n  # instance_name = instanceName\n\n  ## Timeout for varnishstat command\n  # timeout = \"1s\"\n\n"
    },
    {
      "type": "input",
      "name": "wireless",
      "description": "Monitor wifi signal strength and quality",
      "config": "# Monitor wifi signal strength and quality\n[[inputs.wireless]]\n  # alias=\"wireless\"\n  ## Sets 'proc' directory path\n  ## If not specified, then default is /proc\n  # host_proc = \"/proc\"\n\n"
    },
    {
      "type": "input",
      "name": "rabbitmq",
      "description": "Reads metrics from RabbitMQ servers via the Management Plugin",
      "config": "# Reads metrics from RabbitMQ servers via the Management Plugin\n[[inputs.rabbitmq]]\n  # alias=\"rabbitmq\"\n  ## Management Plugin url. (default: http://localhost:15672)\n  # url = \"http://localhost:15672\"\n  ## Tag added to rabbitmq_overview series; deprecated: use tags\n  # name = \"rmq-server-1\"\n  ## Credentials\n  # username = \"guest\"\n  # password = \"guest\"\n\n  ## Optional TLS Config\n  # tls_ca = \"/etc/telegraf/ca.pem\"\n  # tls_cert = \"/etc/telegraf/cert.pem\"\n  # tls_key = \"/etc/telegraf/key.pem\"\n  ## Use TLS but skip chain \u0026 host verification\n  # insecure_skip_verify = false\n\n  ## Optional request timeouts\n  ##\n  ## ResponseHeaderTimeout, if non-zero, specifies the amount of time to wait\n  ## for a server's response headers after fully writing the request.\n  # header_timeout = \"3s\"\n  ##\n  ## client_timeout specifies a time limit for requests made by this client.\n  ## Includes connection time, any redirects, and reading the response body.\n  # client_timeout = \"4s\"\n\n  ## A list of nodes to gather as the rabbitmq_node measurement. If not\n  ## specified, metrics for all nodes are gathered.\n  # nodes = [\"rabbit@node1\", \"rabbit@node2\"]\n\n  ## A list of queues to gather as the rabbitmq_queue measurement. If not\n  ## specified, metrics for all queues are gathered.\n  # queues = [\"telegraf\"]\n\n  ## A list of exchanges to gather as the rabbitmq_exchange measurement. If not\n  ## specified, metrics for all exchanges are gathered.\n  # exchanges = [\"telegraf\"]\n\n  ## Queues to include and exclude. Globs accepted.\n  ## Note that an empty array for both will include all queues\n  queue_name_include = []\n  queue_name_exclude = []\n\n  ## Federation upstreams include and exclude when gathering the rabbitmq_federation measurement.\n  ## If neither are specified, metrics for all federation upstreams are gathered.\n  ## Federation link metrics will only be gathered for queues and exchanges\n  ## whose non-federation metrics will be collected (e.g a queue excluded\n  ## by the 'queue_name_exclude' option will also be excluded from federation).\n  ## Globs accepted.\n  # federation_upstream_include = [\"dataCentre-*\"]\n  # federation_upstream_exclude = []\n\n"
    },
    {
      "type": "input",
      "name": "x509_cert",
      "description": "Reads metrics from a SSL certificate",
      "config": "# Reads metrics from a SSL certificate\n[[inputs.x509_cert]]\n  # alias=\"x509_cert\"\n  ## List certificate sources\n  sources = [\"/etc/ssl/certs/ssl-cert-snakeoil.pem\", \"tcp://example.org:443\"]\n\n  ## Timeout for SSL connection\n  # timeout = \"5s\"\n\n  ## Optional TLS Config\n  # tls_ca = \"/etc/telegraf/ca.pem\"\n  # tls_cert = \"/etc/telegraf/cert.pem\"\n  # tls_key = \"/etc/telegraf/key.pem\"\n\n"
    },
    {
      "type": "input",
      "name": "cassandra",
      "description": "Read Cassandra metrics through Jolokia",
      "config": "# Read Cassandra metrics through Jolokia\n[[inputs.cassandra]]\n  # alias=\"cassandra\"\n  ## DEPRECATED: The cassandra plugin has been deprecated.  Please use the\n  ## jolokia2 plugin instead.\n  ##\n  ## see https://github.com/influxdata/telegraf/tree/master/plugins/inputs/jolokia2\n\n  context = \"/jolokia/read\"\n  ## List of cassandra servers exposing jolokia read service\n  servers = [\"myuser:mypassword@10.10.10.1:8778\",\"10.10.10.2:8778\",\":8778\"]\n  ## List of metrics collected on above servers\n  ## Each metric consists of a jmx path.\n  ## This will collect all heap memory usage metrics from the jvm and\n  ## ReadLatency metrics for all keyspaces and tables.\n  ## \"type=Table\" in the query works with Cassandra3.0. Older versions might\n  ## need to use \"type=ColumnFamily\"\n  metrics  = [\n    \"/java.lang:type=Memory/HeapMemoryUsage\",\n    \"/org.apache.cassandra.metrics:type=Table,keyspace=*,scope=*,name=ReadLatency\"\n  ]\n\n"
    },
    {
      "type": "input",
      "name": "cloud_pubsub",
      "description": "Read metrics from Google PubSub",
      "config": "# Read metrics from Google PubSub\n[[inputs.cloud_pubsub]]\n  # alias=\"cloud_pubsub\"\n  ## Required. Name of Google Cloud Platform (GCP) Project that owns\n  ## the given PubSub subscription.\n  project = \"my-project\"\n\n  ## Required. Name of PubSub subscription to ingest metrics from.\n  subscription = \"my-subscription\"\n\n  ## Required. Data format to consume.\n  ## Each data format has its own unique set of configuration options.\n  ## Read more about them here:\n  ## https://github.com/influxdata/telegraf/blob/master/docs/DATA_FORMATS_INPUT.md\n  data_format = \"influx\"\n\n  ## Optional. Filepath for GCP credentials JSON file to authorize calls to\n  ## PubSub APIs. If not set explicitly, Telegraf will attempt to use\n  ## Application Default Credentials, which is preferred.\n  # credentials_file = \"path/to/my/creds.json\"\n\n  ## Optional. Number of seconds to wait before attempting to restart the \n  ## PubSub subscription receiver after an unexpected error. \n  ## If the streaming pull for a PubSub Subscription fails (receiver),\n  ## the agent attempts to restart receiving messages after this many seconds.\n  # retry_delay_seconds = 5\n\n  ## Optional. Maximum byte length of a message to consume.\n  ## Larger messages are dropped with an error. If less than 0 or unspecified,\n  ## treated as no limit.\n  # max_message_len = 1000000\n\n  ## Optional. Maximum messages to read from PubSub that have not been written\n  ## to an output. Defaults to 1000.\n  ## For best throughput set based on the number of metrics within\n  ## each message and the size of the output's metric_batch_size.\n  ##\n  ## For example, if each message contains 10 metrics and the output\n  ## metric_batch_size is 1000, setting this to 100 will ensure that a\n  ## full batch is collected and the write is triggered immediately without\n  ## waiting until the next flush_interval.\n  # max_undelivered_messages = 1000\n\n  ## The following are optional Subscription ReceiveSettings in PubSub.\n  ## Read more about these values:\n  ## https://godoc.org/cloud.google.com/go/pubsub#ReceiveSettings\n\n  ## Optional. Maximum number of seconds for which a PubSub subscription\n  ## should auto-extend the PubSub ACK deadline for each message. If less than\n  ## 0, auto-extension is disabled.\n  # max_extension = 0\n\n  ## Optional. Maximum number of unprocessed messages in PubSub\n  ## (unacknowledged but not yet expired in PubSub).\n  ## A value of 0 is treated as the default PubSub value.\n  ## Negative values will be treated as unlimited.\n  # max_outstanding_messages = 0\n\n  ## Optional. Maximum size in bytes of unprocessed messages in PubSub\n  ## (unacknowledged but not yet expired in PubSub).\n  ## A value of 0 is treated as the default PubSub value.\n  ## Negative values will be treated as unlimited.\n  # max_outstanding_bytes = 0\n\n  ## Optional. Max number of goroutines a PubSub Subscription receiver can spawn\n  ## to pull messages from PubSub concurrently. This limit applies to each\n  ## subscription separately and is treated as the PubSub default if less than\n  ## 1. Note this setting does not limit the number of messages that can be\n  ## processed concurrently (use \"max_outstanding_messages\" instead).\n  # max_receiver_go_routines = 0\n\n  ## Optional. If true, Telegraf will attempt to base64 decode the \n  ## PubSub message data before parsing\n  # base64_data = false\n\n"
    },
    {
      "type": "input",
      "name": "ipmi_sensor",
      "description": "Read metrics from the bare metal servers via IPMI",
      "config": "# Read metrics from the bare metal servers via IPMI\n[[inputs.ipmi_sensor]]\n  # alias=\"ipmi_sensor\"\n  ## optionally specify the path to the ipmitool executable\n  # path = \"/usr/bin/ipmitool\"\n  ##\n  ## optionally force session privilege level. Can be CALLBACK, USER, OPERATOR, ADMINISTRATOR\n  # privilege = \"ADMINISTRATOR\"\n  ##\n  ## optionally specify one or more servers via a url matching\n  ##  [username[:password]@][protocol[(address)]]\n  ##  e.g.\n  ##    root:passwd@lan(127.0.0.1)\n  ##\n  ## if no servers are specified, local machine sensor stats will be queried\n  ##\n  # servers = [\"USERID:PASSW0RD@lan(192.168.1.1)\"]\n\n  ## Recommended: use metric 'interval' that is a multiple of 'timeout' to avoid\n  ## gaps or overlap in pulled data\n  interval = \"30s\"\n\n  ## Timeout for the ipmitool command to complete\n  timeout = \"20s\"\n\n  ## Schema Version: (Optional, defaults to version 1)\n  metric_version = 2\n\n"
    },
    {
      "type": "input",
      "name": "jolokia",
      "description": "Read JMX metrics through Jolokia",
      "config": "# Read JMX metrics through Jolokia\n[[inputs.jolokia]]\n  # alias=\"jolokia\"\n  # DEPRECATED: the jolokia plugin has been deprecated in favor of the\n  # jolokia2 plugin\n  # see https://github.com/influxdata/telegraf/tree/master/plugins/inputs/jolokia2\n\n  ## This is the context root used to compose the jolokia url\n  ## NOTE that Jolokia requires a trailing slash at the end of the context root\n  ## NOTE that your jolokia security policy must allow for POST requests.\n  context = \"/jolokia/\"\n\n  ## This specifies the mode used\n  # mode = \"proxy\"\n  #\n  ## When in proxy mode this section is used to specify further\n  ## proxy address configurations.\n  ## Remember to change host address to fit your environment.\n  # [inputs.jolokia.proxy]\n  #   host = \"127.0.0.1\"\n  #   port = \"8080\"\n\n  ## Optional http timeouts\n  ##\n  ## response_header_timeout, if non-zero, specifies the amount of time to wait\n  ## for a server's response headers after fully writing the request.\n  # response_header_timeout = \"3s\"\n  ##\n  ## client_timeout specifies a time limit for requests made by this client.\n  ## Includes connection time, any redirects, and reading the response body.\n  # client_timeout = \"4s\"\n\n  ## Attribute delimiter\n  ##\n  ## When multiple attributes are returned for a single\n  ## [inputs.jolokia.metrics], the field name is a concatenation of the metric\n  ## name, and the attribute name, separated by the given delimiter.\n  # delimiter = \"_\"\n\n  ## List of servers exposing jolokia read service\n  [[inputs.jolokia.servers]]\n    name = \"as-server-01\"\n    host = \"127.0.0.1\"\n    port = \"8080\"\n    # username = \"myuser\"\n    # password = \"mypassword\"\n\n  ## List of metrics collected on above servers\n  ## Each metric consists in a name, a jmx path and either\n  ## a pass or drop slice attribute.\n  ## This collect all heap memory usage metrics.\n  [[inputs.jolokia.metrics]]\n    name = \"heap_memory_usage\"\n    mbean  = \"java.lang:type=Memory\"\n    attribute = \"HeapMemoryUsage\"\n\n  ## This collect thread counts metrics.\n  [[inputs.jolokia.metrics]]\n    name = \"thread_count\"\n    mbean  = \"java.lang:type=Threading\"\n    attribute = \"TotalStartedThreadCount,ThreadCount,DaemonThreadCount,PeakThreadCount\"\n\n  ## This collect number of class loaded/unloaded counts metrics.\n  [[inputs.jolokia.metrics]]\n    name = \"class_count\"\n    mbean  = \"java.lang:type=ClassLoading\"\n    attribute = \"LoadedClassCount,UnloadedClassCount,TotalLoadedClassCount\"\n\n"
    },
    {
      "type": "input",
      "name": "mem",
      "description": "Read metrics about memory usage",
      "config": "# Read metrics about memory usage\n[[inputs.mem]]\n  # alias=\"mem\"\n"
    },
    {
      "type": "input",
      "name": "filecount",
      "description": "Count files in a directory",
      "config": "# Count files in a directory\n[[inputs.filecount]]\n  # alias=\"filecount\"\n  ## Directory to gather stats about.\n  ##   deprecated in 1.9; use the directories option\n  # directory = \"/var/cache/apt/archives\"\n\n  ## Directories to gather stats about.\n  ## This accept standard unit glob matching rules, but with the addition of\n  ## ** as a \"super asterisk\". ie:\n  ##   /var/log/**    -\u003e recursively find all directories in /var/log and count files in each directories\n  ##   /var/log/*/*   -\u003e find all directories with a parent dir in /var/log and count files in each directories\n  ##   /var/log       -\u003e count all files in /var/log and all of its subdirectories\n  directories = [\"/var/cache/apt/archives\"]\n\n  ## Only count files that match the name pattern. Defaults to \"*\".\n  name = \"*.deb\"\n\n  ## Count files in subdirectories. Defaults to true.\n  recursive = false\n\n  ## Only count regular files. Defaults to true.\n  regular_only = true\n\n  ## Follow all symlinks while walking the directory tree. Defaults to false.\n  follow_symlinks = false\n\n  ## Only count files that are at least this size. If size is\n  ## a negative number, only count files that are smaller than the\n  ## absolute value of size. Acceptable units are B, KiB, MiB, KB, ...\n  ## Without quotes and units, interpreted as size in bytes.\n  size = \"0B\"\n\n  ## Only count files that have not been touched for at least this\n  ## duration. If mtime is negative, only count files that have been\n  ## touched in this duration. Defaults to \"0s\".\n  mtime = \"0s\"\n\n"
    },
    {
      "type": "input",
      "name": "kafka_consumer_legacy",
      "description": "Read metrics from Kafka topic(s)",
      "config": "# Read metrics from Kafka topic(s)\n[[inputs.kafka_consumer_legacy]]\n  # alias=\"kafka_consumer_legacy\"\n  ## topic(s) to consume\n  topics = [\"telegraf\"]\n\n  ## an array of Zookeeper connection strings\n  zookeeper_peers = [\"localhost:2181\"]\n\n  ## Zookeeper Chroot\n  zookeeper_chroot = \"\"\n\n  ## the name of the consumer group\n  consumer_group = \"telegraf_metrics_consumers\"\n\n  ## Offset (must be either \"oldest\" or \"newest\")\n  offset = \"oldest\"\n\n  ## Data format to consume.\n  ## Each data format has its own unique set of configuration options, read\n  ## more about them here:\n  ## https://github.com/influxdata/telegraf/blob/master/docs/DATA_FORMATS_INPUT.md\n  data_format = \"influx\"\n\n  ## Maximum length of a message to consume, in bytes (default 0/unlimited);\n  ## larger messages are dropped\n  max_message_len = 65536\n\n"
    },
    {
      "type": "input",
      "name": "net",
      "description": "Read metrics about network interface usage",
      "config": "# Read metrics about network interface usage\n[[inputs.net]]\n  # alias=\"net\"\n  ## By default, telegraf gathers stats from any up interface (excluding loopback)\n  ## Setting interfaces will tell it to gather these explicit interfaces,\n  ## regardless of status.\n  ##\n  # interfaces = [\"eth0\"]\n  ##\n  ## On linux systems telegraf also collects protocol stats.\n  ## Setting ignore_protocol_stats to true will skip reporting of protocol metrics.\n  ##\n  # ignore_protocol_stats = false\n  ##\n\n"
    },
    {
      "type": "input",
      "name": "nsq",
      "description": "Read NSQ topic and channel statistics.",
      "config": "# Read NSQ topic and channel statistics.\n[[inputs.nsq]]\n  # alias=\"nsq\"\n  ## An array of NSQD HTTP API endpoints\n  endpoints  = [\"http://localhost:4151\"]\n\n  ## Optional TLS Config\n  # tls_ca = \"/etc/telegraf/ca.pem\"\n  # tls_cert = \"/etc/telegraf/cert.pem\"\n  # tls_key = \"/etc/telegraf/key.pem\"\n  ## Use TLS but skip chain \u0026 host verification\n  # insecure_skip_verify = false\n\n"
    },
    {
      "type": "input",
      "name": "conntrack",
      "description": "Collects conntrack stats from the configured directories and files.",
      "config": "# Collects conntrack stats from the configured directories and files.\n[[inputs.conntrack]]\n  # alias=\"conntrack\"\n   ## The following defaults would work with multiple versions of conntrack.\n   ## Note the nf_ and ip_ filename prefixes are mutually exclusive across\n   ## kernel versions, as are the directory locations.\n\n   ## Superset of filenames to look for within the conntrack dirs.\n   ## Missing files will be ignored.\n   files = [\"ip_conntrack_count\",\"ip_conntrack_max\",\n            \"nf_conntrack_count\",\"nf_conntrack_max\"]\n\n   ## Directories to search within for the conntrack files above.\n   ## Missing directrories will be ignored.\n   dirs = [\"/proc/sys/net/ipv4/netfilter\",\"/proc/sys/net/netfilter\"]\n\n"
    },
    {
      "type": "input",
      "name": "iptables",
      "description": "Gather packets and bytes throughput from iptables",
      "config": "# Gather packets and bytes throughput from iptables\n[[inputs.iptables]]\n  # alias=\"iptables\"\n  ## iptables require root access on most systems.\n  ## Setting 'use_sudo' to true will make use of sudo to run iptables.\n  ## Users must configure sudo to allow telegraf user to run iptables with no password.\n  ## iptables can be restricted to only list command \"iptables -nvL\".\n  use_sudo = false\n  ## Setting 'use_lock' to true runs iptables with the \"-w\" option.\n  ## Adjust your sudo settings appropriately if using this option (\"iptables -w 5 -nvl\")\n  use_lock = false\n  ## Define an alternate executable, such as \"ip6tables\". Default is \"iptables\".\n  # binary = \"ip6tables\"\n  ## defines the table to monitor:\n  table = \"filter\"\n  ## defines the chains to monitor.\n  ## NOTE: iptables rules without a comment will not be monitored.\n  ## Read the plugin documentation for more information.\n  chains = [ \"INPUT\" ]\n\n"
    },
    {
      "type": "input",
      "name": "memcached",
      "description": "Read metrics from one or many memcached servers",
      "config": "# Read metrics from one or many memcached servers\n[[inputs.memcached]]\n  # alias=\"memcached\"\n  ## An array of address to gather stats about. Specify an ip on hostname\n  ## with optional port. ie localhost, 10.0.0.1:11211, etc.\n  servers = [\"localhost:11211\"]\n  # unix_sockets = [\"/var/run/memcached.sock\"]\n\n"
    },
    {
      "type": "input",
      "name": "snmp_trap",
      "description": "Receive SNMP traps",
      "config": "# Receive SNMP traps\n[[inputs.snmp_trap]]\n  # alias=\"snmp_trap\"\n  ## Transport, local address, and port to listen on.  Transport must\n  ## be \"udp://\".  Omit local address to listen on all interfaces.\n  ##   example: \"udp://127.0.0.1:1234\"\n  # service_address = udp://:162\n  ## Timeout running snmptranslate command\n  # timeout = \"5s\"\n\n"
    },
    {
      "type": "input",
      "name": "dns_query",
      "description": "Query given DNS server and gives statistics",
      "config": "# Query given DNS server and gives statistics\n[[inputs.dns_query]]\n  # alias=\"dns_query\"\n  ## servers to query\n  servers = [\"8.8.8.8\"]\n\n  ## Network is the network protocol name.\n  # network = \"udp\"\n\n  ## Domains or subdomains to query.\n  # domains = [\".\"]\n\n  ## Query record type.\n  ## Posible values: A, AAAA, CNAME, MX, NS, PTR, TXT, SOA, SPF, SRV.\n  # record_type = \"A\"\n\n  ## Dns server port.\n  # port = 53\n\n  ## Query timeout in seconds.\n  # timeout = 2\n\n"
    },
    {
      "type": "input",
      "name": "linux_sysctl_fs",
      "description": "Provides Linux sysctl fs metrics",
      "config": "# Provides Linux sysctl fs metrics\n[[inputs.linux_sysctl_fs]]\n  # alias=\"linux_sysctl_fs\"\n"
    },
    {
      "type": "input",
      "name": "netstat",
      "description": "Read TCP metrics such as established, time wait and sockets counts.",
      "config": "# Read TCP metrics such as established, time wait and sockets counts.\n[[inputs.netstat]]\n  # alias=\"netstat\"\n"
    },
    {
      "type": "input",
      "name": "postfix",
      "description": "Measure postfix queue statistics",
      "config": "# Measure postfix queue statistics\n[[inputs.postfix]]\n  # alias=\"postfix\"\n  ## Postfix queue directory. If not provided, telegraf will try to use\n  ## 'postconf -h queue_directory' to determine it.\n  # queue_directory = \"/var/spool/postfix\"\n\n"
    },
    {
      "type": "input",
      "name": "rethinkdb",
      "description": "Read metrics from one or many RethinkDB servers",
      "config": "# Read metrics from one or many RethinkDB servers\n[[inputs.rethinkdb]]\n  # alias=\"rethinkdb\"\n  ## An array of URI to gather stats about. Specify an ip or hostname\n  ## with optional port add password. ie,\n  ##   rethinkdb://user:auth_key@10.10.3.30:28105,\n  ##   rethinkdb://10.10.3.33:18832,\n  ##   10.0.0.1:10000, etc.\n  servers = [\"127.0.0.1:28015\"]\n  ##\n  ## If you use actual rethinkdb of \u003e 2.3.0 with username/password authorization,\n  ## protocol have to be named \"rethinkdb2\" - it will use 1_0 H.\n  # servers = [\"rethinkdb2://username:password@127.0.0.1:28015\"]\n  ##\n  ## If you use older versions of rethinkdb (\u003c2.2) with auth_key, protocol\n  ## have to be named \"rethinkdb\".\n  # servers = [\"rethinkdb://username:auth_key@127.0.0.1:28015\"]\n\n"
    },
    {
      "type": "input",
      "name": "bond",
      "description": "Collect bond interface status, slaves statuses and failures count",
      "config": "# Collect bond interface status, slaves statuses and failures count\n[[inputs.bond]]\n  # alias=\"bond\"\n  ## Sets 'proc' directory path\n  ## If not specified, then default is /proc\n  # host_proc = \"/proc\"\n\n  ## By default, telegraf gather stats for all bond interfaces\n  ## Setting interfaces will restrict the stats to the specified\n  ## bond interfaces.\n  # bond_interfaces = [\"bond0\"]\n\n"
    },
    {
      "type": "input",
      "name": "couchdb",
      "description": "Read CouchDB Stats from one or more servers",
      "config": "# Read CouchDB Stats from one or more servers\n[[inputs.couchdb]]\n  # alias=\"couchdb\"\n  ## Works with CouchDB stats endpoints out of the box\n  ## Multiple Hosts from which to read CouchDB stats:\n  hosts = [\"http://localhost:8086/_stats\"]\n\n  ## Use HTTP Basic Authentication.\n  # basic_username = \"telegraf\"\n  # basic_password = \"p@ssw0rd\"\n\n"
    },
    {
      "type": "input",
      "name": "kibana",
      "description": "Read status information from one or more Kibana servers",
      "config": "# Read status information from one or more Kibana servers\n[[inputs.kibana]]\n  # alias=\"kibana\"\n  ## specify a list of one or more Kibana servers\n  servers = [\"http://localhost:5601\"]\n\n  ## Timeout for HTTP requests\n  timeout = \"5s\"\n\n  ## HTTP Basic Auth credentials\n  # username = \"username\"\n  # password = \"pa$$word\"\n\n  ## Optional TLS Config\n  # tls_ca = \"/etc/telegraf/ca.pem\"\n  # tls_cert = \"/etc/telegraf/cert.pem\"\n  # tls_key = \"/etc/telegraf/key.pem\"\n  ## Use TLS but skip chain \u0026 host verification\n  # insecure_skip_verify = false\n\n"
    },
    {
      "type": "input",
      "name": "sensors",
      "description": "Monitor sensors, requires lm-sensors package",
      "config": "# Monitor sensors, requires lm-sensors package\n[[inputs.sensors]]\n  # alias=\"sensors\"\n  ## Remove numbers from field names.\n  ## If true, a field name like 'temp1_input' will be changed to 'temp_input'.\n  # remove_numbers = true\n\n  ## Timeout is the maximum amount of time that the sensors command can run.\n  # timeout = \"5s\"\n\n"
    },
    {
      "type": "input",
      "name": "synproxy",
      "description": "Get synproxy counter statistics from procfs",
      "config": "# Get synproxy counter statistics from procfs\n[[inputs.synproxy]]\n  # alias=\"synproxy\"\n"
    },
    {
      "type": "input",
      "name": "prometheus",
      "description": "Read metrics from one or many prometheus clients",
      "config": "# Read metrics from one or many prometheus clients\n[[inputs.prometheus]]\n  # alias=\"prometheus\"\n  ## An array of urls to scrape metrics from.\n  urls = [\"http://localhost:9100/metrics\"]\n\n  ## Metric version controls the mapping from Prometheus metrics into\n  ## Telegraf metrics.  When using the prometheus_client output, use the same\n  ## value in both plugins to ensure metrics are round-tripped without\n  ## modification.\n  ##\n  ##   example: metric_version = 1; deprecated in 1.13\n  ##            metric_version = 2; recommended version\n  # metric_version = 1\n\n  ## Url tag name (tag containing scrapped url. optional, default is \"url\")\n  # url_tag = \"scrapeUrl\"\n\n  ## An array of Kubernetes services to scrape metrics from.\n  # kubernetes_services = [\"http://my-service-dns.my-namespace:9100/metrics\"]\n\n  ## Kubernetes config file to create client from.\n  # kube_config = \"/path/to/kubernetes.config\"\n\n  ## Scrape Kubernetes pods for the following prometheus annotations:\n  ## - prometheus.io/scrape: Enable scraping for this pod\n  ## - prometheus.io/scheme: If the metrics endpoint is secured then you will need to\n  ##     set this to 'https' \u0026 most likely set the tls config.\n  ## - prometheus.io/path: If the metrics path is not /metrics, define it with this annotation.\n  ## - prometheus.io/port: If port is not 9102 use this annotation\n  # monitor_kubernetes_pods = true\n  ## Restricts Kubernetes monitoring to a single namespace\n  ##   ex: monitor_kubernetes_pods_namespace = \"default\"\n  # monitor_kubernetes_pods_namespace = \"\"\n\n  ## Use bearer token for authorization. ('bearer_token' takes priority)\n  # bearer_token = \"/path/to/bearer/token\"\n  ## OR\n  # bearer_token_string = \"abc_123\"\n\n  ## HTTP Basic Authentication username and password. ('bearer_token' and\n  ## 'bearer_token_string' take priority)\n  # username = \"\"\n  # password = \"\"\n\n  ## Specify timeout duration for slower prometheus clients (default is 3s)\n  # response_timeout = \"3s\"\n\n  ## Optional TLS Config\n  # tls_ca = /path/to/cafile\n  # tls_cert = /path/to/certfile\n  # tls_key = /path/to/keyfile\n  ## Use TLS but skip chain \u0026 host verification\n  # insecure_skip_verify = false\n\n"
    },
    {
      "type": "input",
      "name": "cisco_telemetry_mdt",
      "description": "Cisco model-driven telemetry (MDT) input plugin for IOS XR, IOS XE and NX-OS platforms",
      "config": "# Cisco model-driven telemetry (MDT) input plugin for IOS XR, IOS XE and NX-OS platforms\n[[inputs.cisco_telemetry_mdt]]\n  # alias=\"cisco_telemetry_mdt\"\n ## Telemetry transport can be \"tcp\" or \"grpc\".  TLS is only supported when\n ## using the grpc transport.\n transport = \"grpc\"\n\n ## Address and port to host telemetry listener\n service_address = \":57000\"\n\n ## Enable TLS; grpc transport only.\n # tls_cert = \"/etc/telegraf/cert.pem\"\n # tls_key = \"/etc/telegraf/key.pem\"\n\n ## Enable TLS client authentication and define allowed CA certificates; grpc\n ##  transport only.\n # tls_allowed_cacerts = [\"/etc/telegraf/clientca.pem\"]\n\n ## Define (for certain nested telemetry measurements with embedded tags) which fields are tags\n # embedded_tags = [\"Cisco-IOS-XR-qos-ma-oper:qos/interface-table/interface/input/service-policy-names/service-policy-instance/statistics/class-stats/class-name\"]\n\n ## Define aliases to map telemetry encoding paths to simple measurement names\n [inputs.cisco_telemetry_mdt.aliases]\n   ifstats = \"ietf-interfaces:interfaces-state/interface/statistics\"\n\n"
    },
    {
      "type": "input",
      "name": "fail2ban",
      "description": "Read metrics from fail2ban.",
      "config": "# Read metrics from fail2ban.\n[[inputs.fail2ban]]\n  # alias=\"fail2ban\"\n  ## Use sudo to run fail2ban-client\n  use_sudo = false\n\n"
    },
    {
      "type": "input",
      "name": "nsq_consumer",
      "description": "Read NSQ topic for metrics.",
      "config": "# Read NSQ topic for metrics.\n[[inputs.nsq_consumer]]\n  # alias=\"nsq_consumer\"\n  ## Server option still works but is deprecated, we just prepend it to the nsqd array.\n  # server = \"localhost:4150\"\n\n  ## An array representing the NSQD TCP HTTP Endpoints\n  nsqd = [\"localhost:4150\"]\n\n  ## An array representing the NSQLookupd HTTP Endpoints\n  nsqlookupd = [\"localhost:4161\"]\n  topic = \"telegraf\"\n  channel = \"consumer\"\n  max_in_flight = 100\n\n  ## Maximum messages to read from the broker that have not been written by an\n  ## output.  For best throughput set based on the number of metrics within\n  ## each message and the size of the output's metric_batch_size.\n  ##\n  ## For example, if each message from the queue contains 10 metrics and the\n  ## output metric_batch_size is 1000, setting this to 100 will ensure that a\n  ## full batch is collected and the write is triggered immediately without\n  ## waiting until the next flush_interval.\n  # max_undelivered_messages = 1000\n\n  ## Data format to consume.\n  ## Each data format has its own unique set of configuration options, read\n  ## more about them here:\n  ## https://github.com/influxdata/telegraf/blob/master/docs/DATA_FORMATS_INPUT.md\n  data_format = \"influx\"\n\n"
    },
    {
      "type": "input",
      "name": "opensmtpd",
      "description": "A plugin to collect stats from Opensmtpd - a validating, recursive, and caching DNS resolver ",
      "config": "# A plugin to collect stats from Opensmtpd - a validating, recursive, and caching DNS resolver \n[[inputs.opensmtpd]]\n  # alias=\"opensmtpd\"\n  ## If running as a restricted user you can prepend sudo for additional access:\n  #use_sudo = false\n\n  ## The default location of the smtpctl binary can be overridden with:\n  binary = \"/usr/sbin/smtpctl\"\n\n  ## The default timeout of 1000ms can be overriden with (in milliseconds):\n  timeout = 1000\n\n"
    },
    {
      "type": "input",
      "name": "postgresql",
      "description": "Read metrics from one or many postgresql servers",
      "config": "# Read metrics from one or many postgresql servers\n[[inputs.postgresql]]\n  # alias=\"postgresql\"\n  ## specify address via a url matching:\n  ##   postgres://[pqgotest[:password]]@localhost[/dbname]\\\n  ##       ?sslmode=[disable|verify-ca|verify-full]\n  ## or a simple string:\n  ##   host=localhost user=pqotest password=... sslmode=... dbname=app_production\n  ##\n  ## All connection parameters are optional.\n  ##\n  ## Without the dbname parameter, the driver will default to a database\n  ## with the same name as the user. This dbname is just for instantiating a\n  ## connection with the server and doesn't restrict the databases we are trying\n  ## to grab metrics for.\n  ##\n  address = \"host=localhost user=postgres sslmode=disable\"\n  ## A custom name for the database that will be used as the \"server\" tag in the\n  ## measurement output. If not specified, a default one generated from\n  ## the connection address is used.\n  # outputaddress = \"db01\"\n\n  ## connection configuration.\n  ## maxlifetime - specify the maximum lifetime of a connection.\n  ## default is forever (0s)\n  max_lifetime = \"0s\"\n\n  ## A  list of databases to explicitly ignore.  If not specified, metrics for all\n  ## databases are gathered.  Do NOT use with the 'databases' option.\n  # ignored_databases = [\"postgres\", \"template0\", \"template1\"]\n\n  ## A list of databases to pull metrics about. If not specified, metrics for all\n  ## databases are gathered.  Do NOT use with the 'ignored_databases' option.\n  # databases = [\"app_production\", \"testing\"]\n\n"
    },
    {
      "type": "input",
      "name": "apcupsd",
      "description": "Monitor APC UPSes connected to apcupsd",
      "config": "# Monitor APC UPSes connected to apcupsd\n[[inputs.apcupsd]]\n  # alias=\"apcupsd\"\n  # A list of running apcupsd server to connect to.\n  # If not provided will default to tcp://127.0.0.1:3551\n  servers = [\"tcp://127.0.0.1:3551\"]\n\n  ## Timeout for dialing server.\n  timeout = \"5s\"\n\n"
    },
    {
      "type": "input",
      "name": "phpfpm",
      "description": "Read metrics of phpfpm, via HTTP status page or socket",
      "config": "# Read metrics of phpfpm, via HTTP status page or socket\n[[inputs.phpfpm]]\n  # alias=\"phpfpm\"\n  ## An array of addresses to gather stats about. Specify an ip or hostname\n  ## with optional port and path\n  ##\n  ## Plugin can be configured in three modes (either can be used):\n  ##   - http: the URL must start with http:// or https://, ie:\n  ##       \"http://localhost/status\"\n  ##       \"http://192.168.130.1/status?full\"\n  ##\n  ##   - unixsocket: path to fpm socket, ie:\n  ##       \"/var/run/php5-fpm.sock\"\n  ##      or using a custom fpm status path:\n  ##       \"/var/run/php5-fpm.sock:fpm-custom-status-path\"\n  ##\n  ##   - fcgi: the URL must start with fcgi:// or cgi://, and port must be present, ie:\n  ##       \"fcgi://10.0.0.12:9000/status\"\n  ##       \"cgi://10.0.10.12:9001/status\"\n  ##\n  ## Example of multiple gathering from local socket and remote host\n  ## urls = [\"http://192.168.1.20/status\", \"/tmp/fpm.sock\"]\n  urls = [\"http://localhost/status\"]\n\n  ## Duration allowed to complete HTTP requests.\n  # timeout = \"5s\"\n\n  ## Optional TLS Config\n  # tls_ca = \"/etc/telegraf/ca.pem\"\n  # tls_cert = \"/etc/telegraf/cert.pem\"\n  # tls_key = \"/etc/telegraf/key.pem\"\n  ## Use TLS but skip chain \u0026 host verification\n  # insecure_skip_verify = false\n\n"
    },
    {
      "type": "input",
      "name": "smart",
      "description": "Read metrics from storage devices supporting S.M.A.R.T.",
      "config": "# Read metrics from storage devices supporting S.M.A.R.T.\n[[inputs.smart]]\n  # alias=\"smart\"\n  ## Optionally specify the path to the smartctl executable\n  # path = \"/usr/bin/smartctl\"\n\n  ## On most platforms smartctl requires root access.\n  ## Setting 'use_sudo' to true will make use of sudo to run smartctl.\n  ## Sudo must be configured to to allow the telegraf user to run smartctl\n  ## without a password.\n  # use_sudo = false\n\n  ## Skip checking disks in this power mode. Defaults to\n  ## \"standby\" to not wake up disks that have stoped rotating.\n  ## See --nocheck in the man pages for smartctl.\n  ## smartctl version 5.41 and 5.42 have faulty detection of\n  ## power mode and might require changing this value to\n  ## \"never\" depending on your disks.\n  # nocheck = \"standby\"\n\n  ## Gather all returned S.M.A.R.T. attribute metrics and the detailed\n  ## information from each drive into the 'smart_attribute' measurement.\n  # attributes = false\n\n  ## Optionally specify devices to exclude from reporting.\n  # excludes = [ \"/dev/pass6\" ]\n\n  ## Optionally specify devices and device type, if unset\n  ## a scan (smartctl --scan) for S.M.A.R.T. devices will\n  ## done and all found will be included except for the\n  ## excluded in excludes.\n  # devices = [ \"/dev/ada0 -d atacam\" ]\n\n  ## Timeout for the smartctl command to complete.\n  # timeout = \"30s\"\n\n"
    },
    {
      "type": "input",
      "name": "swap",
      "description": "Read metrics about swap memory usage",
      "config": "# Read metrics about swap memory usage\n[[inputs.swap]]\n  # alias=\"swap\"\n"
    },
    {
      "type": "input",
      "name": "zookeeper",
      "description": "Reads 'mntr' stats from one or many zookeeper servers",
      "config": "# Reads 'mntr' stats from one or many zookeeper servers\n[[inputs.zookeeper]]\n  # alias=\"zookeeper\"\n  ## An array of address to gather stats about. Specify an ip or hostname\n  ## with port. ie localhost:2181, 10.0.0.1:2181, etc.\n\n  ## If no servers are specified, then localhost is used as the host.\n  ## If no port is specified, 2181 is used\n  servers = [\":2181\"]\n\n  ## Timeout for metric collections from all servers.  Minimum timeout is \"1s\".\n  # timeout = \"5s\"\n\n  ## Optional TLS Config\n  # enable_tls = true\n  # tls_ca = \"/etc/telegraf/ca.pem\"\n  # tls_cert = \"/etc/telegraf/cert.pem\"\n  # tls_key = \"/etc/telegraf/key.pem\"\n  ## If false, skip chain \u0026 host verification\n  # insecure_skip_verify = true\n\n"
    },
    {
      "type": "input",
      "name": "disque",
      "description": "Read metrics from one or many disque servers",
      "config": "# Read metrics from one or many disque servers\n[[inputs.disque]]\n  # alias=\"disque\"\n  ## An array of URI to gather stats about. Specify an ip or hostname\n  ## with optional port and password.\n  ## ie disque://localhost, disque://10.10.3.33:18832, 10.0.0.1:10000, etc.\n  ## If no servers are specified, then localhost is used as the host.\n  servers = [\"localhost\"]\n\n"
    },
    {
      "type": "input",
      "name": "hddtemp",
      "description": "Monitor disks' temperatures using hddtemp",
      "config": "# Monitor disks' temperatures using hddtemp\n[[inputs.hddtemp]]\n  # alias=\"hddtemp\"\n  ## By default, telegraf gathers temps data from all disks detected by the\n  ## hddtemp.\n  ##\n  ## Only collect temps from the selected disks.\n  ##\n  ## A * as the device name will return the temperature values of all disks.\n  ##\n  # address = \"127.0.0.1:7634\"\n  # devices = [\"sda\", \"*\"]\n\n"
    },
    {
      "type": "input",
      "name": "interrupts",
      "description": "This plugin gathers interrupts data from /proc/interrupts and /proc/softirqs.",
      "config": "# This plugin gathers interrupts data from /proc/interrupts and /proc/softirqs.\n[[inputs.interrupts]]\n  # alias=\"interrupts\"\n  ## When set to true, cpu metrics are tagged with the cpu.  Otherwise cpu is\n  ## stored as a field.\n  ##\n  ## The default is false for backwards compatibility, and will be changed to\n  ## true in a future version.  It is recommended to set to true on new\n  ## deployments.\n  # cpu_as_tag = false\n\n  ## To filter which IRQs to collect, make use of tagpass / tagdrop, i.e.\n  # [inputs.interrupts.tagdrop]\n  #   irq = [ \"NET_RX\", \"TASKLET\" ]\n\n"
    },
    {
      "type": "input",
      "name": "jenkins",
      "description": "Read jobs and cluster metrics from Jenkins instances",
      "config": "# Read jobs and cluster metrics from Jenkins instances\n[[inputs.jenkins]]\n  # alias=\"jenkins\"\n  ## The Jenkins URL in the format \"schema://host:port\"\n  url = \"http://my-jenkins-instance:8080\"\n  # username = \"admin\"\n  # password = \"admin\"\n\n  ## Set response_timeout\n  response_timeout = \"5s\"\n\n  ## Optional TLS Config\n  # tls_ca = \"/etc/telegraf/ca.pem\"\n  # tls_cert = \"/etc/telegraf/cert.pem\"\n  # tls_key = \"/etc/telegraf/key.pem\"\n  ## Use SSL but skip chain \u0026 host verification\n  # insecure_skip_verify = false\n\n  ## Optional Max Job Build Age filter\n  ## Default 1 hour, ignore builds older than max_build_age\n  # max_build_age = \"1h\"\n\n  ## Optional Sub Job Depth filter\n  ## Jenkins can have unlimited layer of sub jobs\n  ## This config will limit the layers of pulling, default value 0 means\n  ## unlimited pulling until no more sub jobs\n  # max_subjob_depth = 0\n\n  ## Optional Sub Job Per Layer\n  ## In workflow-multibranch-plugin, each branch will be created as a sub job.\n  ## This config will limit to call only the lasted branches in each layer, \n  ## empty will use default value 10\n  # max_subjob_per_layer = 10\n\n  ## Jobs to exclude from gathering\n  # job_exclude = [ \"job1\", \"job2/subjob1/subjob2\", \"job3/*\"]\n\n  ## Nodes to exclude from gathering\n  # node_exclude = [ \"node1\", \"node2\" ]\n\n  ## Worker pool for jenkins plugin only\n  ## Empty this field will use default value 5\n  # max_connections = 5\n\n"
    },
    {
      "type": "input",
      "name": "nvidia_smi",
      "description": "Pulls statistics from nvidia GPUs attached to the host",
      "config": "# Pulls statistics from nvidia GPUs attached to the host\n[[inputs.nvidia_smi]]\n  # alias=\"nvidia_smi\"\n  ## Optional: path to nvidia-smi binary, defaults to $PATH via exec.LookPath\n  # bin_path = \"/usr/bin/nvidia-smi\"\n\n  ## Optional: timeout for GPU polling\n  # timeout = \"5s\"\n\n"
    },
    {
      "type": "input",
      "name": "ceph",
      "description": "Collects performance metrics from the MON and OSD nodes in a Ceph storage cluster.",
      "config": "# Collects performance metrics from the MON and OSD nodes in a Ceph storage cluster.\n[[inputs.ceph]]\n  # alias=\"ceph\"\n  ## This is the recommended interval to poll.  Too frequent and you will lose\n  ## data points due to timeouts during rebalancing and recovery\n  interval = '1m'\n\n  ## All configuration values are optional, defaults are shown below\n\n  ## location of ceph binary\n  ceph_binary = \"/usr/bin/ceph\"\n\n  ## directory in which to look for socket files\n  socket_dir = \"/var/run/ceph\"\n\n  ## prefix of MON and OSD socket files, used to determine socket type\n  mon_prefix = \"ceph-mon\"\n  osd_prefix = \"ceph-osd\"\n\n  ## suffix used to identify socket files\n  socket_suffix = \"asok\"\n\n  ## Ceph user to authenticate as\n  ceph_user = \"client.admin\"\n\n  ## Ceph configuration to use to locate the cluster\n  ceph_config = \"/etc/ceph/ceph.conf\"\n\n  ## Whether to gather statistics via the admin socket\n  gather_admin_socket_stats = true\n\n  ## Whether to gather statistics via ceph commands\n  gather_cluster_stats = false\n\n"
    },
    {
      "type": "input",
      "name": "dmcache",
      "description": "Provide a native collection for dmsetup based statistics for dm-cache",
      "config": "# Provide a native collection for dmsetup based statistics for dm-cache\n[[inputs.dmcache]]\n  # alias=\"dmcache\"\n  ## Whether to report per-device stats or not\n  per_device = true\n\n"
    },
    {
      "type": "input",
      "name": "net_response",
      "description": "Collect response time of a TCP or UDP connection",
      "config": "# Collect response time of a TCP or UDP connection\n[[inputs.net_response]]\n  # alias=\"net_response\"\n  ## Protocol, must be \"tcp\" or \"udp\"\n  ## NOTE: because the \"udp\" protocol does not respond to requests, it requires\n  ## a send/expect string pair (see below).\n  protocol = \"tcp\"\n  ## Server address (default localhost)\n  address = \"localhost:80\"\n\n  ## Set timeout\n  # timeout = \"1s\"\n\n  ## Set read timeout (only used if expecting a response)\n  # read_timeout = \"1s\"\n\n  ## The following options are required for UDP checks. For TCP, they are\n  ## optional. The plugin will send the given string to the server and then\n  ## expect to receive the given 'expect' string back.\n  ## string sent to the server\n  # send = \"ssh\"\n  ## expected string in answer\n  # expect = \"ssh\"\n\n  ## Uncomment to remove deprecated fields\n  # fielddrop = [\"result_type\", \"string_found\"]\n\n"
    },
    {
      "type": "input",
      "name": "puppetagent",
      "description": "Reads last_run_summary.yaml file and converts to measurments",
      "config": "# Reads last_run_summary.yaml file and converts to measurments\n[[inputs.puppetagent]]\n  # alias=\"puppetagent\"\n  ## Location of puppet last run summary file\n  location = \"/var/lib/puppet/state/last_run_summary.yaml\"\n\n"
    },
    {
      "type": "input",
      "name": "zfs",
      "description": "Read metrics of ZFS from arcstats, zfetchstats, vdev_cache_stats, and pools",
      "config": "# Read metrics of ZFS from arcstats, zfetchstats, vdev_cache_stats, and pools\n[[inputs.zfs]]\n  # alias=\"zfs\"\n  ## ZFS kstat path. Ignored on FreeBSD\n  ## If not specified, then default is:\n  # kstatPath = \"/proc/spl/kstat/zfs\"\n\n  ## By default, telegraf gather all zfs stats\n  ## If not specified, then default is:\n  # kstatMetrics = [\"arcstats\", \"zfetchstats\", \"vdev_cache_stats\"]\n  ## For Linux, the default is:\n  # kstatMetrics = [\"abdstats\", \"arcstats\", \"dnodestats\", \"dbufcachestats\",\n  #   \"dmu_tx\", \"fm\", \"vdev_mirror_stats\", \"zfetchstats\", \"zil\"]\n  ## By default, don't gather zpool stats\n  # poolMetrics = false\n\n"
    },
    {
      "type": "input",
      "name": "aerospike",
      "description": "Read stats from aerospike server(s)",
      "config": "# Read stats from aerospike server(s)\n[[inputs.aerospike]]\n  # alias=\"aerospike\"\n  ## Aerospike servers to connect to (with port)\n  ## This plugin will query all namespaces the aerospike\n  ## server has configured and get stats for them.\n  servers = [\"localhost:3000\"]\n\n  # username = \"telegraf\"\n  # password = \"pa$$word\"\n\n  ## Optional TLS Config\n  # enable_tls = false\n  # tls_ca = \"/etc/telegraf/ca.pem\"\n  # tls_cert = \"/etc/telegraf/cert.pem\"\n  # tls_key = \"/etc/telegraf/key.pem\"\n  ## If false, skip chain \u0026 host verification\n  # insecure_skip_verify = true\n \n"
    },
    {
      "type": "input",
      "name": "exec",
      "description": "Read metrics from one or more commands that can output to stdout",
      "config": "# Read metrics from one or more commands that can output to stdout\n[[inputs.exec]]\n  # alias=\"exec\"\n  ## Commands array\n  commands = [\n    \"/tmp/test.sh\",\n    \"/usr/bin/mycollector --foo=bar\",\n    \"/tmp/collect_*.sh\"\n  ]\n\n  ## Timeout for each command to complete.\n  timeout = \"5s\"\n\n  ## measurement name suffix (for separating different commands)\n  name_suffix = \"_mycollector\"\n\n  ## Data format to consume.\n  ## Each data format has its own unique set of configuration options, read\n  ## more about them here:\n  ## https://github.com/influxdata/telegraf/blob/master/docs/DATA_FORMATS_INPUT.md\n  data_format = \"influx\"\n\n"
    },
    {
      "type": "input",
      "name": "influxdb",
      "description": "Read InfluxDB-formatted JSON metrics from one or more HTTP endpoints",
      "config": "# Read InfluxDB-formatted JSON metrics from one or more HTTP endpoints\n[[inputs.influxdb]]\n  # alias=\"influxdb\"\n  ## Works with InfluxDB debug endpoints out of the box,\n  ## but other services can use this format too.\n  ## See the influxdb plugin's README for more details.\n\n  ## Multiple URLs from which to read InfluxDB-formatted JSON\n  ## Default is \"http://localhost:8086/debug/vars\".\n  urls = [\n    \"http://localhost:8086/debug/vars\"\n  ]\n\n  ## Username and password to send using HTTP Basic Authentication.\n  # username = \"\"\n  # password = \"\"\n\n  ## Optional TLS Config\n  # tls_ca = \"/etc/telegraf/ca.pem\"\n  # tls_cert = \"/etc/telegraf/cert.pem\"\n  # tls_key = \"/etc/telegraf/key.pem\"\n  ## Use TLS but skip chain \u0026 host verification\n  # insecure_skip_verify = false\n\n  ## http request \u0026 header timeout\n  timeout = \"5s\"\n\n"
    },
    {
      "type": "input",
      "name": "nginx",
      "description": "Read Nginx's basic status information (ngx_http_stub_status_module)",
      "config": "# Read Nginx's basic status information (ngx_http_stub_status_module)\n[[inputs.nginx]]\n  # alias=\"nginx\"\n  # An array of Nginx stub_status URI to gather stats.\n  urls = [\"http://localhost/server_status\"]\n\n  ## Optional TLS Config\n  tls_ca = \"/etc/telegraf/ca.pem\"\n  tls_cert = \"/etc/telegraf/cert.cer\"\n  tls_key = \"/etc/telegraf/key.key\"\n  ## Use TLS but skip chain \u0026 host verification\n  insecure_skip_verify = false\n\n  # HTTP response timeout (default: 5s)\n  response_timeout = \"5s\"\n\n"
    },
    {
      "type": "input",
      "name": "ping",
      "description": "Ping given url(s) and return statistics",
      "config": "# Ping given url(s) and return statistics\n[[inputs.ping]]\n  # alias=\"ping\"\n  ## Hosts to send ping packets to.\n  urls = [\"example.org\"]\n\n  ## Method used for sending pings, can be either \"exec\" or \"native\".  When set\n  ## to \"exec\" the systems ping command will be executed.  When set to \"native\"\n  ## the plugin will send pings directly.\n  ##\n  ## While the default is \"exec\" for backwards compatibility, new deployments\n  ## are encouraged to use the \"native\" method for improved compatibility and\n  ## performance.\n  # method = \"exec\"\n\n  ## Number of ping packets to send per interval.  Corresponds to the \"-c\"\n  ## option of the ping command.\n  # count = 1\n\n  ## Time to wait between sending ping packets in seconds.  Operates like the\n  ## \"-i\" option of the ping command.\n  # ping_interval = 1.0\n\n  ## If set, the time to wait for a ping response in seconds.  Operates like\n  ## the \"-W\" option of the ping command.\n  # timeout = 1.0\n\n  ## If set, the total ping deadline, in seconds.  Operates like the -w option\n  ## of the ping command.\n  # deadline = 10\n\n  ## Interface or source address to send ping from.  Operates like the -I or -S\n  ## option of the ping command.\n  # interface = \"\"\n\n  ## Specify the ping executable binary.\n  # binary = \"ping\"\n\n  ## Arguments for ping command. When arguments is not empty, the command from\n  ## the binary option will be used and other options (ping_interval, timeout,\n  ## etc) will be ignored.\n  # arguments = [\"-c\", \"3\"]\n\n  ## Use only IPv6 addresses when resolving a hostname.\n  # ipv6 = false\n\n"
    },
    {
      "type": "input",
      "name": "stackdriver",
      "description": "Gather timeseries from Google Cloud Platform v3 monitoring API",
      "config": "# Gather timeseries from Google Cloud Platform v3 monitoring API\n[[inputs.stackdriver]]\n  # alias=\"stackdriver\"\n  ## GCP Project\n  project = \"erudite-bloom-151019\"\n\n  ## Include timeseries that start with the given metric type.\n  metric_type_prefix_include = [\n    \"compute.googleapis.com/\",\n  ]\n\n  ## Exclude timeseries that start with the given metric type.\n  # metric_type_prefix_exclude = []\n\n  ## Many metrics are updated once per minute; it is recommended to override\n  ## the agent level interval with a value of 1m or greater.\n  interval = \"1m\"\n\n  ## Maximum number of API calls to make per second.  The quota for accounts\n  ## varies, it can be viewed on the API dashboard:\n  ##   https://cloud.google.com/monitoring/quotas#quotas_and_limits\n  # rate_limit = 14\n\n  ## The delay and window options control the number of points selected on\n  ## each gather.  When set, metrics are gathered between:\n  ##   start: now() - delay - window\n  ##   end:   now() - delay\n  #\n  ## Collection delay; if set too low metrics may not yet be available.\n  # delay = \"5m\"\n  #\n  ## If unset, the window will start at 1m and be updated dynamically to span\n  ## the time between calls (approximately the length of the plugin interval).\n  # window = \"1m\"\n\n  ## TTL for cached list of metric types.  This is the maximum amount of time\n  ## it may take to discover new metrics.\n  # cache_ttl = \"1h\"\n\n  ## If true, raw bucket counts are collected for distribution value types.\n  ## For a more lightweight collection, you may wish to disable and use\n  ## distribution_aggregation_aligners instead.\n  # gather_raw_distribution_buckets = true\n\n  ## Aggregate functions to be used for metrics whose value type is\n  ## distribution.  These aggregate values are recorded in in addition to raw\n  ## bucket counts; if they are enabled.\n  ##\n  ## For a list of aligner strings see:\n  ##   https://cloud.google.com/monitoring/api/ref_v3/rpc/google.monitoring.v3#aligner\n  # distribution_aggregation_aligners = [\n  # \t\"ALIGN_PERCENTILE_99\",\n  # \t\"ALIGN_PERCENTILE_95\",\n  # \t\"ALIGN_PERCENTILE_50\",\n  # ]\n\n  ## Filters can be added to reduce the number of time series matched.  All\n  ## functions are supported: starts_with, ends_with, has_substring, and\n  ## one_of.  Only the '=' operator is supported.\n  ##\n  ## The logical operators when combining filters are defined statically using\n  ## the following values:\n  ##   filter ::= \u003cresource_labels\u003e {AND \u003cmetric_labels\u003e}\n  ##   resource_labels ::= \u003cresource_labels\u003e {OR \u003cresource_label\u003e}\n  ##   metric_labels ::= \u003cmetric_labels\u003e {OR \u003cmetric_label\u003e}\n  ##\n  ## For more details, see https://cloud.google.com/monitoring/api/v3/filters\n  #\n  ## Resource labels refine the time series selection with the following expression:\n  ##   resource.labels.\u003ckey\u003e = \u003cvalue\u003e\n  # [[inputs.stackdriver.filter.resource_labels]]\n  #   key = \"instance_name\"\n  #   value = 'starts_with(\"localhost\")'\n  #\n  ## Metric labels refine the time series selection with the following expression:\n  ##   metric.labels.\u003ckey\u003e = \u003cvalue\u003e\n  #  [[inputs.stackdriver.filter.metric_labels]]\n  #  \t key = \"device_name\"\n  #  \t value = 'one_of(\"sda\", \"sdb\")'\n\n"
    },
    {
      "type": "input",
      "name": "syslog",
      "description": "Accepts syslog messages following RFC5424 format with transports as per RFC5426, RFC5425, or RFC6587",
      "config": "# Accepts syslog messages following RFC5424 format with transports as per RFC5426, RFC5425, or RFC6587\n[[inputs.syslog]]\n  # alias=\"syslog\"\n  ## Specify an ip or hostname with port - eg., tcp://localhost:6514, tcp://10.0.0.1:6514\n  ## Protocol, address and port to host the syslog receiver.\n  ## If no host is specified, then localhost is used.\n  ## If no port is specified, 6514 is used (RFC5425#section-4.1).\n  server = \"tcp://:6514\"\n\n  ## TLS Config\n  # tls_allowed_cacerts = [\"/etc/telegraf/ca.pem\"]\n  # tls_cert = \"/etc/telegraf/cert.pem\"\n  # tls_key = \"/etc/telegraf/key.pem\"\n\n  ## Period between keep alive probes.\n  ## 0 disables keep alive probes.\n  ## Defaults to the OS configuration.\n  ## Only applies to stream sockets (e.g. TCP).\n  # keep_alive_period = \"5m\"\n\n  ## Maximum number of concurrent connections (default = 0).\n  ## 0 means unlimited.\n  ## Only applies to stream sockets (e.g. TCP).\n  # max_connections = 1024\n\n  ## Read timeout is the maximum time allowed for reading a single message (default = 5s).\n  ## 0 means unlimited.\n  # read_timeout = \"5s\"\n\n  ## The framing technique with which it is expected that messages are transported (default = \"octet-counting\").\n  ## Whether the messages come using the octect-counting (RFC5425#section-4.3.1, RFC6587#section-3.4.1),\n  ## or the non-transparent framing technique (RFC6587#section-3.4.2).\n  ## Must be one of \"octet-counting\", \"non-transparent\".\n  # framing = \"octet-counting\"\n\n  ## The trailer to be expected in case of non-trasparent framing (default = \"LF\").\n  ## Must be one of \"LF\", or \"NUL\".\n  # trailer = \"LF\"\n\n  ## Whether to parse in best effort mode or not (default = false).\n  ## By default best effort parsing is off.\n  # best_effort = false\n\n  ## Character to prepend to SD-PARAMs (default = \"_\").\n  ## A syslog message can contain multiple parameters and multiple identifiers within structured data section.\n  ## Eg., [id1 name1=\"val1\" name2=\"val2\"][id2 name1=\"val1\" nameA=\"valA\"]\n  ## For each combination a field is created.\n  ## Its name is created concatenating identifier, sdparam_separator, and parameter name.\n  # sdparam_separator = \"_\"\n\n"
    },
    {
      "type": "input",
      "name": "activemq",
      "description": "Gather ActiveMQ metrics",
      "config": "# Gather ActiveMQ metrics\n[[inputs.activemq]]\n  # alias=\"activemq\"\n  ## ActiveMQ WebConsole URL\n  url = \"http://127.0.0.1:8161\"\n\n  ## Required ActiveMQ Endpoint\n  ##   deprecated in 1.11; use the url option\n  # server = \"127.0.0.1\"\n  # port = 8161\n\n  ## Credentials for basic HTTP authentication\n  # username = \"admin\"\n  # password = \"admin\"\n\n  ## Required ActiveMQ webadmin root path\n  # webadmin = \"admin\"\n\n  ## Maximum time to receive response.\n  # response_timeout = \"5s\"\n\n  ## Optional TLS Config\n  # tls_ca = \"/etc/telegraf/ca.pem\"\n  # tls_cert = \"/etc/telegraf/cert.pem\"\n  # tls_key = \"/etc/telegraf/key.pem\"\n  ## Use TLS but skip chain \u0026 host verification\n  # insecure_skip_verify = false\n  \n"
    },
    {
      "type": "input",
      "name": "bind",
      "description": "Read BIND nameserver XML statistics",
      "config": "# Read BIND nameserver XML statistics\n[[inputs.bind]]\n  # alias=\"bind\"\n  ## An array of BIND XML statistics URI to gather stats.\n  ## Default is \"http://localhost:8053/xml/v3\".\n  # urls = [\"http://localhost:8053/xml/v3\"]\n  # gather_memory_contexts = false\n  # gather_views = false\n\n"
    },
    {
      "type": "input",
      "name": "httpjson",
      "description": "Read flattened metrics from one or more JSON HTTP endpoints",
      "config": "# Read flattened metrics from one or more JSON HTTP endpoints\n[[inputs.httpjson]]\n  # alias=\"httpjson\"\n  ## NOTE This plugin only reads numerical measurements, strings and booleans\n  ## will be ignored.\n\n  ## Name for the service being polled.  Will be appended to the name of the\n  ## measurement e.g. httpjson_webserver_stats\n  ##\n  ## Deprecated (1.3.0): Use name_override, name_suffix, name_prefix instead.\n  name = \"webserver_stats\"\n\n  ## URL of each server in the service's cluster\n  servers = [\n    \"http://localhost:9999/stats/\",\n    \"http://localhost:9998/stats/\",\n  ]\n  ## Set response_timeout (default 5 seconds)\n  response_timeout = \"5s\"\n\n  ## HTTP method to use: GET or POST (case-sensitive)\n  method = \"GET\"\n\n  ## List of tag names to extract from top-level of JSON server response\n  # tag_keys = [\n  #   \"my_tag_1\",\n  #   \"my_tag_2\"\n  # ]\n\n  ## Optional TLS Config\n  # tls_ca = \"/etc/telegraf/ca.pem\"\n  # tls_cert = \"/etc/telegraf/cert.pem\"\n  # tls_key = \"/etc/telegraf/key.pem\"\n  ## Use TLS but skip chain \u0026 host verification\n  # insecure_skip_verify = false\n\n  ## HTTP parameters (all values must be strings).  For \"GET\" requests, data\n  ## will be included in the query.  For \"POST\" requests, data will be included\n  ## in the request body as \"x-www-form-urlencoded\".\n  # [inputs.httpjson.parameters]\n  #   event_type = \"cpu_spike\"\n  #   threshold = \"0.75\"\n\n  ## HTTP Headers (all values must be strings)\n  # [inputs.httpjson.headers]\n  #   X-Auth-Token = \"my-xauth-token\"\n  #   apiVersion = \"v1\"\n\n"
    },
    {
      "type": "input",
      "name": "kapacitor",
      "description": "Read Kapacitor-formatted JSON metrics from one or more HTTP endpoints",
      "config": "# Read Kapacitor-formatted JSON metrics from one or more HTTP endpoints\n[[inputs.kapacitor]]\n  # alias=\"kapacitor\"\n  ## Multiple URLs from which to read Kapacitor-formatted JSON\n  ## Default is \"http://localhost:9092/kapacitor/v1/debug/vars\".\n  urls = [\n    \"http://localhost:9092/kapacitor/v1/debug/vars\"\n  ]\n\n  ## Time limit for http requests\n  timeout = \"5s\"\n\n  ## Optional TLS Config\n  # tls_ca = \"/etc/telegraf/ca.pem\"\n  # tls_cert = \"/etc/telegraf/cert.pem\"\n  # tls_key = \"/etc/telegraf/key.pem\"\n  ## Use TLS but skip chain \u0026 host verification\n  # insecure_skip_verify = false\n\n"
    },
    {
      "type": "input",
      "name": "multifile",
      "description": "Aggregates the contents of multiple files into a single point",
      "config": "# Aggregates the contents of multiple files into a single point\n[[inputs.multifile]]\n  # alias=\"multifile\"\n  ## Base directory where telegraf will look for files.\n  ## Omit this option to use absolute paths.\n  base_dir = \"/sys/bus/i2c/devices/1-0076/iio:device0\"\n\n  ## If true, Telegraf discard all data when a single file can't be read.\n  ## Else, Telegraf omits the field generated from this file.\n  # fail_early = true\n\n  ## Files to parse each interval.\n  [[inputs.multifile.file]]\n    file = \"in_pressure_input\"\n    dest = \"pressure\"\n    conversion = \"float\"\n  [[inputs.multifile.file]]\n    file = \"in_temp_input\"\n    dest = \"temperature\"\n    conversion = \"float(3)\"\n  [[inputs.multifile.file]]\n    file = \"in_humidityrelative_input\"\n    dest = \"humidityrelative\"\n    conversion = \"float(3)\"\n\n"
    },
    {
      "type": "input",
      "name": "raindrops",
      "description": "Read raindrops stats (raindrops - real-time stats for preforking Rack servers)",
      "config": "# Read raindrops stats (raindrops - real-time stats for preforking Rack servers)\n[[inputs.raindrops]]\n  # alias=\"raindrops\"\n  ## An array of raindrops middleware URI to gather stats.\n  urls = [\"http://localhost:8080/_raindrops\"]\n\n"
    },
    {
      "type": "input",
      "name": "riak",
      "description": "Read metrics one or many Riak servers",
      "config": "# Read metrics one or many Riak servers\n[[inputs.riak]]\n  # alias=\"riak\"\n  # Specify a list of one or more riak http servers\n  servers = [\"http://localhost:8098\"]\n\n"
    },
    {
      "type": "input",
      "name": "socket_listener",
      "description": "Generic socket listener capable of handling multiple socket types.",
      "config": "# Generic socket listener capable of handling multiple socket types.\n[[inputs.socket_listener]]\n  # alias=\"socket_listener\"\n  ## URL to listen on\n  # service_address = \"tcp://:8094\"\n  # service_address = \"tcp://127.0.0.1:http\"\n  # service_address = \"tcp4://:8094\"\n  # service_address = \"tcp6://:8094\"\n  # service_address = \"tcp6://[2001:db8::1]:8094\"\n  # service_address = \"udp://:8094\"\n  # service_address = \"udp4://:8094\"\n  # service_address = \"udp6://:8094\"\n  # service_address = \"unix:///tmp/telegraf.sock\"\n  # service_address = \"unixgram:///tmp/telegraf.sock\"\n\n  ## Change the file mode bits on unix sockets.  These permissions may not be\n  ## respected by some platforms, to safely restrict write permissions it is best\n  ## to place the socket into a directory that has previously been created\n  ## with the desired permissions.\n  ##   ex: socket_mode = \"777\"\n  # socket_mode = \"\"\n\n  ## Maximum number of concurrent connections.\n  ## Only applies to stream sockets (e.g. TCP).\n  ## 0 (default) is unlimited.\n  # max_connections = 1024\n\n  ## Read timeout.\n  ## Only applies to stream sockets (e.g. TCP).\n  ## 0 (default) is unlimited.\n  # read_timeout = \"30s\"\n\n  ## Optional TLS configuration.\n  ## Only applies to stream sockets (e.g. TCP).\n  # tls_cert = \"/etc/telegraf/cert.pem\"\n  # tls_key  = \"/etc/telegraf/key.pem\"\n  ## Enables client authentication if set.\n  # tls_allowed_cacerts = [\"/etc/telegraf/clientca.pem\"]\n\n  ## Maximum socket buffer size (in bytes when no unit specified).\n  ## For stream sockets, once the buffer fills up, the sender will start backing up.\n  ## For datagram sockets, once the buffer fills up, metrics will start dropping.\n  ## Defaults to the OS default.\n  # read_buffer_size = \"64KiB\"\n\n  ## Period between keep alive probes.\n  ## Only applies to TCP sockets.\n  ## 0 disables keep alive probes.\n  ## Defaults to the OS configuration.\n  # keep_alive_period = \"5m\"\n\n  ## Data format to consume.\n  ## Each data format has its own unique set of configuration options, read\n  ## more about them here:\n  ## https://github.com/influxdata/telegraf/blob/master/docs/DATA_FORMATS_INPUT.md\n  # data_format = \"influx\"\n\n  ## Content encoding for message payloads, can be set to \"gzip\" to or\n  ## \"identity\" to apply no encoding.\n  # content_encoding = \"identity\"\n\n"
    },
    {
      "type": "input",
      "name": "cisco_telemetry_gnmi",
      "description": "Cisco GNMI telemetry input plugin based on GNMI telemetry data produced in IOS XR",
      "config": "# Cisco GNMI telemetry input plugin based on GNMI telemetry data produced in IOS XR\n[[inputs.cisco_telemetry_gnmi]]\n  # alias=\"cisco_telemetry_gnmi\"\n ## Address and port of the GNMI GRPC server\n addresses = [\"10.49.234.114:57777\"]\n\n ## define credentials\n username = \"cisco\"\n password = \"cisco\"\n\n ## GNMI encoding requested (one of: \"proto\", \"json\", \"json_ietf\")\n # encoding = \"proto\"\n\n ## redial in case of failures after\n redial = \"10s\"\n\n ## enable client-side TLS and define CA to authenticate the device\n # enable_tls = true\n # tls_ca = \"/etc/telegraf/ca.pem\"\n # insecure_skip_verify = true\n\n ## define client-side TLS certificate \u0026 key to authenticate to the device\n # tls_cert = \"/etc/telegraf/cert.pem\"\n # tls_key = \"/etc/telegraf/key.pem\"\n\n ## GNMI subscription prefix (optional, can usually be left empty)\n ## See: https://github.com/openconfig/reference/blob/master/rpc/gnmi/gnmi-specification.md#222-paths\n # origin = \"\"\n # prefix = \"\"\n # target = \"\"\n\n ## Define additional aliases to map telemetry encoding paths to simple measurement names\n #[inputs.cisco_telemetry_gnmi.aliases]\n #  ifcounters = \"openconfig:/interfaces/interface/state/counters\"\n\n [[inputs.cisco_telemetry_gnmi.subscription]]\n  ## Name of the measurement that will be emitted\n  name = \"ifcounters\"\n\n  ## Origin and path of the subscription\n  ## See: https://github.com/openconfig/reference/blob/master/rpc/gnmi/gnmi-specification.md#222-paths\n  ##\n  ## origin usually refers to a (YANG) data model implemented by the device\n  ## and path to a specific substructe inside it that should be subscribed to (similar to an XPath)\n  ## YANG models can be found e.g. here: https://github.com/YangModels/yang/tree/master/vendor/cisco/xr\n  origin = \"openconfig-interfaces\"\n  path = \"/interfaces/interface/state/counters\"\n\n  # Subscription mode (one of: \"target_defined\", \"sample\", \"on_change\") and interval\n  subscription_mode = \"sample\"\n  sample_interval = \"10s\"\n\n  ## Suppress redundant transmissions when measured values are unchanged\n  # suppress_redundant = false\n\n  ## If suppression is enabled, send updates at least every X seconds anyway\n  # heartbeat_interval = \"60s\"\n\n"
    },
    {
      "type": "input",
      "name": "haproxy",
      "description": "Read metrics of haproxy, via socket or csv stats page",
      "config": "# Read metrics of haproxy, via socket or csv stats page\n[[inputs.haproxy]]\n  # alias=\"haproxy\"\n  ## An array of address to gather stats about. Specify an ip on hostname\n  ## with optional port. ie localhost, 10.10.3.33:1936, etc.\n  ## Make sure you specify the complete path to the stats endpoint\n  ## including the protocol, ie http://10.10.3.33:1936/haproxy?stats\n\n  ## If no servers are specified, then default to 127.0.0.1:1936/haproxy?stats\n  servers = [\"http://myhaproxy.com:1936/haproxy?stats\"]\n\n  ## Credentials for basic HTTP authentication\n  # username = \"admin\"\n  # password = \"admin\"\n\n  ## You can also use local socket with standard wildcard globbing.\n  ## Server address not starting with 'http' will be treated as a possible\n  ## socket, so both examples below are valid.\n  # servers = [\"socket:/run/haproxy/admin.sock\", \"/run/haproxy/*.sock\"]\n\n  ## By default, some of the fields are renamed from what haproxy calls them.\n  ## Setting this option to true results in the plugin keeping the original\n  ## field names.\n  # keep_field_names = false\n\n  ## Optional TLS Config\n  # tls_ca = \"/etc/telegraf/ca.pem\"\n  # tls_cert = \"/etc/telegraf/cert.pem\"\n  # tls_key = \"/etc/telegraf/key.pem\"\n  ## Use TLS but skip chain \u0026 host verification\n  # insecure_skip_verify = false\n\n"
    },
    {
      "type": "input",
      "name": "kubernetes",
      "description": "Read metrics from the kubernetes kubelet api",
      "config": "# Read metrics from the kubernetes kubelet api\n[[inputs.kubernetes]]\n  # alias=\"kubernetes\"\n  ## URL for the kubelet\n  url = \"http://127.0.0.1:10255\"\n\n  ## Use bearer token for authorization. ('bearer_token' takes priority)\n  ## If both of these are empty, we'll use the default serviceaccount:\n  ## at: /run/secrets/kubernetes.io/serviceaccount/token\n  # bearer_token = \"/path/to/bearer/token\"\n  ## OR\n  # bearer_token_string = \"abc_123\"\n\n  ## Set response_timeout (default 5 seconds)\n  # response_timeout = \"5s\"\n\n  ## Optional TLS Config\n  # tls_ca = /path/to/cafile\n  # tls_cert = /path/to/certfile\n  # tls_key = /path/to/keyfile\n  ## Use TLS but skip chain \u0026 host verification\n  # insecure_skip_verify = false\n\n"
    },
    {
      "type": "input",
      "name": "logstash",
      "description": "Read metrics exposed by Logstash",
      "config": "# Read metrics exposed by Logstash\n[[inputs.logstash]]\n  # alias=\"logstash\"\n  ## The URL of the exposed Logstash API endpoint.\n  url = \"http://127.0.0.1:9600\"\n\n  ## Use Logstash 5 single pipeline API, set to true when monitoring\n  ## Logstash 5.\n  # single_pipeline = false\n\n  ## Enable optional collection components.  Can contain\n  ## \"pipelines\", \"process\", and \"jvm\".\n  # collect = [\"pipelines\", \"process\", \"jvm\"]\n\n  ## Timeout for HTTP requests.\n  # timeout = \"5s\"\n\n  ## Optional HTTP Basic Auth credentials.\n  # username = \"username\"\n  # password = \"pa$$word\"\n\n  ## Optional TLS Config.\n  # tls_ca = \"/etc/telegraf/ca.pem\"\n  # tls_cert = \"/etc/telegraf/cert.pem\"\n  # tls_key = \"/etc/telegraf/key.pem\"\n\n  ## Use TLS but skip chain \u0026 host verification.\n  # insecure_skip_verify = false\n\n  ## Optional HTTP headers.\n  # [inputs.logstash.headers]\n  #   \"X-Special-Header\" = \"Special-Value\"\n\n"
    },
    {
      "type": "input",
      "name": "nats_consumer",
      "description": "Read metrics from NATS subject(s)",
      "config": "# Read metrics from NATS subject(s)\n[[inputs.nats_consumer]]\n  # alias=\"nats_consumer\"\n  ## urls of NATS servers\n  servers = [\"nats://localhost:4222\"]\n\n  ## subject(s) to consume\n  subjects = [\"telegraf\"]\n\n  ## name a queue group\n  queue_group = \"telegraf_consumers\"\n\n  ## Optional credentials\n  # username = \"\"\n  # password = \"\"\n\n  ## Use Transport Layer Security\n  # secure = false\n\n  ## Optional TLS Config\n  # tls_ca = \"/etc/telegraf/ca.pem\"\n  # tls_cert = \"/etc/telegraf/cert.pem\"\n  # tls_key = \"/etc/telegraf/key.pem\"\n  ## Use TLS but skip chain \u0026 host verification\n  # insecure_skip_verify = false\n\n  ## Sets the limits for pending msgs and bytes for each subscription\n  ## These shouldn't need to be adjusted except in very high throughput scenarios\n  # pending_message_limit = 65536\n  # pending_bytes_limit = 67108864\n\n  ## Maximum messages to read from the broker that have not been written by an\n  ## output.  For best throughput set based on the number of metrics within\n  ## each message and the size of the output's metric_batch_size.\n  ##\n  ## For example, if each message from the queue contains 10 metrics and the\n  ## output metric_batch_size is 1000, setting this to 100 will ensure that a\n  ## full batch is collected and the write is triggered immediately without\n  ## waiting until the next flush_interval.\n  # max_undelivered_messages = 1000\n\n  ## Data format to consume.\n  ## Each data format has its own unique set of configuration options, read\n  ## more about them here:\n  ## https://github.com/influxdata/telegraf/blob/master/docs/DATA_FORMATS_INPUT.md\n  data_format = \"influx\"\n\n"
    },
    {
      "type": "input",
      "name": "trig",
      "description": "Inserts sine and cosine waves for demonstration purposes",
      "config": "# Inserts sine and cosine waves for demonstration purposes\n[[inputs.trig]]\n  # alias=\"trig\"\n  ## Set the amplitude\n  amplitude = 10.0\n\n"
    },
    {
      "type": "input",
      "name": "mqtt_consumer",
      "description": "Read metrics from MQTT topic(s)",
      "config": "# Read metrics from MQTT topic(s)\n[[inputs.mqtt_consumer]]\n  # alias=\"mqtt_consumer\"\n  ## MQTT broker URLs to be used. The format should be scheme://host:port,\n  ## schema can be tcp, ssl, or ws.\n  servers = [\"tcp://127.0.0.1:1883\"]\n\n  ## Topics that will be subscribed to.\n  topics = [\n    \"telegraf/host01/cpu\",\n    \"telegraf/+/mem\",\n    \"sensors/#\",\n  ]\n\n  ## The message topic will be stored in a tag specified by this value.  If set\n  ## to the empty string no topic tag will be created.\n  # topic_tag = \"topic\"\n\n  ## QoS policy for messages\n  ##   0 = at most once\n  ##   1 = at least once\n  ##   2 = exactly once\n  ##\n  ## When using a QoS of 1 or 2, you should enable persistent_session to allow\n  ## resuming unacknowledged messages.\n  # qos = 0\n\n  ## Connection timeout for initial connection in seconds\n  # connection_timeout = \"30s\"\n\n  ## Maximum messages to read from the broker that have not been written by an\n  ## output.  For best throughput set based on the number of metrics within\n  ## each message and the size of the output's metric_batch_size.\n  ##\n  ## For example, if each message from the queue contains 10 metrics and the\n  ## output metric_batch_size is 1000, setting this to 100 will ensure that a\n  ## full batch is collected and the write is triggered immediately without\n  ## waiting until the next flush_interval.\n  # max_undelivered_messages = 1000\n\n  ## Persistent session disables clearing of the client session on connection.\n  ## In order for this option to work you must also set client_id to identify\n  ## the client.  To receive messages that arrived while the client is offline,\n  ## also set the qos option to 1 or 2 and don't forget to also set the QoS when\n  ## publishing.\n  # persistent_session = false\n\n  ## If unset, a random client ID will be generated.\n  # client_id = \"\"\n\n  ## Username and password to connect MQTT server.\n  # username = \"telegraf\"\n  # password = \"metricsmetricsmetricsmetrics\"\n\n  ## Optional TLS Config\n  # tls_ca = \"/etc/telegraf/ca.pem\"\n  # tls_cert = \"/etc/telegraf/cert.pem\"\n  # tls_key = \"/etc/telegraf/key.pem\"\n  ## Use TLS but skip chain \u0026 host verification\n  # insecure_skip_verify = false\n\n  ## Data format to consume.\n  ## Each data format has its own unique set of configuration options, read\n  ## more about them here:\n  ## https://github.com/influxdata/telegraf/blob/master/docs/DATA_FORMATS_INPUT.md\n  data_format = \"influx\"\n\n"
    },
    {
      "type": "input",
      "name": "snmp",
      "description": "Retrieves SNMP values from remote agents",
      "config": "# Retrieves SNMP values from remote agents\n[[inputs.snmp]]\n  # alias=\"snmp\"\n  agents = [ \"127.0.0.1:161\" ]\n  ## Timeout for each SNMP query.\n  timeout = \"5s\"\n  ## Number of retries to attempt within timeout.\n  retries = 3\n  ## SNMP version, values can be 1, 2, or 3\n  version = 2\n\n  ## SNMP community string.\n  community = \"public\"\n\n  ## The GETBULK max-repetitions parameter\n  max_repetitions = 10\n\n  ## SNMPv3 auth parameters\n  #sec_name = \"myuser\"\n  #auth_protocol = \"md5\"      # Values: \"MD5\", \"SHA\", \"\"\n  #auth_password = \"pass\"\n  #sec_level = \"authNoPriv\"   # Values: \"noAuthNoPriv\", \"authNoPriv\", \"authPriv\"\n  #context_name = \"\"\n  #priv_protocol = \"\"         # Values: \"DES\", \"AES\", \"\"\n  #priv_password = \"\"\n\n  ## measurement name\n  name = \"system\"\n  [[inputs.snmp.field]]\n    name = \"hostname\"\n    oid = \".1.0.0.1.1\"\n  [[inputs.snmp.field]]\n    name = \"uptime\"\n    oid = \".1.0.0.1.2\"\n  [[inputs.snmp.field]]\n    name = \"load\"\n    oid = \".1.0.0.1.3\"\n  [[inputs.snmp.field]]\n    oid = \"HOST-RESOURCES-MIB::hrMemorySize\"\n\n  [[inputs.snmp.table]]\n    ## measurement name\n    name = \"remote_servers\"\n    inherit_tags = [ \"hostname\" ]\n    [[inputs.snmp.table.field]]\n      name = \"server\"\n      oid = \".1.0.0.0.1.0\"\n      is_tag = true\n    [[inputs.snmp.table.field]]\n      name = \"connections\"\n      oid = \".1.0.0.0.1.1\"\n    [[inputs.snmp.table.field]]\n      name = \"latency\"\n      oid = \".1.0.0.0.1.2\"\n\n  [[inputs.snmp.table]]\n    ## auto populate table's fields using the MIB\n    oid = \"HOST-RESOURCES-MIB::hrNetworkTable\"\n\n"
    },
    {
      "type": "input",
      "name": "teamspeak",
      "description": "Reads metrics from a Teamspeak 3 Server via ServerQuery",
      "config": "# Reads metrics from a Teamspeak 3 Server via ServerQuery\n[[inputs.teamspeak]]\n  # alias=\"teamspeak\"\n  ## Server address for Teamspeak 3 ServerQuery\n  # server = \"127.0.0.1:10011\"\n  ## Username for ServerQuery\n  username = \"serverqueryuser\"\n  ## Password for ServerQuery\n  password = \"secret\"\n  ## Array of virtual servers\n  # virtual_servers = [1]\n\n"
    },
    {
      "type": "input",
      "name": "azure_storage_queue",
      "description": "Gather Azure Storage Queue metrics",
      "config": "# Gather Azure Storage Queue metrics\n[[inputs.azure_storage_queue]]\n  # alias=\"azure_storage_queue\"\n  ## Required Azure Storage Account name\n  account_name = \"mystorageaccount\"\n\n  ## Required Azure Storage Account access key\n  account_key = \"storageaccountaccesskey\"\n\n  ## Set to false to disable peeking age of oldest message (executes faster)\n  # peek_oldest_message_age = true\n  \n"
    },
    {
      "type": "input",
      "name": "cpu",
      "description": "Read metrics about cpu usage",
      "config": "# Read metrics about cpu usage\n[[inputs.cpu]]\n  # alias=\"cpu\"\n  ## Whether to report per-cpu stats or not\n  percpu = true\n  ## Whether to report total system cpu stats or not\n  totalcpu = true\n  ## If true, collect raw CPU time metrics.\n  collect_cpu_time = false\n  ## If true, compute and report the sum of all non-idle CPU states.\n  report_active = false\n\n"
    },
    {
      "type": "input",
      "name": "dcos",
      "description": "Input plugin for DC/OS metrics",
      "config": "# Input plugin for DC/OS metrics\n[[inputs.dcos]]\n  # alias=\"dcos\"\n  ## The DC/OS cluster URL.\n  cluster_url = \"https://dcos-ee-master-1\"\n\n  ## The ID of the service account.\n  service_account_id = \"telegraf\"\n  ## The private key file for the service account.\n  service_account_private_key = \"/etc/telegraf/telegraf-sa-key.pem\"\n\n  ## Path containing login token.  If set, will read on every gather.\n  # token_file = \"/home/dcos/.dcos/token\"\n\n  ## In all filter options if both include and exclude are empty all items\n  ## will be collected.  Arrays may contain glob patterns.\n  ##\n  ## Node IDs to collect metrics from.  If a node is excluded, no metrics will\n  ## be collected for its containers or apps.\n  # node_include = []\n  # node_exclude = []\n  ## Container IDs to collect container metrics from.\n  # container_include = []\n  # container_exclude = []\n  ## Container IDs to collect app metrics from.\n  # app_include = []\n  # app_exclude = []\n\n  ## Maximum concurrent connections to the cluster.\n  # max_connections = 10\n  ## Maximum time to receive a response from cluster.\n  # response_timeout = \"20s\"\n\n  ## Optional TLS Config\n  # tls_ca = \"/etc/telegraf/ca.pem\"\n  # tls_cert = \"/etc/telegraf/cert.pem\"\n  # tls_key = \"/etc/telegraf/key.pem\"\n  ## If false, skip chain \u0026 host verification\n  # insecure_skip_verify = true\n\n  ## Recommended filtering to reduce series cardinality.\n  # [inputs.dcos.tagdrop]\n  #   path = [\"/var/lib/mesos/slave/slaves/*\"]\n\n"
    },
    {
      "type": "input",
      "name": "http_response",
      "description": "HTTP/HTTPS request given an address a method and a timeout",
      "config": "# HTTP/HTTPS request given an address a method and a timeout\n[[inputs.http_response]]\n  # alias=\"http_response\"\n  ## Deprecated in 1.12, use 'urls'\n  ## Server address (default http://localhost)\n  # address = \"http://localhost\"\n\n  ## List of urls to query.\n  # urls = [\"http://localhost\"]\n\n  ## Set http_proxy (telegraf uses the system wide proxy settings if it's is not set)\n  # http_proxy = \"http://localhost:8888\"\n\n  ## Set response_timeout (default 5 seconds)\n  # response_timeout = \"5s\"\n\n  ## HTTP Request Method\n  # method = \"GET\"\n\n  ## Whether to follow redirects from the server (defaults to false)\n  # follow_redirects = false\n\n  ## Optional HTTP Request Body\n  # body = '''\n  # {'fake':'data'}\n  # '''\n\n  ## Optional substring or regex match in body of the response\n  # response_string_match = \"\\\"service_status\\\": \\\"up\\\"\"\n  # response_string_match = \"ok\"\n  # response_string_match = \"\\\".*_status\\\".?:.?\\\"up\\\"\"\n\n  ## Optional TLS Config\n  # tls_ca = \"/etc/telegraf/ca.pem\"\n  # tls_cert = \"/etc/telegraf/cert.pem\"\n  # tls_key = \"/etc/telegraf/key.pem\"\n  ## Use TLS but skip chain \u0026 host verification\n  # insecure_skip_verify = false\n\n  ## HTTP Request Headers (all values must be strings)\n  # [inputs.http_response.headers]\n  #   Host = \"github.com\"\n\n  ## Interface to use when dialing an address\n  # interface = \"eth0\"\n\n"
    },
    {
      "type": "input",
      "name": "mongodb",
      "description": "Read metrics from one or many MongoDB servers",
      "config": "# Read metrics from one or many MongoDB servers\n[[inputs.mongodb]]\n  # alias=\"mongodb\"\n  ## An array of URLs of the form:\n  ##   \"mongodb://\" [user \":\" pass \"@\"] host [ \":\" port]\n  ## For example:\n  ##   mongodb://user:auth_key@10.10.3.30:27017,\n  ##   mongodb://10.10.3.33:18832,\n  servers = [\"mongodb://127.0.0.1:27017\"]\n\n  ## When true, collect per database stats\n  # gather_perdb_stats = false\n\n  ## When true, collect per collection stats\n  # gather_col_stats = false\n\n  ## List of db where collections stats are collected\n  ## If empty, all db are concerned\n  # col_stats_dbs = [\"local\"]\n\n  ## Optional TLS Config\n  # tls_ca = \"/etc/telegraf/ca.pem\"\n  # tls_cert = \"/etc/telegraf/cert.pem\"\n  # tls_key = \"/etc/telegraf/key.pem\"\n  ## Use TLS but skip chain \u0026 host verification\n  # insecure_skip_verify = false\n\n"
    },
    {
      "type": "input",
      "name": "unbound",
      "description": "A plugin to collect stats from the Unbound DNS resolver",
      "config": "# A plugin to collect stats from the Unbound DNS resolver\n[[inputs.unbound]]\n  # alias=\"unbound\"\n  ## Address of server to connect to, read from unbound conf default, optionally ':port'\n  ## Will lookup IP if given a hostname\n  server = \"127.0.0.1:8953\"\n\n  ## If running as a restricted user you can prepend sudo for additional access:\n  # use_sudo = false\n\n  ## The default location of the unbound-control binary can be overridden with:\n  # binary = \"/usr/sbin/unbound-control\"\n\n  ## The default timeout of 1s can be overriden with:\n  # timeout = \"1s\"\n\n  ## When set to true, thread metrics are tagged with the thread id.\n  ##\n  ## The default is false for backwards compatibility, and will be changed to\n  ## true in a future version.  It is recommended to set to true on new\n  ## deployments.\n  thread_as_tag = false\n\n"
    },
    {
      "type": "input",
      "name": "jolokia2_agent",
      "description": "Read JMX metrics from a Jolokia REST agent endpoint",
      "config": "# Read JMX metrics from a Jolokia REST agent endpoint\n[[inputs.jolokia2_agent]]\n  # alias=\"jolokia2_agent\"\n  # default_tag_prefix      = \"\"\n  # default_field_prefix    = \"\"\n  # default_field_separator = \".\"\n\n  # Add agents URLs to query\n  urls = [\"http://localhost:8080/jolokia\"]\n  # username = \"\"\n  # password = \"\"\n  # response_timeout = \"5s\"\n\n  ## Optional TLS config\n  # tls_ca   = \"/var/private/ca.pem\"\n  # tls_cert = \"/var/private/client.pem\"\n  # tls_key  = \"/var/private/client-key.pem\"\n  # insecure_skip_verify = false\n\n  ## Add metrics to read\n  [[inputs.jolokia2_agent.metric]]\n    name  = \"java_runtime\"\n    mbean = \"java.lang:type=Runtime\"\n    paths = [\"Uptime\"]\n\n"
    },
    {
      "type": "input",
      "name": "jolokia2_proxy",
      "description": "Read JMX metrics from a Jolokia REST proxy endpoint",
      "config": "# Read JMX metrics from a Jolokia REST proxy endpoint\n[[inputs.jolokia2_proxy]]\n  # alias=\"jolokia2_proxy\"\n  # default_tag_prefix      = \"\"\n  # default_field_prefix    = \"\"\n  # default_field_separator = \".\"\n\n  ## Proxy agent\n  url = \"http://localhost:8080/jolokia\"\n  # username = \"\"\n  # password = \"\"\n  # response_timeout = \"5s\"\n\n  ## Optional TLS config\n  # tls_ca   = \"/var/private/ca.pem\"\n  # tls_cert = \"/var/private/client.pem\"\n  # tls_key  = \"/var/private/client-key.pem\"\n  # insecure_skip_verify = false\n\n  ## Add proxy targets to query\n  # default_target_username = \"\"\n  # default_target_password = \"\"\n  [[inputs.jolokia2_proxy.target]]\n    url = \"service:jmx:rmi:///jndi/rmi://targethost:9999/jmxrmi\"\n    # username = \"\"\n    # password = \"\"\n\n  ## Add metrics to read\n  [[inputs.jolokia2_proxy.metric]]\n    name  = \"java_runtime\"\n    mbean = \"java.lang:type=Runtime\"\n    paths = [\"Uptime\"]\n\n"
    },
    {
      "type": "input",
      "name": "mailchimp",
      "description": "Gathers metrics from the /3.0/reports MailChimp API",
      "config": "# Gathers metrics from the /3.0/reports MailChimp API\n[[inputs.mailchimp]]\n  # alias=\"mailchimp\"\n  ## MailChimp API key\n  ## get from https://admin.mailchimp.com/account/api/\n  api_key = \"\" # required\n  ## Reports for campaigns sent more than days_old ago will not be collected.\n  ## 0 means collect all.\n  days_old = 0\n  ## Campaign ID to get, if empty gets all campaigns, this option overrides days_old\n  # campaign_id = \"\"\n\n"
    },
    {
      "type": "input",
      "name": "minecraft",
      "description": "Collects scores from a Minecraft server's scoreboard using the RCON protocol",
      "config": "# Collects scores from a Minecraft server's scoreboard using the RCON protocol\n[[inputs.minecraft]]\n  # alias=\"minecraft\"\n  ## Address of the Minecraft server.\n  # server = \"localhost\"\n\n  ## Server RCON Port.\n  # port = \"25575\"\n\n  ## Server RCON Password.\n  password = \"\"\n\n  ## Uncomment to remove deprecated metric components.\n  # tagdrop = [\"server\"]\n\n"
    },
    {
      "type": "input",
      "name": "solr",
      "description": "Read stats from one or more Solr servers or cores",
      "config": "# Read stats from one or more Solr servers or cores\n[[inputs.solr]]\n  # alias=\"solr\"\n  ## specify a list of one or more Solr servers\n  servers = [\"http://localhost:8983\"]\n\n  ## specify a list of one or more Solr cores (default - all)\n  # cores = [\"main\"]\n\n  ## Optional HTTP Basic Auth Credentials\n  # username = \"username\"\n  # password = \"pa$$word\"\n\n"
    },
    {
      "type": "input",
      "name": "nginx_plus_api",
      "description": "Read Nginx Plus Api documentation",
      "config": "# Read Nginx Plus Api documentation\n[[inputs.nginx_plus_api]]\n  # alias=\"nginx_plus_api\"\n  ## An array of API URI to gather stats.\n  urls = [\"http://localhost/api\"]\n\n  # Nginx API version, default: 3\n  # api_version = 3\n\n  # HTTP response timeout (default: 5s)\n  response_timeout = \"5s\"\n\n  ## Optional TLS Config\n  # tls_ca = \"/etc/telegraf/ca.pem\"\n  # tls_cert = \"/etc/telegraf/cert.pem\"\n  # tls_key = \"/etc/telegraf/key.pem\"\n  ## Use TLS but skip chain \u0026 host verification\n  # insecure_skip_verify = false\n\n"
    },
    {
      "type": "input",
      "name": "nstat",
      "description": "Collect kernel snmp counters and network interface statistics",
      "config": "# Collect kernel snmp counters and network interface statistics\n[[inputs.nstat]]\n  # alias=\"nstat\"\n  ## file paths for proc files. If empty default paths will be used:\n  ##    /proc/net/netstat, /proc/net/snmp, /proc/net/snmp6\n  ## These can also be overridden with env variables, see README.\n  proc_net_netstat = \"/proc/net/netstat\"\n  proc_net_snmp = \"/proc/net/snmp\"\n  proc_net_snmp6 = \"/proc/net/snmp6\"\n  ## dump metrics with 0 values too\n  dump_zeros       = true\n\n"
    },
    {
      "type": "input",
      "name": "openweathermap",
      "description": "Read current weather and forecasts data from openweathermap.org",
      "config": "# Read current weather and forecasts data from openweathermap.org\n[[inputs.openweathermap]]\n  # alias=\"openweathermap\"\n  ## OpenWeatherMap API key.\n  app_id = \"xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx\"\n\n  ## City ID's to collect weather data from.\n  city_id = [\"5391959\"]\n\n  ## Language of the description field. Can be one of \"ar\", \"bg\",\n  ## \"ca\", \"cz\", \"de\", \"el\", \"en\", \"fa\", \"fi\", \"fr\", \"gl\", \"hr\", \"hu\",\n  ## \"it\", \"ja\", \"kr\", \"la\", \"lt\", \"mk\", \"nl\", \"pl\", \"pt\", \"ro\", \"ru\",\n  ## \"se\", \"sk\", \"sl\", \"es\", \"tr\", \"ua\", \"vi\", \"zh_cn\", \"zh_tw\"\n  # lang = \"en\"\n\n  ## APIs to fetch; can contain \"weather\" or \"forecast\".\n  fetch = [\"weather\", \"forecast\"]\n\n  ## OpenWeatherMap base URL\n  # base_url = \"https://api.openweathermap.org/\"\n\n  ## Timeout for HTTP response.\n  # response_timeout = \"5s\"\n\n  ## Preferred unit system for temperature and wind speed. Can be one of\n  ## \"metric\", \"imperial\", or \"standard\".\n  # units = \"metric\"\n\n  ## Query interval; OpenWeatherMap updates their weather data every 10\n  ## minutes.\n  interval = \"10m\"\n\n"
    },
    {
      "type": "input",
      "name": "amqp_consumer",
      "description": "AMQP consumer plugin",
      "config": "# AMQP consumer plugin\n[[inputs.amqp_consumer]]\n  # alias=\"amqp_consumer\"\n  ## Broker to consume from.\n  ##   deprecated in 1.7; use the brokers option\n  # url = \"amqp://localhost:5672/influxdb\"\n\n  ## Brokers to consume from.  If multiple brokers are specified a random broker\n  ## will be selected anytime a connection is established.  This can be\n  ## helpful for load balancing when not using a dedicated load balancer.\n  brokers = [\"amqp://localhost:5672/influxdb\"]\n\n  ## Authentication credentials for the PLAIN auth_method.\n  # username = \"\"\n  # password = \"\"\n\n  ## Name of the exchange to declare.  If unset, no exchange will be declared.\n  exchange = \"telegraf\"\n\n  ## Exchange type; common types are \"direct\", \"fanout\", \"topic\", \"header\", \"x-consistent-hash\".\n  # exchange_type = \"topic\"\n\n  ## If true, exchange will be passively declared.\n  # exchange_passive = false\n\n  ## Exchange durability can be either \"transient\" or \"durable\".\n  # exchange_durability = \"durable\"\n\n  ## Additional exchange arguments.\n  # exchange_arguments = { }\n  # exchange_arguments = {\"hash_propery\" = \"timestamp\"}\n\n  ## AMQP queue name.\n  queue = \"telegraf\"\n\n  ## AMQP queue durability can be \"transient\" or \"durable\".\n  queue_durability = \"durable\"\n\n  ## If true, queue will be passively declared.\n  # queue_passive = false\n\n  ## A binding between the exchange and queue using this binding key is\n  ## created.  If unset, no binding is created.\n  binding_key = \"#\"\n\n  ## Maximum number of messages server should give to the worker.\n  # prefetch_count = 50\n\n  ## Maximum messages to read from the broker that have not been written by an\n  ## output.  For best throughput set based on the number of metrics within\n  ## each message and the size of the output's metric_batch_size.\n  ##\n  ## For example, if each message from the queue contains 10 metrics and the\n  ## output metric_batch_size is 1000, setting this to 100 will ensure that a\n  ## full batch is collected and the write is triggered immediately without\n  ## waiting until the next flush_interval.\n  # max_undelivered_messages = 1000\n\n  ## Auth method. PLAIN and EXTERNAL are supported\n  ## Using EXTERNAL requires enabling the rabbitmq_auth_mechanism_ssl plugin as\n  ## described here: https://www.rabbitmq.com/plugins.html\n  # auth_method = \"PLAIN\"\n\n  ## Optional TLS Config\n  # tls_ca = \"/etc/telegraf/ca.pem\"\n  # tls_cert = \"/etc/telegraf/cert.pem\"\n  # tls_key = \"/etc/telegraf/key.pem\"\n  ## Use TLS but skip chain \u0026 host verification\n  # insecure_skip_verify = false\n\n  ## Content encoding for message payloads, can be set to \"gzip\" to or\n  ## \"identity\" to apply no encoding.\n  # content_encoding = \"identity\"\n\n  ## Data format to consume.\n  ## Each data format has its own unique set of configuration options, read\n  ## more about them here:\n  ## https://github.com/influxdata/telegraf/blob/master/docs/DATA_FORMATS_INPUT.md\n  data_format = \"influx\"\n\n"
    },
    {
      "type": "input",
      "name": "ethtool",
      "description": "Returns ethtool statistics for given interfaces",
      "config": "# Returns ethtool statistics for given interfaces\n[[inputs.ethtool]]\n  # alias=\"ethtool\"\n  ## List of interfaces to pull metrics for\n  # interface_include = [\"eth0\"]\n\n  ## List of interfaces to ignore when pulling metrics.\n  # interface_exclude = [\"eth1\"]\n\n"
    },
    {
      "type": "input",
      "name": "filestat",
      "description": "Read stats about given file(s)",
      "config": "# Read stats about given file(s)\n[[inputs.filestat]]\n  # alias=\"filestat\"\n  ## Files to gather stats about.\n  ## These accept standard unix glob matching rules, but with the addition of\n  ## ** as a \"super asterisk\". ie:\n  ##   \"/var/log/**.log\"  -\u003e recursively find all .log files in /var/log\n  ##   \"/var/log/*/*.log\" -\u003e find all .log files with a parent dir in /var/log\n  ##   \"/var/log/apache.log\" -\u003e just tail the apache log file\n  ##\n  ## See https://github.com/gobwas/glob for more examples\n  ##\n  files = [\"/var/log/**.log\"]\n\n  ## If true, read the entire file and calculate an md5 checksum.\n  md5 = false\n\n"
    },
    {
      "type": "input",
      "name": "kernel_vmstat",
      "description": "Get kernel statistics from /proc/vmstat",
      "config": "# Get kernel statistics from /proc/vmstat\n[[inputs.kernel_vmstat]]\n  # alias=\"kernel_vmstat\"\n"
    },
    {
      "type": "input",
      "name": "nginx_plus",
      "description": "Read Nginx Plus' full status information (ngx_http_status_module)",
      "config": "# Read Nginx Plus' full status information (ngx_http_status_module)\n[[inputs.nginx_plus]]\n  # alias=\"nginx_plus\"\n  ## An array of ngx_http_status_module or status URI to gather stats.\n  urls = [\"http://localhost/status\"]\n\n  # HTTP response timeout (default: 5s)\n  response_timeout = \"5s\"\n\n  ## Optional TLS Config\n  # tls_ca = \"/etc/telegraf/ca.pem\"\n  # tls_cert = \"/etc/telegraf/cert.pem\"\n  # tls_key = \"/etc/telegraf/key.pem\"\n  ## Use TLS but skip chain \u0026 host verification\n  # insecure_skip_verify = false\n\n"
    },
    {
      "type": "input",
      "name": "powerdns_recursor",
      "description": "Read metrics from one or many PowerDNS Recursor servers",
      "config": "# Read metrics from one or many PowerDNS Recursor servers\n[[inputs.powerdns_recursor]]\n  # alias=\"powerdns_recursor\"\n  ## Path to the Recursor control socket.\n  unix_sockets = [\"/var/run/pdns_recursor.controlsocket\"]\n\n  ## Directory to create receive socket.  This default is likely not writable,\n  ## please reference the full plugin documentation for a recommended setup.\n  # socket_dir = \"/var/run/\"\n  ## Socket permissions for the receive socket.\n  # socket_mode = \"0666\"\n\n"
    },
    {
      "type": "input",
      "name": "sqlserver",
      "description": "Read metrics from Microsoft SQL Server",
      "config": "# Read metrics from Microsoft SQL Server\n[[inputs.sqlserver]]\n  # alias=\"sqlserver\"\n  ## Specify instances to monitor with a list of connection strings.\n  ## All connection parameters are optional.\n  ## By default, the host is localhost, listening on default port, TCP 1433.\n  ##   for Windows, the user is the currently running AD user (SSO).\n  ##   See https://github.com/denisenkom/go-mssqldb for detailed connection\n  ##   parameters, in particular, tls connections can be created like so:\n  ##   \"encrypt=true;certificate=\u003ccert\u003e;hostNameInCertificate=\u003cSqlServer host fqdn\u003e\"\n  # servers = [\n  #  \"Server=192.168.1.10;Port=1433;User Id=\u003cuser\u003e;Password=\u003cpw\u003e;app name=telegraf;log=1;\",\n  # ]\n\n  ## Optional parameter, setting this to 2 will use a new version\n  ## of the collection queries that break compatibility with the original\n  ## dashboards.\n  query_version = 2\n\n  ## If you are using AzureDB, setting this to true will gather resource utilization metrics\n  # azuredb = false\n\n  ## If you would like to exclude some of the metrics queries, list them here\n  ## Possible choices:\n  ## - PerformanceCounters\n  ## - WaitStatsCategorized\n  ## - DatabaseIO\n  ## - DatabaseProperties\n  ## - CPUHistory\n  ## - DatabaseSize\n  ## - DatabaseStats\n  ## - MemoryClerk\n  ## - VolumeSpace\n  ## - PerformanceMetrics\n  ## - Schedulers\n  ## - AzureDBResourceStats\n  ## - AzureDBResourceGovernance\n  ## - SqlRequests\n  ## - ServerProperties\n  exclude_query = [ 'Schedulers' ]\n\n"
    },
    {
      "type": "input",
      "name": "disk",
      "description": "Read metrics about disk usage by mount point",
      "config": "# Read metrics about disk usage by mount point\n[[inputs.disk]]\n  # alias=\"disk\"\n  ## By default stats will be gathered for all mount points.\n  ## Set mount_points will restrict the stats to only the specified mount points.\n  # mount_points = [\"/\"]\n\n  ## Ignore mount points by filesystem type.\n  ignore_fs = [\"tmpfs\", \"devtmpfs\", \"devfs\", \"iso9660\", \"overlay\", \"aufs\", \"squashfs\"]\n\n"
    },
    {
      "type": "input",
      "name": "fibaro",
      "description": "Read devices value(s) from a Fibaro controller",
      "config": "# Read devices value(s) from a Fibaro controller\n[[inputs.fibaro]]\n  # alias=\"fibaro\"\n  ## Required Fibaro controller address/hostname.\n  ## Note: at the time of writing this plugin, Fibaro only implemented http - no https available\n  url = \"http://\u003ccontroller\u003e:80\"\n\n  ## Required credentials to access the API (http://\u003ccontroller/api/\u003ccomponent\u003e)\n  username = \"\u003cusername\u003e\"\n  password = \"\u003cpassword\u003e\"\n\n  ## Amount of time allowed to complete the HTTP request\n  # timeout = \"5s\"\n\n"
    },
    {
      "type": "input",
      "name": "graylog",
      "description": "Read flattened metrics from one or more GrayLog HTTP endpoints",
      "config": "# Read flattened metrics from one or more GrayLog HTTP endpoints\n[[inputs.graylog]]\n  # alias=\"graylog\"\n  ## API endpoint, currently supported API:\n  ##\n  ##   - multiple  (Ex http://\u003chost\u003e:12900/system/metrics/multiple)\n  ##   - namespace (Ex http://\u003chost\u003e:12900/system/metrics/namespace/{namespace})\n  ##\n  ## For namespace endpoint, the metrics array will be ignored for that call.\n  ## Endpoint can contain namespace and multiple type calls.\n  ##\n  ## Please check http://[graylog-server-ip]:12900/api-browser for full list\n  ## of endpoints\n  servers = [\n    \"http://[graylog-server-ip]:12900/system/metrics/multiple\",\n  ]\n\n  ## Metrics list\n  ## List of metrics can be found on Graylog webservice documentation.\n  ## Or by hitting the the web service api at:\n  ##   http://[graylog-host]:12900/system/metrics\n  metrics = [\n    \"jvm.cl.loaded\",\n    \"jvm.memory.pools.Metaspace.committed\"\n  ]\n\n  ## Username and password\n  username = \"\"\n  password = \"\"\n\n  ## Optional TLS Config\n  # tls_ca = \"/etc/telegraf/ca.pem\"\n  # tls_cert = \"/etc/telegraf/cert.pem\"\n  # tls_key = \"/etc/telegraf/key.pem\"\n  ## Use TLS but skip chain \u0026 host verification\n  # insecure_skip_verify = false\n\n"
    },
    {
      "type": "input",
      "name": "lustre2",
      "description": "Read metrics from local Lustre service on OST, MDS",
      "config": "# Read metrics from local Lustre service on OST, MDS\n[[inputs.lustre2]]\n  # alias=\"lustre2\"\n  ## An array of /proc globs to search for Lustre stats\n  ## If not specified, the default will work on Lustre 2.5.x\n  ##\n  # ost_procfiles = [\n  #   \"/proc/fs/lustre/obdfilter/*/stats\",\n  #   \"/proc/fs/lustre/osd-ldiskfs/*/stats\",\n  #   \"/proc/fs/lustre/obdfilter/*/job_stats\",\n  # ]\n  # mds_procfiles = [\n  #   \"/proc/fs/lustre/mdt/*/md_stats\",\n  #   \"/proc/fs/lustre/mdt/*/job_stats\",\n  # ]\n\n"
    },
    {
      "type": "input",
      "name": "nginx_upstream_check",
      "description": "Read nginx_upstream_check module status information (https://github.com/yaoweibin/nginx_upstream_check_module)",
      "config": "# Read nginx_upstream_check module status information (https://github.com/yaoweibin/nginx_upstream_check_module)\n[[inputs.nginx_upstream_check]]\n  # alias=\"nginx_upstream_check\"\n  ## An URL where Nginx Upstream check module is enabled\n  ## It should be set to return a JSON formatted response\n  url = \"http://127.0.0.1/status?format=json\"\n\n  ## HTTP method\n  # method = \"GET\"\n\n  ## Optional HTTP headers\n  # headers = {\"X-Special-Header\" = \"Special-Value\"}\n\n  ## Override HTTP \"Host\" header\n  # host_header = \"check.example.com\"\n\n  ## Timeout for HTTP requests\n  timeout = \"5s\"\n\n  ## Optional HTTP Basic Auth credentials\n  # username = \"username\"\n  # password = \"pa$$word\"\n\n  ## Optional TLS Config\n  # tls_ca = \"/etc/telegraf/ca.pem\"\n  # tls_cert = \"/etc/telegraf/cert.pem\"\n  # tls_key = \"/etc/telegraf/key.pem\"\n  ## Use TLS but skip chain \u0026 host verification\n  # insecure_skip_verify = false\n\n"
    },
    {
      "type": "input",
      "name": "apache",
      "description": "Read Apache status information (mod_status)",
      "config": "# Read Apache status information (mod_status)\n[[inputs.apache]]\n  # alias=\"apache\"\n  ## An array of URLs to gather from, must be directed at the machine\n  ## readable version of the mod_status page including the auto query string.\n  ## Default is \"http://localhost/server-status?auto\".\n  urls = [\"http://localhost/server-status?auto\"]\n\n  ## Credentials for basic HTTP authentication.\n  # username = \"myuser\"\n  # password = \"mypassword\"\n\n  ## Maximum time to receive response.\n  # response_timeout = \"5s\"\n\n  ## Optional TLS Config\n  # tls_ca = \"/etc/telegraf/ca.pem\"\n  # tls_cert = \"/etc/telegraf/cert.pem\"\n  # tls_key = \"/etc/telegraf/key.pem\"\n  ## Use TLS but skip chain \u0026 host verification\n  # insecure_skip_verify = false\n\n"
    },
    {
      "type": "input",
      "name": "passenger",
      "description": "Read metrics of passenger using passenger-status",
      "config": "# Read metrics of passenger using passenger-status\n[[inputs.passenger]]\n  # alias=\"passenger\"\n  ## Path of passenger-status.\n  ##\n  ## Plugin gather metric via parsing XML output of passenger-status\n  ## More information about the tool:\n  ##   https://www.phusionpassenger.com/library/admin/apache/overall_status_report.html\n  ##\n  ## If no path is specified, then the plugin simply execute passenger-status\n  ## hopefully it can be found in your PATH\n  command = \"passenger-status -v --show=xml\"\n\n"
    },
    {
      "type": "input",
      "name": "suricata",
      "description": "Suricata stats plugin",
      "config": "# Suricata stats plugin\n[[inputs.suricata]]\n  # alias=\"suricata\"\n  ## Data sink for Suricata stats log\n  # This is expected to be a filename of a\n  # unix socket to be created for listening.\n  source = \"/var/run/suricata-stats.sock\"\n\n  # Delimiter for flattening field keys, e.g. subitem \"alert\" of \"detect\"\n  # becomes \"detect_alert\" when delimiter is \"_\".\n  delimiter = \"_\"\n\n"
    },
    {
      "type": "input",
      "name": "zipkin",
      "description": "This plugin implements the Zipkin http server to gather trace and timing data needed to troubleshoot latency problems in microservice architectures.",
      "config": "# This plugin implements the Zipkin http server to gather trace and timing data needed to troubleshoot latency problems in microservice architectures.\n[[inputs.zipkin]]\n  # alias=\"zipkin\"\n  # path = \"/api/v1/spans\" # URL path for span data\n  # port = 9411            # Port on which Telegraf listens\n\n"
    },
    {
      "type": "input",
      "name": "marklogic",
      "description": "Retrives information on a specific host in a MarkLogic Cluster",
      "config": "# Retrives information on a specific host in a MarkLogic Cluster\n[[inputs.marklogic]]\n  # alias=\"marklogic\"\n  ## Base URL of the MarkLogic HTTP Server.\n  url = \"http://localhost:8002\"\n\n  ## List of specific hostnames to retrieve information. At least (1) required.\n  # hosts = [\"hostname1\", \"hostname2\"]\n\n  ## Using HTTP Basic Authentication. Management API requires 'manage-user' role privileges\n  # username = \"myuser\"\n  # password = \"mypassword\"\n\n  ## Optional TLS Config\n  # tls_ca = \"/etc/telegraf/ca.pem\"\n  # tls_cert = \"/etc/telegraf/cert.pem\"\n  # tls_key = \"/etc/telegraf/key.pem\"\n  ## Use TLS but skip chain \u0026 host verification\n  # insecure_skip_verify = false\n\n"
    },
    {
      "type": "input",
      "name": "cloudwatch",
      "description": "Pull Metric Statistics from Amazon CloudWatch",
      "config": "# Pull Metric Statistics from Amazon CloudWatch\n[[inputs.cloudwatch]]\n  # alias=\"cloudwatch\"\n  ## Amazon Region\n  region = \"us-east-1\"\n\n  ## Amazon Credentials\n  ## Credentials are loaded in the following order\n  ## 1) Assumed credentials via STS if role_arn is specified\n  ## 2) explicit credentials from 'access_key' and 'secret_key'\n  ## 3) shared profile from 'profile'\n  ## 4) environment variables\n  ## 5) shared credentials file\n  ## 6) EC2 Instance Profile\n  # access_key = \"\"\n  # secret_key = \"\"\n  # token = \"\"\n  # role_arn = \"\"\n  # profile = \"\"\n  # shared_credential_file = \"\"\n\n  ## Endpoint to make request against, the correct endpoint is automatically\n  ## determined and this option should only be set if you wish to override the\n  ## default.\n  ##   ex: endpoint_url = \"http://localhost:8000\"\n  # endpoint_url = \"\"\n\n  # The minimum period for Cloudwatch metrics is 1 minute (60s). However not all\n  # metrics are made available to the 1 minute period. Some are collected at\n  # 3 minute, 5 minute, or larger intervals. See https://aws.amazon.com/cloudwatch/faqs/#monitoring.\n  # Note that if a period is configured that is smaller than the minimum for a\n  # particular metric, that metric will not be returned by the Cloudwatch API\n  # and will not be collected by Telegraf.\n  #\n  ## Requested CloudWatch aggregation Period (required - must be a multiple of 60s)\n  period = \"5m\"\n\n  ## Collection Delay (required - must account for metrics availability via CloudWatch API)\n  delay = \"5m\"\n\n  ## Recommended: use metric 'interval' that is a multiple of 'period' to avoid\n  ## gaps or overlap in pulled data\n  interval = \"5m\"\n\n  ## Configure the TTL for the internal cache of metrics.\n  # cache_ttl = \"1h\"\n\n  ## Metric Statistic Namespace (required)\n  namespace = \"AWS/ELB\"\n\n  ## Maximum requests per second. Note that the global default AWS rate limit is\n  ## 50 reqs/sec, so if you define multiple namespaces, these should add up to a\n  ## maximum of 50.\n  ## See http://docs.aws.amazon.com/AmazonCloudWatch/latest/monitoring/cloudwatch_limits.html\n  # ratelimit = 25\n\n  ## Timeout for http requests made by the cloudwatch client.\n  # timeout = \"5s\"\n\n  ## Namespace-wide statistic filters. These allow fewer queries to be made to\n  ## cloudwatch.\n  # statistic_include = [ \"average\", \"sum\", \"minimum\", \"maximum\", sample_count\" ]\n  # statistic_exclude = []\n\n  ## Metrics to Pull\n  ## Defaults to all Metrics in Namespace if nothing is provided\n  ## Refreshes Namespace available metrics every 1h\n  #[[inputs.cloudwatch.metrics]]\n  #  names = [\"Latency\", \"RequestCount\"]\n  #\n  #  ## Statistic filters for Metric.  These allow for retrieving specific\n  #  ## statistics for an individual metric.\n  #  # statistic_include = [ \"average\", \"sum\", \"minimum\", \"maximum\", sample_count\" ]\n  #  # statistic_exclude = []\n  #\n  #  ## Dimension filters for Metric.  All dimensions defined for the metric names\n  #  ## must be specified in order to retrieve the metric statistics.\n  #  [[inputs.cloudwatch.metrics.dimensions]]\n  #    name = \"LoadBalancerName\"\n  #    value = \"p-example\"\n\n"
    },
    {
      "type": "input",
      "name": "system",
      "description": "Read metrics about system load \u0026 uptime",
      "config": "# Read metrics about system load \u0026 uptime\n[[inputs.system]]\n  # alias=\"system\"\n  ## Uncomment to remove deprecated metrics.\n  # fielddrop = [\"uptime_format\"]\n\n"
    },
    {
      "type": "input",
      "name": "docker",
      "description": "Read metrics about docker containers",
      "config": "# Read metrics about docker containers\n[[inputs.docker]]\n  # alias=\"docker\"\n  ## Docker Endpoint\n  ##   To use TCP, set endpoint = \"tcp://[ip]:[port]\"\n  ##   To use environment variables (ie, docker-machine), set endpoint = \"ENV\"\n  endpoint = \"unix:///var/run/docker.sock\"\n\n  ## Set to true to collect Swarm metrics(desired_replicas, running_replicas)\n  gather_services = false\n\n  ## Only collect metrics for these containers, collect all if empty\n  container_names = []\n\n  ## Set the source tag for the metrics to the container ID hostname, eg first 12 chars\n  source_tag = false\n\n  ## Containers to include and exclude. Globs accepted.\n  ## Note that an empty array for both will include all containers\n  container_name_include = []\n  container_name_exclude = []\n\n  ## Container states to include and exclude. Globs accepted.\n  ## When empty only containers in the \"running\" state will be captured.\n  ## example: container_state_include = [\"created\", \"restarting\", \"running\", \"removing\", \"paused\", \"exited\", \"dead\"]\n  ## example: container_state_exclude = [\"created\", \"restarting\", \"running\", \"removing\", \"paused\", \"exited\", \"dead\"]\n  # container_state_include = []\n  # container_state_exclude = []\n\n  ## Timeout for docker list, info, and stats commands\n  timeout = \"5s\"\n\n  ## Whether to report for each container per-device blkio (8:0, 8:1...) and\n  ## network (eth0, eth1, ...) stats or not\n  perdevice = true\n\n  ## Whether to report for each container total blkio and network stats or not\n  total = false\n\n  ## Which environment variables should we use as a tag\n  ##tag_env = [\"JAVA_HOME\", \"HEAP_SIZE\"]\n\n  ## docker labels to include and exclude as tags.  Globs accepted.\n  ## Note that an empty array for both will include all labels as tags\n  docker_label_include = []\n  docker_label_exclude = []\n\n  ## Optional TLS Config\n  # tls_ca = \"/etc/telegraf/ca.pem\"\n  # tls_cert = \"/etc/telegraf/cert.pem\"\n  # tls_key = \"/etc/telegraf/key.pem\"\n  ## Use TLS but skip chain \u0026 host verification\n  # insecure_skip_verify = false\n\n"
    },
    {
      "type": "input",
      "name": "docker_log",
      "description": "Read logging output from the Docker engine",
      "config": "# Read logging output from the Docker engine\n[[inputs.docker_log]]\n  # alias=\"docker_log\"\n  ## Docker Endpoint\n  ##   To use TCP, set endpoint = \"tcp://[ip]:[port]\"\n  ##   To use environment variables (ie, docker-machine), set endpoint = \"ENV\"\n  # endpoint = \"unix:///var/run/docker.sock\"\n\n  ## When true, container logs are read from the beginning; otherwise\n  ## reading begins at the end of the log.\n  # from_beginning = false\n\n  ## Timeout for Docker API calls.\n  # timeout = \"5s\"\n\n  ## Containers to include and exclude. Globs accepted.\n  ## Note that an empty array for both will include all containers\n  # container_name_include = []\n  # container_name_exclude = []\n\n  ## Container states to include and exclude. Globs accepted.\n  ## When empty only containers in the \"running\" state will be captured.\n  # container_state_include = []\n  # container_state_exclude = []\n\n  ## docker labels to include and exclude as tags.  Globs accepted.\n  ## Note that an empty array for both will include all labels as tags\n  # docker_label_include = []\n  # docker_label_exclude = []\n\n  ## Set the source tag for the metrics to the container ID hostname, eg first 12 chars\n  source_tag = false\n\n  ## Optional TLS Config\n  # tls_ca = \"/etc/telegraf/ca.pem\"\n  # tls_cert = \"/etc/telegraf/cert.pem\"\n  # tls_key = \"/etc/telegraf/key.pem\"\n  ## Use TLS but skip chain \u0026 host verification\n  # insecure_skip_verify = false\n\n"
    },
    {
      "type": "input",
      "name": "leofs",
      "description": "Read metrics from a LeoFS Server via SNMP",
      "config": "# Read metrics from a LeoFS Server via SNMP\n[[inputs.leofs]]\n  # alias=\"leofs\"\n  ## An array of URLs of the form:\n  ##   host [ \":\" port]\n  servers = [\"127.0.0.1:4020\"]\n\n"
    },
    {
      "type": "input",
      "name": "procstat",
      "description": "Monitor process cpu and memory usage",
      "config": "# Monitor process cpu and memory usage\n[[inputs.procstat]]\n  # alias=\"procstat\"\n  ## PID file to monitor process\n  pid_file = \"/var/run/nginx.pid\"\n  ## executable name (ie, pgrep \u003cexe\u003e)\n  # exe = \"nginx\"\n  ## pattern as argument for pgrep (ie, pgrep -f \u003cpattern\u003e)\n  # pattern = \"nginx\"\n  ## user as argument for pgrep (ie, pgrep -u \u003cuser\u003e)\n  # user = \"nginx\"\n  ## Systemd unit name\n  # systemd_unit = \"nginx.service\"\n  ## CGroup name or path\n  # cgroup = \"systemd/system.slice/nginx.service\"\n\n  ## Windows service name\n  # win_service = \"\"\n\n  ## override for process_name\n  ## This is optional; default is sourced from /proc/\u003cpid\u003e/status\n  # process_name = \"bar\"\n\n  ## Field name prefix\n  # prefix = \"\"\n\n  ## When true add the full cmdline as a tag.\n  # cmdline_tag = false\n\n  ## Add PID as a tag instead of a field; useful to differentiate between\n  ## processes whose tags are otherwise the same.  Can create a large number\n  ## of series, use judiciously.\n  # pid_tag = false\n\n  ## Method to use when finding process IDs.  Can be one of 'pgrep', or\n  ## 'native'.  The pgrep finder calls the pgrep executable in the PATH while\n  ## the native finder performs the search directly in a manor dependent on the\n  ## platform.  Default is 'pgrep'\n  # pid_finder = \"pgrep\"\n\n"
    },
    {
      "type": "input",
      "name": "salesforce",
      "description": "Read API usage and limits for a Salesforce organisation",
      "config": "# Read API usage and limits for a Salesforce organisation\n[[inputs.salesforce]]\n  # alias=\"salesforce\"\n  ## specify your credentials\n  ##\n  username = \"your_username\"\n  password = \"your_password\"\n  ##\n  ## (optional) security token\n  # security_token = \"your_security_token\"\n  ##\n  ## (optional) environment type (sandbox or production)\n  ## default is: production\n  ##\n  # environment = \"production\"\n  ##\n  ## (optional) API version (default: \"39.0\")\n  ##\n  # version = \"39.0\"\n\n"
    },
    {
      "type": "input",
      "name": "cloud_pubsub_push",
      "description": "Google Cloud Pub/Sub Push HTTP listener",
      "config": "# Google Cloud Pub/Sub Push HTTP listener\n[[inputs.cloud_pubsub_push]]\n  # alias=\"cloud_pubsub_push\"\n  ## Address and port to host HTTP listener on\n  service_address = \":8080\"\n\n  ## Application secret to verify messages originate from Cloud Pub/Sub\n  # token = \"\"\n\n  ## Path to listen to.\n  # path = \"/\"\n\n  ## Maximum duration before timing out read of the request\n  # read_timeout = \"10s\"\n  ## Maximum duration before timing out write of the response. This should be set to a value\n  ## large enough that you can send at least 'metric_batch_size' number of messages within the\n  ## duration.\n  # write_timeout = \"10s\"\n\n  ## Maximum allowed http request body size in bytes.\n  ## 0 means to use the default of 524,288,00 bytes (500 mebibytes)\n  # max_body_size = \"500MB\"\n\n  ## Whether to add the pubsub metadata, such as message attributes and subscription as a tag.\n  # add_meta = false\n\n  ## Optional. Maximum messages to read from PubSub that have not been written\n  ## to an output. Defaults to 1000.\n  ## For best throughput set based on the number of metrics within\n  ## each message and the size of the output's metric_batch_size.\n  ##\n  ## For example, if each message contains 10 metrics and the output\n  ## metric_batch_size is 1000, setting this to 100 will ensure that a\n  ## full batch is collected and the write is triggered immediately without\n  ## waiting until the next flush_interval.\n  # max_undelivered_messages = 1000\n\n  ## Set one or more allowed client CA certificate file names to\n  ## enable mutually authenticated TLS connections\n  # tls_allowed_cacerts = [\"/etc/telegraf/clientca.pem\"]\n\n  ## Add service certificate and key\n  # tls_cert = \"/etc/telegraf/cert.pem\"\n  # tls_key = \"/etc/telegraf/key.pem\"\n\n  ## Data format to consume.\n  ## Each data format has its own unique set of configuration options, read\n  ## more about them here:\n  ## https://github.com/influxdata/telegraf/blob/master/docs/DATA_FORMATS_INPUT.md\n  data_format = \"influx\"\n\n"
    },
    {
      "type": "input",
      "name": "ipvs",
      "description": "Collect virtual and real server stats from Linux IPVS",
      "config": "# Collect virtual and real server stats from Linux IPVS\n[[inputs.ipvs]]\n  # alias=\"ipvs\"\n"
    },
    {
      "type": "input",
      "name": "nginx_vts",
      "description": "Read Nginx virtual host traffic status module information (nginx-module-vts)",
      "config": "# Read Nginx virtual host traffic status module information (nginx-module-vts)\n[[inputs.nginx_vts]]\n  # alias=\"nginx_vts\"\n  ## An array of ngx_http_status_module or status URI to gather stats.\n  urls = [\"http://localhost/status\"]\n\n  ## HTTP response timeout (default: 5s)\n  response_timeout = \"5s\"\n\n  ## Optional TLS Config\n  # tls_ca = \"/etc/telegraf/ca.pem\"\n  # tls_cert = \"/etc/telegraf/cert.pem\"\n  # tls_key = \"/etc/telegraf/key.pem\"\n  ## Use TLS but skip chain \u0026 host verification\n  # insecure_skip_verify = false\n\n"
    },
    {
      "type": "input",
      "name": "ntpq",
      "description": "Get standard NTP query metrics, requires ntpq executable.",
      "config": "# Get standard NTP query metrics, requires ntpq executable.\n[[inputs.ntpq]]\n  # alias=\"ntpq\"\n  ## If false, set the -n ntpq flag. Can reduce metric gather time.\n  dns_lookup = true\n\n"
    },
    {
      "type": "input",
      "name": "openldap",
      "description": "OpenLDAP cn=Monitor plugin",
      "config": "# OpenLDAP cn=Monitor plugin\n[[inputs.openldap]]\n  # alias=\"openldap\"\n  host = \"localhost\"\n  port = 389\n\n  # ldaps, starttls, or no encryption. default is an empty string, disabling all encryption.\n  # note that port will likely need to be changed to 636 for ldaps\n  # valid options: \"\" | \"starttls\" | \"ldaps\"\n  tls = \"\"\n\n  # skip peer certificate verification. Default is false.\n  insecure_skip_verify = false\n\n  # Path to PEM-encoded Root certificate to use to verify server certificate\n  tls_ca = \"/etc/ssl/certs.pem\"\n\n  # dn/password to bind with. If bind_dn is empty, an anonymous bind is performed.\n  bind_dn = \"\"\n  bind_password = \"\"\n\n  # Reverse metric names so they sort more naturally. Recommended.\n  # This defaults to false if unset, but is set to true when generating a new config\n  reverse_metric_names = true\n\n"
    },
    {
      "type": "input",
      "name": "fluentd",
      "description": "Read metrics exposed by fluentd in_monitor plugin",
      "config": "# Read metrics exposed by fluentd in_monitor plugin\n[[inputs.fluentd]]\n  # alias=\"fluentd\"\n  ## This plugin reads information exposed by fluentd (using /api/plugins.json endpoint).\n  ##\n  ## Endpoint:\n  ## - only one URI is allowed\n  ## - https is not supported\n  endpoint = \"http://localhost:24220/api/plugins.json\"\n\n  ## Define which plugins have to be excluded (based on \"type\" field - e.g. monitor_agent)\n  exclude = [\n\t  \"monitor_agent\",\n\t  \"dummy\",\n  ]\n\n"
    },
    {
      "type": "input",
      "name": "nats",
      "description": "Provides metrics about the state of a NATS server",
      "config": "# Provides metrics about the state of a NATS server\n[[inputs.nats]]\n  # alias=\"nats\"\n  ## The address of the monitoring endpoint of the NATS server\n  server = \"http://localhost:8222\"\n\n  ## Maximum time to receive response\n  # response_timeout = \"5s\"\n\n"
    }
  ]
}
`
var availableOutputs = `{
  "version": "1.13.0",
  "os": "linux",
  "plugins": [
    {
      "type": "output",
      "name": "http",
      "description": "A plugin that can transmit metrics over HTTP",
      "config": "# A plugin that can transmit metrics over HTTP\n[[outputs.http]]\n  # alias=\"http\"\n  ## URL is the address to send metrics to\n  url = \"http://127.0.0.1:8080/telegraf\"\n\n  ## Timeout for HTTP message\n  # timeout = \"5s\"\n\n  ## HTTP method, one of: \"POST\" or \"PUT\"\n  # method = \"POST\"\n\n  ## HTTP Basic Auth credentials\n  # username = \"username\"\n  # password = \"pa$$word\"\n\n  ## OAuth2 Client Credentials Grant\n  # client_id = \"clientid\"\n  # client_secret = \"secret\"\n  # token_url = \"https://indentityprovider/oauth2/v1/token\"\n  # scopes = [\"urn:opc:idm:__myscopes__\"]\n\n  ## Optional TLS Config\n  # tls_ca = \"/etc/telegraf/ca.pem\"\n  # tls_cert = \"/etc/telegraf/cert.pem\"\n  # tls_key = \"/etc/telegraf/key.pem\"\n  ## Use TLS but skip chain \u0026 host verification\n  # insecure_skip_verify = false\n\n  ## Data format to output.\n  ## Each data format has it's own unique set of configuration options, read\n  ## more about them here:\n  ## https://github.com/influxdata/telegraf/blob/master/docs/DATA_FORMATS_OUTPUT.md\n  # data_format = \"influx\"\n\n  ## HTTP Content-Encoding for write request body, can be set to \"gzip\" to\n  ## compress body or \"identity\" to apply no encoding.\n  # content_encoding = \"identity\"\n\n  ## Additional HTTP headers\n  # [outputs.http.headers]\n  #   # Should be set manually to \"application/json\" for json data_format\n  #   Content-Type = \"text/plain; charset=utf-8\"\n\n"
    },
    {
      "type": "output",
      "name": "influxdb",
      "description": "Configuration for sending metrics to InfluxDB",
      "config": "# Configuration for sending metrics to InfluxDB\n[[outputs.influxdb]]\n  # alias=\"influxdb\"\n  ## The full HTTP or UDP URL for your InfluxDB instance.\n  ##\n  ## Multiple URLs can be specified for a single cluster, only ONE of the\n  ## urls will be written to each interval.\n  # urls = [\"unix:///var/run/influxdb.sock\"]\n  # urls = [\"udp://127.0.0.1:8089\"]\n  # urls = [\"http://127.0.0.1:8086\"]\n\n  ## The target database for metrics; will be created as needed.\n  ## For UDP url endpoint database needs to be configured on server side.\n  # database = \"telegraf\"\n\n  ## The value of this tag will be used to determine the database.  If this\n  ## tag is not set the 'database' option is used as the default.\n  # database_tag = \"\"\n\n  ## If true, the database tag will not be added to the metric.\n  # exclude_database_tag = false\n\n  ## If true, no CREATE DATABASE queries will be sent.  Set to true when using\n  ## Telegraf with a user without permissions to create databases or when the\n  ## database already exists.\n  # skip_database_creation = false\n\n  ## Name of existing retention policy to write to.  Empty string writes to\n  ## the default retention policy.  Only takes effect when using HTTP.\n  # retention_policy = \"\"\n\n  ## Write consistency (clusters only), can be: \"any\", \"one\", \"quorum\", \"all\".\n  ## Only takes effect when using HTTP.\n  # write_consistency = \"any\"\n\n  ## Timeout for HTTP messages.\n  # timeout = \"5s\"\n\n  ## HTTP Basic Auth\n  # username = \"telegraf\"\n  # password = \"metricsmetricsmetricsmetrics\"\n\n  ## HTTP User-Agent\n  # user_agent = \"telegraf\"\n\n  ## UDP payload size is the maximum packet size to send.\n  # udp_payload = \"512B\"\n\n  ## Optional TLS Config for use on HTTP connections.\n  # tls_ca = \"/etc/telegraf/ca.pem\"\n  # tls_cert = \"/etc/telegraf/cert.pem\"\n  # tls_key = \"/etc/telegraf/key.pem\"\n  ## Use TLS but skip chain \u0026 host verification\n  # insecure_skip_verify = false\n\n  ## HTTP Proxy override, if unset values the standard proxy environment\n  ## variables are consulted to determine which proxy, if any, should be used.\n  # http_proxy = \"http://corporate.proxy:3128\"\n\n  ## Additional HTTP headers\n  # http_headers = {\"X-Special-Header\" = \"Special-Value\"}\n\n  ## HTTP Content-Encoding for write request body, can be set to \"gzip\" to\n  ## compress body or \"identity\" to apply no encoding.\n  # content_encoding = \"identity\"\n\n  ## When true, Telegraf will output unsigned integers as unsigned values,\n  ## i.e.: \"42u\".  You will need a version of InfluxDB supporting unsigned\n  ## integer values.  Enabling this option will result in field type errors if\n  ## existing data has been written.\n  # influx_uint_support = false\n\n"
    },
    {
      "type": "output",
      "name": "exec",
      "description": "Send metrics to command as input over stdin",
      "config": "# Send metrics to command as input over stdin\n[[outputs.exec]]\n  # alias=\"exec\"\n  ## Command to injest metrics via stdin.\n  command = [\"tee\", \"-a\", \"/dev/null\"]\n\n  ## Timeout for command to complete.\n  # timeout = \"5s\"\n\n  ## Data format to output.\n  ## Each data format has its own unique set of configuration options, read\n  ## more about them here:\n  ## https://github.com/influxdata/telegraf/blob/master/docs/DATA_FORMATS_OUTPUT.md\n  # data_format = \"influx\"\n\n"
    },
    {
      "type": "output",
      "name": "graphite",
      "description": "Configuration for Graphite server to send metrics to",
      "config": "# Configuration for Graphite server to send metrics to\n[[outputs.graphite]]\n  # alias=\"graphite\"\n  ## TCP endpoint for your graphite instance.\n  ## If multiple endpoints are configured, output will be load balanced.\n  ## Only one of the endpoints will be written to with each iteration.\n  servers = [\"localhost:2003\"]\n  ## Prefix metrics name\n  prefix = \"\"\n  ## Graphite output template\n  ## see https://github.com/influxdata/telegraf/blob/master/docs/DATA_FORMATS_OUTPUT.md\n  template = \"host.tags.measurement.field\"\n\n  ## Enable Graphite tags support\n  # graphite_tag_support = false\n\n  ## timeout in seconds for the write connection to graphite\n  timeout = 2\n\n  ## Optional TLS Config\n  # tls_ca = \"/etc/telegraf/ca.pem\"\n  # tls_cert = \"/etc/telegraf/cert.pem\"\n  # tls_key = \"/etc/telegraf/key.pem\"\n  ## Use TLS but skip chain \u0026 host verification\n  # insecure_skip_verify = false\n\n"
    },
    {
      "type": "output",
      "name": "graylog",
      "description": "Send telegraf metrics to graylog(s)",
      "config": "# Send telegraf metrics to graylog(s)\n[[outputs.graylog]]\n  # alias=\"graylog\"\n  ## UDP endpoint for your graylog instance.\n  servers = [\"127.0.0.1:12201\", \"192.168.1.1:12201\"]\n\n"
    },
    {
      "type": "output",
      "name": "nats",
      "description": "Send telegraf measurements to NATS",
      "config": "# Send telegraf measurements to NATS\n[[outputs.nats]]\n  # alias=\"nats\"\n  ## URLs of NATS servers\n  servers = [\"nats://localhost:4222\"]\n  ## Optional credentials\n  # username = \"\"\n  # password = \"\"\n  ## NATS subject for producer messages\n  subject = \"telegraf\"\n\n  ## Use Transport Layer Security\n  # secure = false\n\n  ## Optional TLS Config\n  # tls_ca = \"/etc/telegraf/ca.pem\"\n  # tls_cert = \"/etc/telegraf/cert.pem\"\n  # tls_key = \"/etc/telegraf/key.pem\"\n  ## Use TLS but skip chain \u0026 host verification\n  # insecure_skip_verify = false\n\n  ## Data format to output.\n  ## Each data format has its own unique set of configuration options, read\n  ## more about them here:\n  ## https://github.com/influxdata/telegraf/blob/master/docs/DATA_FORMATS_OUTPUT.md\n  data_format = \"influx\"\n\n"
    },
    {
      "type": "output",
      "name": "prometheus_client",
      "description": "Configuration for the Prometheus client to spawn",
      "config": "# Configuration for the Prometheus client to spawn\n[[outputs.prometheus_client]]\n  # alias=\"prometheus_client\"\n  ## Address to listen on\n  listen = \":9273\"\n\n  ## Metric version controls the mapping from Telegraf metrics into\n  ## Prometheus format.  When using the prometheus input, use the same value in\n  ## both plugins to ensure metrics are round-tripped without modification.\n  ##\n  ##   example: metric_version = 1; deprecated in 1.13\n  ##            metric_version = 2; recommended version\n  # metric_version = 1\n\n  ## Use HTTP Basic Authentication.\n  # basic_username = \"Foo\"\n  # basic_password = \"Bar\"\n\n  ## If set, the IP Ranges which are allowed to access metrics.\n  ##   ex: ip_range = [\"192.168.0.0/24\", \"192.168.1.0/30\"]\n  # ip_range = []\n\n  ## Path to publish the metrics on.\n  # path = \"/metrics\"\n\n  ## Expiration interval for each metric. 0 == no expiration\n  # expiration_interval = \"60s\"\n\n  ## Collectors to enable, valid entries are \"gocollector\" and \"process\".\n  ## If unset, both are enabled.\n  # collectors_exclude = [\"gocollector\", \"process\"]\n\n  ## Send string metrics as Prometheus labels.\n  ## Unless set to false all string metrics will be sent as labels.\n  # string_as_label = true\n\n  ## If set, enable TLS with the given certificate.\n  # tls_cert = \"/etc/ssl/telegraf.crt\"\n  # tls_key = \"/etc/ssl/telegraf.key\"\n\n  ## Set one or more allowed client CA certificate file names to\n  ## enable mutually authenticated TLS connections\n  # tls_allowed_cacerts = [\"/etc/telegraf/clientca.pem\"]\n\n  ## Export metric collection time.\n  # export_timestamp = false\n\n"
    },
    {
      "type": "output",
      "name": "riemann",
      "description": "Configuration for the Riemann server to send metrics to",
      "config": "# Configuration for the Riemann server to send metrics to\n[[outputs.riemann]]\n  # alias=\"riemann\"\n  ## The full TCP or UDP URL of the Riemann server\n  url = \"tcp://localhost:5555\"\n\n  ## Riemann event TTL, floating-point time in seconds.\n  ## Defines how long that an event is considered valid for in Riemann\n  # ttl = 30.0\n\n  ## Separator to use between measurement and field name in Riemann service name\n  ## This does not have any effect if 'measurement_as_attribute' is set to 'true'\n  separator = \"/\"\n\n  ## Set measurement name as Riemann attribute 'measurement', instead of prepending it to the Riemann service name\n  # measurement_as_attribute = false\n\n  ## Send string metrics as Riemann event states.\n  ## Unless enabled all string metrics will be ignored\n  # string_as_state = false\n\n  ## A list of tag keys whose values get sent as Riemann tags.\n  ## If empty, all Telegraf tag values will be sent as tags\n  # tag_keys = [\"telegraf\",\"custom_tag\"]\n\n  ## Additional Riemann tags to send.\n  # tags = [\"telegraf-output\"]\n\n  ## Description for Riemann event\n  # description_text = \"metrics collected from telegraf\"\n\n  ## Riemann client write timeout, defaults to \"5s\" if not set.\n  # timeout = \"5s\"\n\n"
    },
    {
      "type": "output",
      "name": "wavefront",
      "description": "Configuration for Wavefront server to send metrics to",
      "config": "# Configuration for Wavefront server to send metrics to\n[[outputs.wavefront]]\n  # alias=\"wavefront\"\n  ## Url for Wavefront Direct Ingestion or using HTTP with Wavefront Proxy\n  ## If using Wavefront Proxy, also specify port. example: http://proxyserver:2878\n  url = \"https://metrics.wavefront.com\"\n\n  ## Authentication Token for Wavefront. Only required if using Direct Ingestion\n  #token = \"DUMMY_TOKEN\"  \n  \n  ## DNS name of the wavefront proxy server. Do not use if url is specified\n  #host = \"wavefront.example.com\"\n\n  ## Port that the Wavefront proxy server listens on. Do not use if url is specified\n  #port = 2878\n\n  ## prefix for metrics keys\n  #prefix = \"my.specific.prefix.\"\n\n  ## whether to use \"value\" for name of simple fields. default is false\n  #simple_fields = false\n\n  ## character to use between metric and field name.  default is . (dot)\n  #metric_separator = \".\"\n\n  ## Convert metric name paths to use metricSeparator character\n  ## When true will convert all _ (underscore) characters in final metric name. default is true\n  #convert_paths = true\n\n  ## Use Strict rules to sanitize metric and tag names from invalid characters\n  ## When enabled forward slash (/) and comma (,) will be accpeted\n  #use_strict = false\n\n  ## Use Regex to sanitize metric and tag names from invalid characters\n  ## Regex is more thorough, but significantly slower. default is false\n  #use_regex = false\n\n  ## point tags to use as the source name for Wavefront (if none found, host will be used)\n  #source_override = [\"hostname\", \"address\", \"agent_host\", \"node_host\"]\n\n  ## whether to convert boolean values to numeric values, with false -\u003e 0.0 and true -\u003e 1.0. default is true\n  #convert_bool = true\n\n  ## Define a mapping, namespaced by metric prefix, from string values to numeric values\n  ##   deprecated in 1.9; use the enum processor plugin\n  #[[outputs.wavefront.string_to_number.elasticsearch]]\n  #  green = 1.0\n  #  yellow = 0.5\n  #  red = 0.0\n\n"
    },
    {
      "type": "output",
      "name": "cloudwatch",
      "description": "Configuration for AWS CloudWatch output.",
      "config": "# Configuration for AWS CloudWatch output.\n[[outputs.cloudwatch]]\n  # alias=\"cloudwatch\"\n  ## Amazon REGION\n  region = \"us-east-1\"\n\n  ## Amazon Credentials\n  ## Credentials are loaded in the following order\n  ## 1) Assumed credentials via STS if role_arn is specified\n  ## 2) explicit credentials from 'access_key' and 'secret_key'\n  ## 3) shared profile from 'profile'\n  ## 4) environment variables\n  ## 5) shared credentials file\n  ## 6) EC2 Instance Profile\n  #access_key = \"\"\n  #secret_key = \"\"\n  #token = \"\"\n  #role_arn = \"\"\n  #profile = \"\"\n  #shared_credential_file = \"\"\n\n  ## Endpoint to make request against, the correct endpoint is automatically\n  ## determined and this option should only be set if you wish to override the\n  ## default.\n  ##   ex: endpoint_url = \"http://localhost:8000\"\n  # endpoint_url = \"\"\n\n  ## Namespace for the CloudWatch MetricDatums\n  namespace = \"InfluxData/Telegraf\"\n\n  ## If you have a large amount of metrics, you should consider to send statistic \n  ## values instead of raw metrics which could not only improve performance but \n  ## also save AWS API cost. If enable this flag, this plugin would parse the required \n  ## CloudWatch statistic fields (count, min, max, and sum) and send them to CloudWatch. \n  ## You could use basicstats aggregator to calculate those fields. If not all statistic \n  ## fields are available, all fields would still be sent as raw metrics. \n  # write_statistics = false\n\n  ## Enable high resolution metrics of 1 second (if not enabled, standard resolution are of 60 seconds precision)\n  # high_resolution_metrics = false\n\n"
    },
    {
      "type": "output",
      "name": "datadog",
      "description": "Configuration for DataDog API to send metrics to.",
      "config": "# Configuration for DataDog API to send metrics to.\n[[outputs.datadog]]\n  # alias=\"datadog\"\n  ## Datadog API key\n  apikey = \"my-secret-key\" # required.\n\n  # The base endpoint URL can optionally be specified but it defaults to:\n  #url = \"https://app.datadoghq.com/api/v1/series\"\n\n  ## Connection timeout.\n  # timeout = \"5s\"\n\n"
    },
    {
      "type": "output",
      "name": "discard",
      "description": "Send metrics to nowhere at all",
      "config": "# Send metrics to nowhere at all\n[[outputs.discard]]\n  # alias=\"discard\"\n"
    },
    {
      "type": "output",
      "name": "health",
      "description": "Configurable HTTP health check resource based on metrics",
      "config": "# Configurable HTTP health check resource based on metrics\n[[outputs.health]]\n  # alias=\"health\"\n  ## Address and port to listen on.\n  ##   ex: service_address = \"http://localhost:8080\"\n  ##       service_address = \"unix:///var/run/telegraf-health.sock\"\n  # service_address = \"http://:8080\"\n\n  ## The maximum duration for reading the entire request.\n  # read_timeout = \"5s\"\n  ## The maximum duration for writing the entire response.\n  # write_timeout = \"5s\"\n\n  ## Username and password to accept for HTTP basic authentication.\n  # basic_username = \"user1\"\n  # basic_password = \"secret\"\n\n  ## Allowed CA certificates for client certificates.\n  # tls_allowed_cacerts = [\"/etc/telegraf/clientca.pem\"]\n\n  ## TLS server certificate and private key.\n  # tls_cert = \"/etc/telegraf/cert.pem\"\n  # tls_key = \"/etc/telegraf/key.pem\"\n\n  ## One or more check sub-tables should be defined, it is also recommended to\n  ## use metric filtering to limit the metrics that flow into this output.\n  ##\n  ## When using the default buffer sizes, this example will fail when the\n  ## metric buffer is half full.\n  ##\n  ## namepass = [\"internal_write\"]\n  ## tagpass = { output = [\"influxdb\"] }\n  ##\n  ## [[outputs.health.compares]]\n  ##   field = \"buffer_size\"\n  ##   lt = 5000.0\n  ##\n  ## [[outputs.health.contains]]\n  ##   field = \"buffer_size\"\n\n"
    },
    {
      "type": "output",
      "name": "kinesis",
      "description": "Configuration for the AWS Kinesis output.",
      "config": "# Configuration for the AWS Kinesis output.\n[[outputs.kinesis]]\n  # alias=\"kinesis\"\n  ## Amazon REGION of kinesis endpoint.\n  region = \"ap-southeast-2\"\n\n  ## Amazon Credentials\n  ## Credentials are loaded in the following order\n  ## 1) Assumed credentials via STS if role_arn is specified\n  ## 2) explicit credentials from 'access_key' and 'secret_key'\n  ## 3) shared profile from 'profile'\n  ## 4) environment variables\n  ## 5) shared credentials file\n  ## 6) EC2 Instance Profile\n  #access_key = \"\"\n  #secret_key = \"\"\n  #token = \"\"\n  #role_arn = \"\"\n  #profile = \"\"\n  #shared_credential_file = \"\"\n\n  ## Endpoint to make request against, the correct endpoint is automatically\n  ## determined and this option should only be set if you wish to override the\n  ## default.\n  ##   ex: endpoint_url = \"http://localhost:8000\"\n  # endpoint_url = \"\"\n\n  ## Kinesis StreamName must exist prior to starting telegraf.\n  streamname = \"StreamName\"\n  ## DEPRECATED: PartitionKey as used for sharding data.\n  partitionkey = \"PartitionKey\"\n  ## DEPRECATED: If set the paritionKey will be a random UUID on every put.\n  ## This allows for scaling across multiple shards in a stream.\n  ## This will cause issues with ordering.\n  use_random_partitionkey = false\n  ## The partition key can be calculated using one of several methods:\n  ##\n  ## Use a static value for all writes:\n  #  [outputs.kinesis.partition]\n  #    method = \"static\"\n  #    key = \"howdy\"\n  #\n  ## Use a random partition key on each write:\n  #  [outputs.kinesis.partition]\n  #    method = \"random\"\n  #\n  ## Use the measurement name as the partition key:\n  #  [outputs.kinesis.partition]\n  #    method = \"measurement\"\n  #\n  ## Use the value of a tag for all writes, if the tag is not set the empty\n  ## default option will be used. When no default, defaults to \"telegraf\"\n  #  [outputs.kinesis.partition]\n  #    method = \"tag\"\n  #    key = \"host\"\n  #    default = \"mykey\"\n\n\n  ## Data format to output.\n  ## Each data format has its own unique set of configuration options, read\n  ## more about them here:\n  ## https://github.com/influxdata/telegraf/blob/master/docs/DATA_FORMATS_OUTPUT.md\n  data_format = \"influx\"\n\n  ## debug will show upstream aws messages.\n  debug = false\n\n"
    },
    {
      "type": "output",
      "name": "riemann_legacy",
      "description": "Configuration for the Riemann server to send metrics to",
      "config": "# Configuration for the Riemann server to send metrics to\n[[outputs.riemann_legacy]]\n  # alias=\"riemann_legacy\"\n  ## URL of server\n  url = \"localhost:5555\"\n  ## transport protocol to use either tcp or udp\n  transport = \"tcp\"\n  ## separator to use between input name and field name in Riemann service name\n  separator = \" \"\n\n"
    },
    {
      "type": "output",
      "name": "stackdriver",
      "description": "Configuration for Google Cloud Stackdriver to send metrics to",
      "config": "# Configuration for Google Cloud Stackdriver to send metrics to\n[[outputs.stackdriver]]\n  # alias=\"stackdriver\"\n  ## GCP Project\n  project = \"erudite-bloom-151019\"\n\n  ## The namespace for the metric descriptor\n  namespace = \"telegraf\"\n\n  ## Custom resource type\n  # resource_type = \"generic_node\"\n\n  ## Additonal resource labels\n  # [outputs.stackdriver.resource_labels]\n  #   node_id = \"$HOSTNAME\"\n  #   namespace = \"myapp\"\n  #   location = \"eu-north0\"\n\n"
    },
    {
      "type": "output",
      "name": "amon",
      "description": "Configuration for Amon Server to send metrics to.",
      "config": "# Configuration for Amon Server to send metrics to.\n[[outputs.amon]]\n  # alias=\"amon\"\n  ## Amon Server Key\n  server_key = \"my-server-key\" # required.\n\n  ## Amon Instance URL\n  amon_instance = \"https://youramoninstance\" # required\n\n  ## Connection timeout.\n  # timeout = \"5s\"\n\n"
    },
    {
      "type": "output",
      "name": "application_insights",
      "description": "Send metrics to Azure Application Insights",
      "config": "# Send metrics to Azure Application Insights\n[[outputs.application_insights]]\n  # alias=\"application_insights\"\n  ## Instrumentation key of the Application Insights resource.\n  instrumentation_key = \"xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxx\"\n\n  ## Timeout for closing (default: 5s).\n  # timeout = \"5s\"\n\n  ## Enable additional diagnostic logging.\n  # enable_diagnostic_logging = false\n\n  ## Context Tag Sources add Application Insights context tags to a tag value.\n  ##\n  ## For list of allowed context tag keys see:\n  ## https://github.com/Microsoft/ApplicationInsights-Go/blob/master/appinsights/contracts/contexttagkeys.go\n  # [outputs.application_insights.context_tag_sources]\n  #   \"ai.cloud.role\" = \"kubernetes_container_name\"\n  #   \"ai.cloud.roleInstance\" = \"kubernetes_pod_name\"\n\n"
    },
    {
      "type": "output",
      "name": "file",
      "description": "Send telegraf metrics to file(s)",
      "config": "# Send telegraf metrics to file(s)\n[[outputs.file]]\n  # alias=\"file\"\n  ## Files to write to, \"stdout\" is a specially handled file.\n  files = [\"stdout\", \"/tmp/metrics.out\"]\n\n  ## Use batch serialization format instead of line based delimiting.  The\n  ## batch format allows for the production of non line based output formats and\n  ## may more effiently encode metric groups.\n  # use_batch_format = false\n\n  ## The file will be rotated after the time interval specified.  When set\n  ## to 0 no time based rotation is performed.\n  # rotation_interval = \"0d\"\n\n  ## The logfile will be rotated when it becomes larger than the specified\n  ## size.  When set to 0 no size based rotation is performed.\n  # rotation_max_size = \"0MB\"\n\n  ## Maximum number of rotated archives to keep, any older logs are deleted.\n  ## If set to -1, no archives are removed.\n  # rotation_max_archives = 5\n\n  ## Data format to output.\n  ## Each data format has its own unique set of configuration options, read\n  ## more about them here:\n  ## https://github.com/influxdata/telegraf/blob/master/docs/DATA_FORMATS_OUTPUT.md\n  data_format = \"influx\"\n\n"
    },
    {
      "type": "output",
      "name": "opentsdb",
      "description": "Configuration for OpenTSDB server to send metrics to",
      "config": "# Configuration for OpenTSDB server to send metrics to\n[[outputs.opentsdb]]\n  # alias=\"opentsdb\"\n  ## prefix for metrics keys\n  prefix = \"my.specific.prefix.\"\n\n  ## DNS name of the OpenTSDB server\n  ## Using \"opentsdb.example.com\" or \"tcp://opentsdb.example.com\" will use the\n  ## telnet API. \"http://opentsdb.example.com\" will use the Http API.\n  host = \"opentsdb.example.com\"\n\n  ## Port of the OpenTSDB server\n  port = 4242\n\n  ## Number of data points to send to OpenTSDB in Http requests.\n  ## Not used with telnet API.\n  http_batch_size = 50\n\n  ## URI Path for Http requests to OpenTSDB.\n  ## Used in cases where OpenTSDB is located behind a reverse proxy.\n  http_path = \"/api/put\"\n\n  ## Debug true - Prints OpenTSDB communication\n  debug = false\n\n  ## Separator separates measurement name from field\n  separator = \"_\"\n\n"
    },
    {
      "type": "output",
      "name": "amqp",
      "description": "Publishes metrics to an AMQP broker",
      "config": "# Publishes metrics to an AMQP broker\n[[outputs.amqp]]\n  # alias=\"amqp\"\n  ## Broker to publish to.\n  ##   deprecated in 1.7; use the brokers option\n  # url = \"amqp://localhost:5672/influxdb\"\n\n  ## Brokers to publish to.  If multiple brokers are specified a random broker\n  ## will be selected anytime a connection is established.  This can be\n  ## helpful for load balancing when not using a dedicated load balancer.\n  brokers = [\"amqp://localhost:5672/influxdb\"]\n\n  ## Maximum messages to send over a connection.  Once this is reached, the\n  ## connection is closed and a new connection is made.  This can be helpful for\n  ## load balancing when not using a dedicated load balancer.\n  # max_messages = 0\n\n  ## Exchange to declare and publish to.\n  exchange = \"telegraf\"\n\n  ## Exchange type; common types are \"direct\", \"fanout\", \"topic\", \"header\", \"x-consistent-hash\".\n  # exchange_type = \"topic\"\n\n  ## If true, exchange will be passively declared.\n  # exchange_passive = false\n\n  ## Exchange durability can be either \"transient\" or \"durable\".\n  # exchange_durability = \"durable\"\n\n  ## Additional exchange arguments.\n  # exchange_arguments = { }\n  # exchange_arguments = {\"hash_propery\" = \"timestamp\"}\n\n  ## Authentication credentials for the PLAIN auth_method.\n  # username = \"\"\n  # password = \"\"\n\n  ## Auth method. PLAIN and EXTERNAL are supported\n  ## Using EXTERNAL requires enabling the rabbitmq_auth_mechanism_ssl plugin as\n  ## described here: https://www.rabbitmq.com/plugins.html\n  # auth_method = \"PLAIN\"\n\n  ## Metric tag to use as a routing key.\n  ##   ie, if this tag exists, its value will be used as the routing key\n  # routing_tag = \"host\"\n\n  ## Static routing key.  Used when no routing_tag is set or as a fallback\n  ## when the tag specified in routing tag is not found.\n  # routing_key = \"\"\n  # routing_key = \"telegraf\"\n\n  ## Delivery Mode controls if a published message is persistent.\n  ##   One of \"transient\" or \"persistent\".\n  # delivery_mode = \"transient\"\n\n  ## InfluxDB database added as a message header.\n  ##   deprecated in 1.7; use the headers option\n  # database = \"telegraf\"\n\n  ## InfluxDB retention policy added as a message header\n  ##   deprecated in 1.7; use the headers option\n  # retention_policy = \"default\"\n\n  ## Static headers added to each published message.\n  # headers = { }\n  # headers = {\"database\" = \"telegraf\", \"retention_policy\" = \"default\"}\n\n  ## Connection timeout.  If not provided, will default to 5s.  0s means no\n  ## timeout (not recommended).\n  # timeout = \"5s\"\n\n  ## Optional TLS Config\n  # tls_ca = \"/etc/telegraf/ca.pem\"\n  # tls_cert = \"/etc/telegraf/cert.pem\"\n  # tls_key = \"/etc/telegraf/key.pem\"\n  ## Use TLS but skip chain \u0026 host verification\n  # insecure_skip_verify = false\n\n  ## If true use batch serialization format instead of line based delimiting.\n  ## Only applies to data formats which are not line based such as JSON.\n  ## Recommended to set to true.\n  # use_batch_format = false\n\n  ## Content encoding for message payloads, can be set to \"gzip\" to or\n  ## \"identity\" to apply no encoding.\n  ##\n  ## Please note that when use_batch_format = false each amqp message contains only\n  ## a single metric, it is recommended to use compression with batch format\n  ## for best results.\n  # content_encoding = \"identity\"\n\n  ## Data format to output.\n  ## Each data format has its own unique set of configuration options, read\n  ## more about them here:\n  ## https://github.com/influxdata/telegraf/blob/master/docs/DATA_FORMATS_OUTPUT.md\n  # data_format = \"influx\"\n\n"
    },
    {
      "type": "output",
      "name": "azure_monitor",
      "description": "Send aggregate metrics to Azure Monitor",
      "config": "# Send aggregate metrics to Azure Monitor\n[[outputs.azure_monitor]]\n  # alias=\"azure_monitor\"\n  ## Timeout for HTTP writes.\n  # timeout = \"20s\"\n\n  ## Set the namespace prefix, defaults to \"Telegraf/\u003cinput-name\u003e\".\n  # namespace_prefix = \"Telegraf/\"\n\n  ## Azure Monitor doesn't have a string value type, so convert string\n  ## fields to dimensions (a.k.a. tags) if enabled. Azure Monitor allows\n  ## a maximum of 10 dimensions so Telegraf will only send the first 10\n  ## alphanumeric dimensions.\n  # strings_as_dimensions = false\n\n  ## Both region and resource_id must be set or be available via the\n  ## Instance Metadata service on Azure Virtual Machines.\n  #\n  ## Azure Region to publish metrics against.\n  ##   ex: region = \"southcentralus\"\n  # region = \"\"\n  #\n  ## The Azure Resource ID against which metric will be logged, e.g.\n  ##   ex: resource_id = \"/subscriptions/\u003csubscription_id\u003e/resourceGroups/\u003cresource_group\u003e/providers/Microsoft.Compute/virtualMachines/\u003cvm_name\u003e\"\n  # resource_id = \"\"\n\n  ## Optionally, if in Azure US Government, China or other sovereign\n  ## cloud environment, set appropriate REST endpoint for receiving\n  ## metrics. (Note: region may be unused in this context)\n  # endpoint_url = \"https://monitoring.core.usgovcloudapi.net\"\n\n"
    },
    {
      "type": "output",
      "name": "syslog",
      "description": "Configuration for Syslog server to send metrics to",
      "config": "# Configuration for Syslog server to send metrics to\n[[outputs.syslog]]\n  # alias=\"syslog\"\n  ## URL to connect to\n  ## ex: address = \"tcp://127.0.0.1:8094\"\n  ## ex: address = \"tcp4://127.0.0.1:8094\"\n  ## ex: address = \"tcp6://127.0.0.1:8094\"\n  ## ex: address = \"tcp6://[2001:db8::1]:8094\"\n  ## ex: address = \"udp://127.0.0.1:8094\"\n  ## ex: address = \"udp4://127.0.0.1:8094\"\n  ## ex: address = \"udp6://127.0.0.1:8094\"\n  address = \"tcp://127.0.0.1:8094\"\n\n  ## Optional TLS Config\n  # tls_ca = \"/etc/telegraf/ca.pem\"\n  # tls_cert = \"/etc/telegraf/cert.pem\"\n  # tls_key = \"/etc/telegraf/key.pem\"\n  ## Use TLS but skip chain \u0026 host verification\n  # insecure_skip_verify = false\n\n  ## Period between keep alive probes.\n  ## Only applies to TCP sockets.\n  ## 0 disables keep alive probes.\n  ## Defaults to the OS configuration.\n  # keep_alive_period = \"5m\"\n\n  ## The framing technique with which it is expected that messages are\n  ## transported (default = \"octet-counting\").  Whether the messages come\n  ## using the octect-counting (RFC5425#section-4.3.1, RFC6587#section-3.4.1),\n  ## or the non-transparent framing technique (RFC6587#section-3.4.2).  Must\n  ## be one of \"octet-counting\", \"non-transparent\".\n  # framing = \"octet-counting\"\n\n  ## The trailer to be expected in case of non-trasparent framing (default = \"LF\").\n  ## Must be one of \"LF\", or \"NUL\".\n  # trailer = \"LF\"\n\n  ## SD-PARAMs settings\n  ## Syslog messages can contain key/value pairs within zero or more\n  ## structured data sections.  For each unrecognised metric tag/field a\n  ## SD-PARAMS is created.\n  ##\n  ## Example:\n  ##   [[outputs.syslog]]\n  ##     sdparam_separator = \"_\"\n  ##     default_sdid = \"default@32473\"\n  ##     sdids = [\"foo@123\", \"bar@456\"]\n  ##\n  ##   input =\u003e xyzzy,x=y foo@123_value=42,bar@456_value2=84,something_else=1\n  ##   output (structured data only) =\u003e [foo@123 value=42][bar@456 value2=84][default@32473 something_else=1 x=y]\n\n  ## SD-PARAMs separator between the sdid and tag/field key (default = \"_\")\n  # sdparam_separator = \"_\"\n\n  ## Default sdid used for tags/fields that don't contain a prefix defined in\n  ## the explict sdids setting below If no default is specified, no SD-PARAMs\n  ## will be used for unrecognised field.\n  # default_sdid = \"default@32473\"\n\n  ## List of explicit prefixes to extract from tag/field keys and use as the\n  ## SDID, if they match (see above example for more details):\n  # sdids = [\"foo@123\", \"bar@456\"]\n\n  ## Default severity value. Severity and Facility are used to calculate the\n  ## message PRI value (RFC5424#section-6.2.1).  Used when no metric field\n  ## with key \"severity_code\" is defined.  If unset, 5 (notice) is the default\n  # default_severity_code = 5\n\n  ## Default facility value. Facility and Severity are used to calculate the\n  ## message PRI value (RFC5424#section-6.2.1).  Used when no metric field with\n  ## key \"facility_code\" is defined.  If unset, 1 (user-level) is the default\n  # default_facility_code = 1\n\n  ## Default APP-NAME value (RFC5424#section-6.2.5)\n  ## Used when no metric tag with key \"appname\" is defined.\n  ## If unset, \"Telegraf\" is the default\n  # default_appname = \"Telegraf\"\n\n"
    },
    {
      "type": "output",
      "name": "nsq",
      "description": "Send telegraf measurements to NSQD",
      "config": "# Send telegraf measurements to NSQD\n[[outputs.nsq]]\n  # alias=\"nsq\"\n  ## Location of nsqd instance listening on TCP\n  server = \"localhost:4150\"\n  ## NSQ topic for producer messages\n  topic = \"telegraf\"\n\n  ## Data format to output.\n  ## Each data format has its own unique set of configuration options, read\n  ## more about them here:\n  ## https://github.com/influxdata/telegraf/blob/master/docs/DATA_FORMATS_OUTPUT.md\n  data_format = \"influx\"\n\n"
    },
    {
      "type": "output",
      "name": "socket_writer",
      "description": "Generic socket writer capable of handling multiple socket types.",
      "config": "# Generic socket writer capable of handling multiple socket types.\n[[outputs.socket_writer]]\n  # alias=\"socket_writer\"\n  ## URL to connect to\n  # address = \"tcp://127.0.0.1:8094\"\n  # address = \"tcp://example.com:http\"\n  # address = \"tcp4://127.0.0.1:8094\"\n  # address = \"tcp6://127.0.0.1:8094\"\n  # address = \"tcp6://[2001:db8::1]:8094\"\n  # address = \"udp://127.0.0.1:8094\"\n  # address = \"udp4://127.0.0.1:8094\"\n  # address = \"udp6://127.0.0.1:8094\"\n  # address = \"unix:///tmp/telegraf.sock\"\n  # address = \"unixgram:///tmp/telegraf.sock\"\n\n  ## Optional TLS Config\n  # tls_ca = \"/etc/telegraf/ca.pem\"\n  # tls_cert = \"/etc/telegraf/cert.pem\"\n  # tls_key = \"/etc/telegraf/key.pem\"\n  ## Use TLS but skip chain \u0026 host verification\n  # insecure_skip_verify = false\n\n  ## Period between keep alive probes.\n  ## Only applies to TCP sockets.\n  ## 0 disables keep alive probes.\n  ## Defaults to the OS configuration.\n  # keep_alive_period = \"5m\"\n\n  ## Data format to generate.\n  ## Each data format has its own unique set of configuration options, read\n  ## more about them here:\n  ## https://github.com/influxdata/telegraf/blob/master/docs/DATA_FORMATS_INPUT.md\n  # data_format = \"influx\"\n\n"
    },
    {
      "type": "output",
      "name": "mqtt",
      "description": "Configuration for MQTT server to send metrics to",
      "config": "# Configuration for MQTT server to send metrics to\n[[outputs.mqtt]]\n  # alias=\"mqtt\"\n  servers = [\"localhost:1883\"] # required.\n\n  ## MQTT outputs send metrics to this topic format\n  ##    \"\u003ctopic_prefix\u003e/\u003chostname\u003e/\u003cpluginname\u003e/\"\n  ##   ex: prefix/web01.example.com/mem\n  topic_prefix = \"telegraf\"\n\n  ## QoS policy for messages\n  ##   0 = at most once\n  ##   1 = at least once\n  ##   2 = exactly once\n  # qos = 2\n\n  ## username and password to connect MQTT server.\n  # username = \"telegraf\"\n  # password = \"metricsmetricsmetricsmetrics\"\n\n  ## client ID, if not set a random ID is generated\n  # client_id = \"\"\n\n  ## Timeout for write operations. default: 5s\n  # timeout = \"5s\"\n\n  ## Optional TLS Config\n  # tls_ca = \"/etc/telegraf/ca.pem\"\n  # tls_cert = \"/etc/telegraf/cert.pem\"\n  # tls_key = \"/etc/telegraf/key.pem\"\n  ## Use TLS but skip chain \u0026 host verification\n  # insecure_skip_verify = false\n\n  ## When true, metrics will be sent in one MQTT message per flush.  Otherwise,\n  ## metrics are written one metric per MQTT message.\n  # batch = false\n\n  ## When true, metric will have RETAIN flag set, making broker cache entries until someone\n  ## actually reads it\n  # retain = false\n\n  ## Data format to output.\n  ## Each data format has its own unique set of configuration options, read\n  ## more about them here:\n  ## https://github.com/influxdata/telegraf/blob/master/docs/DATA_FORMATS_OUTPUT.md\n  data_format = \"influx\"\n\n"
    },
    {
      "type": "output",
      "name": "cloud_pubsub",
      "description": "Publish Telegraf metrics to a Google Cloud PubSub topic",
      "config": "# Publish Telegraf metrics to a Google Cloud PubSub topic\n[[outputs.cloud_pubsub]]\n  # alias=\"cloud_pubsub\"\n  ## Required. Name of Google Cloud Platform (GCP) Project that owns\n  ## the given PubSub topic.\n  project = \"my-project\"\n\n  ## Required. Name of PubSub topic to publish metrics to.\n  topic = \"my-topic\"\n\n  ## Required. Data format to consume.\n  ## Each data format has its own unique set of configuration options.\n  ## Read more about them here:\n  ## https://github.com/influxdata/telegraf/blob/master/docs/DATA_FORMATS_INPUT.md\n  data_format = \"influx\"\n\n  ## Optional. Filepath for GCP credentials JSON file to authorize calls to\n  ## PubSub APIs. If not set explicitly, Telegraf will attempt to use\n  ## Application Default Credentials, which is preferred.\n  # credentials_file = \"path/to/my/creds.json\"\n\n  ## Optional. If true, will send all metrics per write in one PubSub message.\n  # send_batched = true\n\n  ## The following publish_* parameters specifically configures batching\n  ## requests made to the GCP Cloud PubSub API via the PubSub Golang library. Read\n  ## more here: https://godoc.org/cloud.google.com/go/pubsub#PublishSettings\n\n  ## Optional. Send a request to PubSub (i.e. actually publish a batch)\n  ## when it has this many PubSub messages. If send_batched is true,\n  ## this is ignored and treated as if it were 1.\n  # publish_count_threshold = 1000\n\n  ## Optional. Send a request to PubSub (i.e. actually publish a batch)\n  ## when it has this many PubSub messages. If send_batched is true,\n  ## this is ignored and treated as if it were 1\n  # publish_byte_threshold = 1000000\n\n  ## Optional. Specifically configures requests made to the PubSub API.\n  # publish_num_go_routines = 2\n\n  ## Optional. Specifies a timeout for requests to the PubSub API.\n  # publish_timeout = \"30s\"\n\n  ## Optional. If true, published PubSub message data will be base64-encoded.\n  # base64_data = false\n\n  ## Optional. PubSub attributes to add to metrics.\n  # [[inputs.pubsub.attributes]]\n  #   my_attr = \"tag_value\"\n\n"
    },
    {
      "type": "output",
      "name": "influxdb_v2",
      "description": "Configuration for sending metrics to InfluxDB",
      "config": "# Configuration for sending metrics to InfluxDB\n[[outputs.influxdb_v2]]\n  # alias=\"influxdb_v2\"\n  ## The URLs of the InfluxDB cluster nodes.\n  ##\n  ## Multiple URLs can be specified for a single cluster, only ONE of the\n  ## urls will be written to each interval.\n  ##   ex: urls = [\"https://us-west-2-1.aws.cloud2.influxdata.com\"]\n  urls = [\"http://127.0.0.1:9999\"]\n\n  ## Token for authentication.\n  token = \"\"\n\n  ## Organization is the name of the organization you wish to write to; must exist.\n  organization = \"\"\n\n  ## Destination bucket to write into.\n  bucket = \"\"\n\n  ## The value of this tag will be used to determine the bucket.  If this\n  ## tag is not set the 'bucket' option is used as the default.\n  # bucket_tag = \"\"\n\n  ## If true, the bucket tag will not be added to the metric.\n  # exclude_bucket_tag = false\n\n  ## Timeout for HTTP messages.\n  # timeout = \"5s\"\n\n  ## Additional HTTP headers\n  # http_headers = {\"X-Special-Header\" = \"Special-Value\"}\n\n  ## HTTP Proxy override, if unset values the standard proxy environment\n  ## variables are consulted to determine which proxy, if any, should be used.\n  # http_proxy = \"http://corporate.proxy:3128\"\n\n  ## HTTP User-Agent\n  # user_agent = \"telegraf\"\n\n  ## Content-Encoding for write request body, can be set to \"gzip\" to\n  ## compress body or \"identity\" to apply no encoding.\n  # content_encoding = \"gzip\"\n\n  ## Enable or disable uint support for writing uints influxdb 2.0.\n  # influx_uint_support = false\n\n  ## Optional TLS Config for use on HTTP connections.\n  # tls_ca = \"/etc/telegraf/ca.pem\"\n  # tls_cert = \"/etc/telegraf/cert.pem\"\n  # tls_key = \"/etc/telegraf/key.pem\"\n  ## Use TLS but skip chain \u0026 host verification\n  # insecure_skip_verify = false\n\n"
    },
    {
      "type": "output",
      "name": "cratedb",
      "description": "Configuration for CrateDB to send metrics to.",
      "config": "# Configuration for CrateDB to send metrics to.\n[[outputs.cratedb]]\n  # alias=\"cratedb\"\n  # A github.com/jackc/pgx connection string.\n  # See https://godoc.org/github.com/jackc/pgx#ParseDSN\n  url = \"postgres://user:password@localhost/schema?sslmode=disable\"\n  # Timeout for all CrateDB queries.\n  timeout = \"5s\"\n  # Name of the table to store metrics in.\n  table = \"metrics\"\n  # If true, and the metrics table does not exist, create it automatically.\n  table_create = true\n\n"
    },
    {
      "type": "output",
      "name": "kafka",
      "description": "Configuration for the Kafka server to send metrics to",
      "config": "# Configuration for the Kafka server to send metrics to\n[[outputs.kafka]]\n  # alias=\"kafka\"\n  ## URLs of kafka brokers\n  brokers = [\"localhost:9092\"]\n  ## Kafka topic for producer messages\n  topic = \"telegraf\"\n\n  ## Optional Client id\n  # client_id = \"Telegraf\"\n\n  ## Set the minimal supported Kafka version.  Setting this enables the use of new\n  ## Kafka features and APIs.  Of particular interest, lz4 compression\n  ## requires at least version 0.10.0.0.\n  ##   ex: version = \"1.1.0\"\n  # version = \"\"\n\n  ## Optional topic suffix configuration.\n  ## If the section is omitted, no suffix is used.\n  ## Following topic suffix methods are supported:\n  ##   measurement - suffix equals to separator + measurement's name\n  ##   tags        - suffix equals to separator + specified tags' values\n  ##                 interleaved with separator\n\n  ## Suffix equals to \"_\" + measurement name\n  # [outputs.kafka.topic_suffix]\n  #   method = \"measurement\"\n  #   separator = \"_\"\n\n  ## Suffix equals to \"__\" + measurement's \"foo\" tag value.\n  ##   If there's no such a tag, suffix equals to an empty string\n  # [outputs.kafka.topic_suffix]\n  #   method = \"tags\"\n  #   keys = [\"foo\"]\n  #   separator = \"__\"\n\n  ## Suffix equals to \"_\" + measurement's \"foo\" and \"bar\"\n  ##   tag values, separated by \"_\". If there is no such tags,\n  ##   their values treated as empty strings.\n  # [outputs.kafka.topic_suffix]\n  #   method = \"tags\"\n  #   keys = [\"foo\", \"bar\"]\n  #   separator = \"_\"\n\n  ## Telegraf tag to use as a routing key\n  ##  ie, if this tag exists, its value will be used as the routing key\n  routing_tag = \"host\"\n\n  ## Static routing key.  Used when no routing_tag is set or as a fallback\n  ## when the tag specified in routing tag is not found.  If set to \"random\",\n  ## a random value will be generated for each message.\n  ##   ex: routing_key = \"random\"\n  ##       routing_key = \"telegraf\"\n  # routing_key = \"\"\n\n  ## CompressionCodec represents the various compression codecs recognized by\n  ## Kafka in messages.\n  ##  0 : No compression\n  ##  1 : Gzip compression\n  ##  2 : Snappy compression\n  ##  3 : LZ4 compression\n  # compression_codec = 0\n\n  ##  RequiredAcks is used in Produce Requests to tell the broker how many\n  ##  replica acknowledgements it must see before responding\n  ##   0 : the producer never waits for an acknowledgement from the broker.\n  ##       This option provides the lowest latency but the weakest durability\n  ##       guarantees (some data will be lost when a server fails).\n  ##   1 : the producer gets an acknowledgement after the leader replica has\n  ##       received the data. This option provides better durability as the\n  ##       client waits until the server acknowledges the request as successful\n  ##       (only messages that were written to the now-dead leader but not yet\n  ##       replicated will be lost).\n  ##   -1: the producer gets an acknowledgement after all in-sync replicas have\n  ##       received the data. This option provides the best durability, we\n  ##       guarantee that no messages will be lost as long as at least one in\n  ##       sync replica remains.\n  # required_acks = -1\n\n  ## The maximum number of times to retry sending a metric before failing\n  ## until the next flush.\n  # max_retry = 3\n\n  ## The maximum permitted size of a message. Should be set equal to or\n  ## smaller than the broker's 'message.max.bytes'.\n  # max_message_bytes = 1000000\n\n  ## Optional TLS Config\n  # tls_ca = \"/etc/telegraf/ca.pem\"\n  # tls_cert = \"/etc/telegraf/cert.pem\"\n  # tls_key = \"/etc/telegraf/key.pem\"\n  ## Use TLS but skip chain \u0026 host verification\n  # insecure_skip_verify = false\n\n  ## Optional SASL Config\n  # sasl_username = \"kafka\"\n  # sasl_password = \"secret\"\n\n  ## Data format to output.\n  ## Each data format has its own unique set of configuration options, read\n  ## more about them here:\n  ## https://github.com/influxdata/telegraf/blob/master/docs/DATA_FORMATS_OUTPUT.md\n  # data_format = \"influx\"\n\n"
    },
    {
      "type": "output",
      "name": "librato",
      "description": "Configuration for Librato API to send metrics to.",
      "config": "# Configuration for Librato API to send metrics to.\n[[outputs.librato]]\n  # alias=\"librato\"\n  ## Librator API Docs\n  ## http://dev.librato.com/v1/metrics-authentication\n  ## Librato API user\n  api_user = \"telegraf@influxdb.com\" # required.\n  ## Librato API token\n  api_token = \"my-secret-token\" # required.\n  ## Debug\n  # debug = false\n  ## Connection timeout.\n  # timeout = \"5s\"\n  ## Output source Template (same as graphite buckets)\n  ## see https://github.com/influxdata/telegraf/blob/master/docs/DATA_FORMATS_OUTPUT.md#graphite\n  ## This template is used in librato's source (not metric's name)\n  template = \"host\"\n\n\n"
    },
    {
      "type": "output",
      "name": "elasticsearch",
      "description": "Configuration for Elasticsearch to send metrics to.",
      "config": "# Configuration for Elasticsearch to send metrics to.\n[[outputs.elasticsearch]]\n  # alias=\"elasticsearch\"\n  ## The full HTTP endpoint URL for your Elasticsearch instance\n  ## Multiple urls can be specified as part of the same cluster,\n  ## this means that only ONE of the urls will be written to each interval.\n  urls = [ \"http://node1.es.example.com:9200\" ] # required.\n  ## Elasticsearch client timeout, defaults to \"5s\" if not set.\n  timeout = \"5s\"\n  ## Set to true to ask Elasticsearch a list of all cluster nodes,\n  ## thus it is not necessary to list all nodes in the urls config option.\n  enable_sniffer = false\n  ## Set the interval to check if the Elasticsearch nodes are available\n  ## Setting to \"0s\" will disable the health check (not recommended in production)\n  health_check_interval = \"10s\"\n  ## HTTP basic authentication details\n  # username = \"telegraf\"\n  # password = \"mypassword\"\n\n  ## Index Config\n  ## The target index for metrics (Elasticsearch will create if it not exists).\n  ## You can use the date specifiers below to create indexes per time frame.\n  ## The metric timestamp will be used to decide the destination index name\n  # %Y - year (2016)\n  # %y - last two digits of year (00..99)\n  # %m - month (01..12)\n  # %d - day of month (e.g., 01)\n  # %H - hour (00..23)\n  # %V - week of the year (ISO week) (01..53)\n  ## Additionally, you can specify a tag name using the notation {{tag_name}}\n  ## which will be used as part of the index name. If the tag does not exist,\n  ## the default tag value will be used.\n  # index_name = \"telegraf-{{host}}-%Y.%m.%d\"\n  # default_tag_value = \"none\"\n  index_name = \"telegraf-%Y.%m.%d\" # required.\n\n  ## Optional TLS Config\n  # tls_ca = \"/etc/telegraf/ca.pem\"\n  # tls_cert = \"/etc/telegraf/cert.pem\"\n  # tls_key = \"/etc/telegraf/key.pem\"\n  ## Use TLS but skip chain \u0026 host verification\n  # insecure_skip_verify = false\n\n  ## Template Config\n  ## Set to true if you want telegraf to manage its index template.\n  ## If enabled it will create a recommended index template for telegraf indexes\n  manage_template = true\n  ## The template name used for telegraf indexes\n  template_name = \"telegraf\"\n  ## Set to true if you want telegraf to overwrite an existing template\n  overwrite_template = false\n\n"
    },
    {
      "type": "output",
      "name": "instrumental",
      "description": "Configuration for sending metrics to an Instrumental project",
      "config": "# Configuration for sending metrics to an Instrumental project\n[[outputs.instrumental]]\n  # alias=\"instrumental\"\n  ## Project API Token (required)\n  api_token = \"API Token\" # required\n  ## Prefix the metrics with a given name\n  prefix = \"\"\n  ## Stats output template (Graphite formatting)\n  ## see https://github.com/influxdata/telegraf/blob/master/docs/DATA_FORMATS_OUTPUT.md#graphite\n  template = \"host.tags.measurement.field\"\n  ## Timeout in seconds to connect\n  timeout = \"2s\"\n  ## Display Communcation to Instrumental\n  debug = false\n\n"
    }
  ]
}
`
var availableProcessors = `{
  "version": "1.13.0",
  "os": "linux",
  "plugins": [
    {
      "type": "processor",
      "name": "converter",
      "description": "Convert values to another metric value type",
      "config": "# Convert values to another metric value type\n[[processors.converter]]\n  # alias=\"converter\"\n  ## Tags to convert\n  ##\n  ## The table key determines the target type, and the array of key-values\n  ## select the keys to convert.  The array may contain globs.\n  ##   \u003ctarget-type\u003e = [\u003ctag-key\u003e...]\n  [processors.converter.tags]\n    string = []\n    integer = []\n    unsigned = []\n    boolean = []\n    float = []\n\n  ## Fields to convert\n  ##\n  ## The table key determines the target type, and the array of key-values\n  ## select the keys to convert.  The array may contain globs.\n  ##   \u003ctarget-type\u003e = [\u003cfield-key\u003e...]\n  [processors.converter.fields]\n    tag = []\n    string = []\n    integer = []\n    unsigned = []\n    boolean = []\n    float = []\n\n"
    },
    {
      "type": "processor",
      "name": "override",
      "description": "Apply metric modifications using override semantics.",
      "config": "# Apply metric modifications using override semantics.\n[[processors.override]]\n  # alias=\"override\"\n  ## All modifications on inputs and aggregators can be overridden:\n  # name_override = \"new_name\"\n  # name_prefix = \"new_name_prefix\"\n  # name_suffix = \"new_name_suffix\"\n\n  ## Tags to be added (all values must be strings)\n  # [processors.override.tags]\n  #   additional_tag = \"tag_value\"\n\n"
    },
    {
      "type": "processor",
      "name": "strings",
      "description": "Perform string processing on tags, fields, and measurements",
      "config": "# Perform string processing on tags, fields, and measurements\n[[processors.strings]]\n  # alias=\"strings\"\n  ## Convert a tag value to uppercase\n  # [[processors.strings.uppercase]]\n  #   tag = \"method\"\n\n  ## Convert a field value to lowercase and store in a new field\n  # [[processors.strings.lowercase]]\n  #   field = \"uri_stem\"\n  #   dest = \"uri_stem_normalised\"\n\n  ## Trim leading and trailing whitespace using the default cutset\n  # [[processors.strings.trim]]\n  #   field = \"message\"\n\n  ## Trim leading characters in cutset\n  # [[processors.strings.trim_left]]\n  #   field = \"message\"\n  #   cutset = \"\\t\"\n\n  ## Trim trailing characters in cutset\n  # [[processors.strings.trim_right]]\n  #   field = \"message\"\n  #   cutset = \"\\r\\n\"\n\n  ## Trim the given prefix from the field\n  # [[processors.strings.trim_prefix]]\n  #   field = \"my_value\"\n  #   prefix = \"my_\"\n\n  ## Trim the given suffix from the field\n  # [[processors.strings.trim_suffix]]\n  #   field = \"read_count\"\n  #   suffix = \"_count\"\n\n  ## Replace all non-overlapping instances of old with new\n  # [[processors.strings.replace]]\n  #   measurement = \"*\"\n  #   old = \":\"\n  #   new = \"_\"\n\n  ## Trims strings based on width\n  # [[processors.strings.left]]\n  #   field = \"message\"\n  #   width = 10\n\n  ## Decode a base64 encoded utf-8 string\n  # [[processors.strings.base64decode]]\n  #   field = \"message\"\n\n"
    },
    {
      "type": "processor",
      "name": "tag_limit",
      "description": "Restricts the number of tags that can pass through this filter and chooses which tags to preserve when over the limit.",
      "config": "# Restricts the number of tags that can pass through this filter and chooses which tags to preserve when over the limit.\n[[processors.tag_limit]]\n  # alias=\"tag_limit\"\n  ## Maximum number of tags to preserve\n  limit = 10\n\n  ## List of tags to preferentially preserve\n  keep = [\"foo\", \"bar\", \"baz\"]\n\n"
    },
    {
      "type": "processor",
      "name": "date",
      "description": "Dates measurements, tags, and fields that pass through this filter.",
      "config": "# Dates measurements, tags, and fields that pass through this filter.\n[[processors.date]]\n  # alias=\"date\"\n  ## New tag to create\n  tag_key = \"month\"\n\n  ## Date format string, must be a representation of the Go \"reference time\"\n  ## which is \"Mon Jan 2 15:04:05 -0700 MST 2006\".\n  date_format = \"Jan\"\n\n"
    },
    {
      "type": "processor",
      "name": "parser",
      "description": "Parse a value in a specified field/tag(s) and add the result in a new metric",
      "config": "# Parse a value in a specified field/tag(s) and add the result in a new metric\n[[processors.parser]]\n  # alias=\"parser\"\n  ## The name of the fields whose value will be parsed.\n  parse_fields = []\n\n  ## If true, incoming metrics are not emitted.\n  drop_original = false\n\n  ## If set to override, emitted metrics will be merged by overriding the\n  ## original metric using the newly parsed metrics.\n  merge = \"override\"\n\n  ## The dataformat to be read from files\n  ## Each data format has its own unique set of configuration options, read\n  ## more about them here:\n  ## https://github.com/influxdata/telegraf/blob/master/docs/DATA_FORMATS_INPUT.md\n  data_format = \"influx\"\n\n"
    },
    {
      "type": "processor",
      "name": "pivot",
      "description": "Rotate a single valued metric into a multi field metric",
      "config": "# Rotate a single valued metric into a multi field metric\n[[processors.pivot]]\n  # alias=\"pivot\"\n  ## Tag to use for naming the new field.\n  tag_key = \"name\"\n  ## Field to use as the value of the new field.\n  value_key = \"value\"\n\n"
    },
    {
      "type": "processor",
      "name": "printer",
      "description": "Print all metrics that pass through this filter.",
      "config": "# Print all metrics that pass through this filter.\n[[processors.printer]]\n  # alias=\"printer\"\n\n"
    },
    {
      "type": "processor",
      "name": "clone",
      "description": "Clone metrics and apply modifications.",
      "config": "# Clone metrics and apply modifications.\n[[processors.clone]]\n  # alias=\"clone\"\n  ## All modifications on inputs and aggregators can be overridden:\n  # name_override = \"new_name\"\n  # name_prefix = \"new_name_prefix\"\n  # name_suffix = \"new_name_suffix\"\n\n  ## Tags to be added (all values must be strings)\n  # [processors.clone.tags]\n  #   additional_tag = \"tag_value\"\n\n"
    },
    {
      "type": "processor",
      "name": "enum",
      "description": "Map enum values according to given table.",
      "config": "# Map enum values according to given table.\n[[processors.enum]]\n  # alias=\"enum\"\n  [[processors.enum.mapping]]\n    ## Name of the field to map\n    field = \"status\"\n\n    ## Name of the tag to map\n    # tag = \"status\"\n\n    ## Destination tag or field to be used for the mapped value.  By default the\n    ## source tag or field is used, overwriting the original value.\n    dest = \"status_code\"\n\n    ## Default value to be used for all values not contained in the mapping\n    ## table.  When unset, the unmodified value for the field will be used if no\n    ## match is found.\n    # default = 0\n\n    ## Table of mappings\n    [processors.enum.mapping.value_mappings]\n      green = 1\n      amber = 2\n      red = 3\n\n"
    },
    {
      "type": "processor",
      "name": "rename",
      "description": "Rename measurements, tags, and fields that pass through this filter.",
      "config": "# Rename measurements, tags, and fields that pass through this filter.\n[[processors.rename]]\n  # alias=\"rename\"\n\n"
    },
    {
      "type": "processor",
      "name": "topk",
      "description": "Print all metrics that pass through this filter.",
      "config": "# Print all metrics that pass through this filter.\n[[processors.topk]]\n  # alias=\"topk\"\n  ## How many seconds between aggregations\n  # period = 10\n\n  ## How many top metrics to return\n  # k = 10\n\n  ## Over which tags should the aggregation be done. Globs can be specified, in\n  ## which case any tag matching the glob will aggregated over. If set to an\n  ## empty list is no aggregation over tags is done\n  # group_by = ['*']\n\n  ## Over which fields are the top k are calculated\n  # fields = [\"value\"]\n\n  ## What aggregation to use. Options: sum, mean, min, max\n  # aggregation = \"mean\"\n\n  ## Instead of the top k largest metrics, return the bottom k lowest metrics\n  # bottomk = false\n\n  ## The plugin assigns each metric a GroupBy tag generated from its name and\n  ## tags. If this setting is different than \"\" the plugin will add a\n  ## tag (which name will be the value of this setting) to each metric with\n  ## the value of the calculated GroupBy tag. Useful for debugging\n  # add_groupby_tag = \"\"\n\n  ## These settings provide a way to know the position of each metric in\n  ## the top k. The 'add_rank_field' setting allows to specify for which\n  ## fields the position is required. If the list is non empty, then a field\n  ## will be added to each and every metric for each string present in this\n  ## setting. This field will contain the ranking of the group that\n  ## the metric belonged to when aggregated over that field.\n  ## The name of the field will be set to the name of the aggregation field,\n  ## suffixed with the string '_topk_rank'\n  # add_rank_fields = []\n\n  ## These settings provide a way to know what values the plugin is generating\n  ## when aggregating metrics. The 'add_agregate_field' setting allows to\n  ## specify for which fields the final aggregation value is required. If the\n  ## list is non empty, then a field will be added to each every metric for\n  ## each field present in this setting. This field will contain\n  ## the computed aggregation for the group that the metric belonged to when\n  ## aggregated over that field.\n  ## The name of the field will be set to the name of the aggregation field,\n  ## suffixed with the string '_topk_aggregate'\n  # add_aggregate_fields = []\n\n"
    },
    {
      "type": "processor",
      "name": "regex",
      "description": "Transforms tag and field values with regex pattern",
      "config": "# Transforms tag and field values with regex pattern\n[[processors.regex]]\n  # alias=\"regex\"\n  ## Tag and field conversions defined in a separate sub-tables\n  # [[processors.regex.tags]]\n  #   ## Tag to change\n  #   key = \"resp_code\"\n  #   ## Regular expression to match on a tag value\n  #   pattern = \"^(\\\\d)\\\\d\\\\d$\"\n  #   ## Matches of the pattern will be replaced with this string.  Use ${1}\n  #   ## notation to use the text of the first submatch.\n  #   replacement = \"${1}xx\"\n\n  # [[processors.regex.fields]]\n  #   ## Field to change\n  #   key = \"request\"\n  #   ## All the power of the Go regular expressions available here\n  #   ## For example, named subgroups\n  #   pattern = \"^/api(?P\u003cmethod\u003e/[\\\\w/]+)\\\\S*\"\n  #   replacement = \"${method}\"\n  #   ## If result_key is present, a new field will be created\n  #   ## instead of changing existing field\n  #   result_key = \"method\"\n\n  ## Multiple conversions may be applied for one field sequentially\n  ## Let's extract one more value\n  # [[processors.regex.fields]]\n  #   key = \"request\"\n  #   pattern = \".*category=(\\\\w+).*\"\n  #   replacement = \"${1}\"\n  #   result_key = \"search_category\"\n\n"
    },
    {
      "type": "processor",
      "name": "unpivot",
      "description": "Rotate multi field metric into several single field metrics",
      "config": "# Rotate multi field metric into several single field metrics\n[[processors.unpivot]]\n  # alias=\"unpivot\"\n  ## Tag to use for the name.\n  tag_key = \"name\"\n  ## Field to use for the name of the value.\n  value_key = \"value\"\n\n"
    }
  ]
}
`
var availableAggregators = `{
  "version": "1.13.0",
  "os": "linux",
  "plugins": [
    {
      "type": "aggregator",
      "name": "merge",
      "description": "Merge metrics into multifield metrics by series key",
      "config": "# Merge metrics into multifield metrics by series key\n[[aggregators.merge]]\n  # alias=\"merge\"\n"
    },
    {
      "type": "aggregator",
      "name": "minmax",
      "description": "Keep the aggregate min/max of each metric passing through.",
      "config": "# Keep the aggregate min/max of each metric passing through.\n[[aggregators.minmax]]\n  # alias=\"minmax\"\n  ## General Aggregator Arguments:\n  ## The period on which to flush \u0026 clear the aggregator.\n  period = \"30s\"\n  ## If true, the original metric will be dropped by the\n  ## aggregator and will not get sent to the output plugins.\n  drop_original = false\n\n"
    },
    {
      "type": "aggregator",
      "name": "valuecounter",
      "description": "Count the occurrence of values in fields.",
      "config": "# Count the occurrence of values in fields.\n[[aggregators.valuecounter]]\n  # alias=\"valuecounter\"\n  ## General Aggregator Arguments:\n  ## The period on which to flush \u0026 clear the aggregator.\n  period = \"30s\"\n  ## If true, the original metric will be dropped by the\n  ## aggregator and will not get sent to the output plugins.\n  drop_original = false\n  ## The fields for which the values will be counted\n  fields = []\n\n"
    },
    {
      "type": "aggregator",
      "name": "basicstats",
      "description": "Keep the aggregate basicstats of each metric passing through.",
      "config": "# Keep the aggregate basicstats of each metric passing through.\n[[aggregators.basicstats]]\n  # alias=\"basicstats\"\n  ## The period on which to flush \u0026 clear the aggregator.\n  period = \"30s\"\n\n  ## If true, the original metric will be dropped by the\n  ## aggregator and will not get sent to the output plugins.\n  drop_original = false\n\n  ## Configures which basic stats to push as fields\n  # stats = [\"count\", \"min\", \"max\", \"mean\", \"stdev\", \"s2\", \"sum\"]\n\n"
    },
    {
      "type": "aggregator",
      "name": "final",
      "description": "Report the final metric of a series",
      "config": "# Report the final metric of a series\n[[aggregators.final]]\n  # alias=\"final\"\n  ## The period on which to flush \u0026 clear the aggregator.\n  period = \"30s\"\n  ## If true, the original metric will be dropped by the\n  ## aggregator and will not get sent to the output plugins.\n  drop_original = false\n\n  ## The time that a series is not updated until considering it final.\n  series_timeout = \"5m\"\n\n"
    },
    {
      "type": "aggregator",
      "name": "histogram",
      "description": "Create aggregate histograms.",
      "config": "# Create aggregate histograms.\n[[aggregators.histogram]]\n  # alias=\"histogram\"\n  ## The period in which to flush the aggregator.\n  period = \"30s\"\n\n  ## If true, the original metric will be dropped by the\n  ## aggregator and will not get sent to the output plugins.\n  drop_original = false\n\n  ## If true, the histogram will be reset on flush instead\n  ## of accumulating the results.\n  reset = false\n\n  ## Example config that aggregates all fields of the metric.\n  # [[aggregators.histogram.config]]\n  #   ## The set of buckets.\n  #   buckets = [0.0, 15.6, 34.5, 49.1, 71.5, 80.5, 94.5, 100.0]\n  #   ## The name of metric.\n  #   measurement_name = \"cpu\"\n\n  ## Example config that aggregates only specific fields of the metric.\n  # [[aggregators.histogram.config]]\n  #   ## The set of buckets.\n  #   buckets = [0.0, 10.0, 20.0, 30.0, 40.0, 50.0, 60.0, 70.0, 80.0, 90.0, 100.0]\n  #   ## The name of metric.\n  #   measurement_name = \"diskio\"\n  #   ## The concrete fields of metric\n  #   fields = [\"io_time\", \"read_time\", \"write_time\"]\n\n"
    }
  ]
}
`
