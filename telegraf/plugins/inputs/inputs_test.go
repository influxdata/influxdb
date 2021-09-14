package inputs

import (
	"errors"
	"reflect"
	"testing"

	"github.com/influxdata/influxdb/v2/telegraf/plugins"
)

// local plugin
type telegrafPluginConfig interface {
	TOML() string
	Type() plugins.Type
	PluginName() string
	UnmarshalTOML(data interface{}) error
}

func TestType(t *testing.T) {
	b := baseInput(0)
	if b.Type() != plugins.Input {
		t.Fatalf("input plugins type should be input, got %s", b.Type())
	}
}

func TestEncodeTOML(t *testing.T) {
	cases := []struct {
		name    string
		plugins map[telegrafPluginConfig]string
	}{
		{
			name: "test empty plugins",
			plugins: map[telegrafPluginConfig]string{
				&CPUStats{}: `[[inputs.cpu]]
  ## Whether to report per-cpu stats or not
  percpu = true
  ## Whether to report total system cpu stats or not
  totalcpu = true
  ## If true, collect raw CPU time metrics
  collect_cpu_time = false
  ## If true, compute and report the sum of all non-idle CPU states
  report_active = false
`,
				&DiskStats{}: `[[inputs.disk]]
  ## By default stats will be gathered for all mount points.
  ## Set mount_points will restrict the stats to only the specified mount points.
  # mount_points = ["/"]
  ## Ignore mount points by filesystem type.
  ignore_fs = ["tmpfs", "devtmpfs", "devfs", "iso9660", "overlay", "aufs", "squashfs"]
`,
				&DiskIO{}: `[[inputs.diskio]]
  ## By default, telegraf will gather stats for all devices including
  ## disk partitions.
  ## Setting devices will restrict the stats to the specified devices.
  # devices = ["sda", "sdb", "vd*"]
  ## Uncomment the following line if you need disk serial numbers.
  # skip_serial_number = false
  #
  ## On systems which support it, device metadata can be added in the form of
  ## tags.
  ## Currently only Linux is supported via udev properties. You can view
  ## available properties for a device by running:
  ## 'udevadm info -q property -n /dev/sda'
  ## Note: Most, but not all, udev properties can be accessed this way. Properties
  ## that are currently inaccessible include DEVTYPE, DEVNAME, and DEVPATH.
  # device_tags = ["ID_FS_TYPE", "ID_FS_USAGE"]
  #
  ## Using the same metadata source as device_tags, you can also customize the
  ## name of the device via templates.
  ## The 'name_templates' parameter is a list of templates to try and apply to
  ## the device. The template may contain variables in the form of '$PROPERTY' or
  ## '${PROPERTY}'. The first template which does not contain any variables not
  ## present for the device is used as the device name tag.
  ## The typical use case is for LVM volumes, to get the VG/LV name instead of
  ## the near-meaningless DM-0 name.
  # name_templates = ["$ID_FS_LABEL","$DM_VG_NAME/$DM_LV_NAME"]
`,
				&Docker{}: `[[inputs.docker]]
  ## Docker Endpoint
  ##   To use TCP, set endpoint = "tcp://[ip]:[port]"
  ##   To use environment variables (ie, docker-machine), set endpoint = "ENV"
  endpoint = ""
  #
  ## Set to true to collect Swarm metrics(desired_replicas, running_replicas)
  gather_services = false
  #
  ## Only collect metrics for these containers, collect all if empty
  container_names = []
  #
  ## Set the source tag for the metrics to the container ID hostname, eg first 12 chars
  source_tag = false
  #
  ## Containers to include and exclude. Globs accepted.
  ## Note that an empty array for both will include all containers
  container_name_include = []
  container_name_exclude = []
  #
  ## Container states to include and exclude. Globs accepted.
  ## When empty only containers in the "running" state will be captured.
  ## example: container_state_include = ["created", "restarting", "running", "removing", "paused", "exited", "dead"]
  ## example: container_state_exclude = ["created", "restarting", "running", "removing", "paused", "exited", "dead"]
  # container_state_include = []
  # container_state_exclude = []
  #
  ## Timeout for docker list, info, and stats commands
  timeout = "5s"
  #
  ## Whether to report for each container per-device blkio (8:0, 8:1...),
  ## network (eth0, eth1, ...) and cpu (cpu0, cpu1, ...) stats or not.
  ## Usage of this setting is discouraged since it will be deprecated in favor of 'perdevice_include'.
  ## Default value is 'true' for backwards compatibility, please set it to 'false' so that 'perdevice_include' setting
  ## is honored.
  perdevice = true
  #
  ## Specifies for which classes a per-device metric should be issued
  ## Possible values are 'cpu' (cpu0, cpu1, ...), 'blkio' (8:0, 8:1, ...) and 'network' (eth0, eth1, ...)
  ## Please note that this setting has no effect if 'perdevice' is set to 'true'
  # perdevice_include = ["cpu"]
  #
  ## Whether to report for each container total blkio and network stats or not.
  ## Usage of this setting is discouraged since it will be deprecated in favor of 'total_include'.
  ## Default value is 'false' for backwards compatibility, please set it to 'true' so that 'total_include' setting
  ## is honored.
  total = false
  #
  ## Specifies for which classes a total metric should be issued. Total is an aggregated of the 'perdevice' values.
  ## Possible values are 'cpu', 'blkio' and 'network'
  ## Total 'cpu' is reported directly by Docker daemon, and 'network' and 'blkio' totals are aggregated by this plugin.
  ## Please note that this setting has no effect if 'total' is set to 'false'
  # total_include = ["cpu", "blkio", "network"]
  #
  ## Which environment variables should we use as a tag
  ##tag_env = ["JAVA_HOME", "HEAP_SIZE"]
  #
  ## docker labels to include and exclude as tags.  Globs accepted.
  ## Note that an empty array for both will include all labels as tags
  docker_label_include = []
  docker_label_exclude = []
  #
  ## Optional TLS Config
  # tls_ca = "/etc/telegraf/ca.pem"
  # tls_cert = "/etc/telegraf/cert.pem"
  # tls_key = "/etc/telegraf/key.pem"
  ## Use TLS but skip chain & host verification
  # insecure_skip_verify = false
`,
				&File{}: `[[inputs.file]]
  ## Files to parse each interval.  Accept standard unix glob matching rules,
  ## as well as ** to match recursive files and directories.
  files = []

  ## Name a tag containing the name of the file the data was parsed from.  Leave empty
  ## to disable.
  # file_tag = ""

  ## Character encoding to use when interpreting the file contents.  Invalid
  ## characters are replaced using the unicode replacement character.  When set
  ## to the empty string the data is not decoded to text.
  ##   ex: character_encoding = "utf-8"
  ##       character_encoding = "utf-16le"
  ##       character_encoding = "utf-16be"
  ##       character_encoding = ""
  # character_encoding = ""

  ## The dataformat to be read from files
  ## Each data format has its own unique set of configuration options, read
  ## more about them here:
  ## https://github.com/influxdata/telegraf/blob/master/docs/DATA_FORMATS_INPUT.md
  data_format = "influx"
`,
				&Kernel{}: `[[inputs.kernel]]
  # no configuration
`,
				&Kubernetes{}: `[[inputs.kubernetes]]
  ## URL for the kubelet
  url = ""

  ## Use bearer token for authorization. ('bearer_token' takes priority)
  ## If both of these are empty, we'll use the default serviceaccount:
  ## at: /run/secrets/kubernetes.io/serviceaccount/token
  # bearer_token = "/path/to/bearer/token"
  ## OR
  # bearer_token_string = "abc_123"

  ## Pod labels to be added as tags.  An empty array for both include and
  ## exclude will include all labels.
  # label_include = []
  # label_exclude = ["*"]

  ## Set response_timeout (default 5 seconds)
  # response_timeout = "5s"

  ## Optional TLS Config
  # tls_ca = /path/to/cafile
  # tls_cert = /path/to/certfile
  # tls_key = /path/to/keyfile
  ## Use TLS but skip chain & host verification
  # insecure_skip_verify = false
`,
				&LogParserPlugin{}: `[[inputs.logparser]]
  ## Log files to parse.
  ## These accept standard unix glob matching rules, but with the addition of
  ## ** as a "super asterisk". ie:
  ##   /var/log/**.log     -> recursively find all .log files in /var/log
  ##   /var/log/*/*.log    -> find all .log files with a parent dir in /var/log
  ##   /var/log/apache.log -> only tail the apache log file
  files = []

  ## Read files that currently exist from the beginning. Files that are created
  ## while telegraf is running (and that match the "files" globs) will always
  ## be read from the beginning.
  from_beginning = false

  ## Method used to watch for file updates.  Can be either "inotify" or "poll".
  # watch_method = "inotify"

  ## Parse logstash-style "grok" patterns:
  [inputs.logparser.grok]
    ## This is a list of patterns to check the given log file(s) for.
    ## Note that adding patterns here increases processing time. The most
    ## efficient configuration is to have one pattern per logparser.
    ## Other common built-in patterns are:
    ##   %{COMMON_LOG_FORMAT}   (plain apache & nginx access logs)
    ##   %{COMBINED_LOG_FORMAT} (access logs + referrer & agent)
    patterns = ["%{COMBINED_LOG_FORMAT}"]

    ## Name of the outputted measurement name.
    measurement = "apache_access_log"

    ## Full path(s) to custom pattern files.
    custom_pattern_files = []

    ## Custom patterns can also be defined here. Put one pattern per line.
    custom_patterns = '''
    '''

    ## Timezone allows you to provide an override for timestamps that
    ## don't already include an offset
    ## e.g. 04/06/2016 12:41:45 data one two 5.43µs
    ##
    ## Default: "" which renders UTC
    ## Options are as follows:
    ##   1. Local             -- interpret based on machine localtime
    ##   2. "Canada/Eastern"  -- Unix TZ values like those found in https://en.wikipedia.org/wiki/List_of_tz_database_time_zones
    ##   3. UTC               -- or blank/unspecified, will return timestamp in UTC
    # timezone = "Canada/Eastern"

      ## When set to "disable", timestamp will not incremented if there is a
      ## duplicate.
    # unique_timestamp = "auto"
`,
				&MemStats{}:   `[[inputs.mem]]
  # no configuration
`,
				&NetIOStats{}: `[[inputs.net]]
  ## By default, telegraf gathers stats from any up interface (excluding loopback)
  ## Setting interfaces will tell it to gather these explicit interfaces,
  ## regardless of status.
  ##
  # interfaces = ["eth0"]
  ##
  ## On linux systems telegraf also collects protocol stats.
  ## Setting ignore_protocol_stats to true will skip reporting of protocol metrics.
  ##
  # ignore_protocol_stats = false
  ##
`,
				&NetResponse{}: `[[inputs.net_response]]
  ## Protocol, must be "tcp" or "udp"
  ## NOTE: because the "udp" protocol does not respond to requests, it requires
  ## a send/expect string pair (see below).
  protocol = "tcp"
  ## Server address (default localhost)
  address = "localhost:80"

  ## Set timeout
  # timeout = "1s"

  ## Set read timeout (only used if expecting a response)
  # read_timeout = "1s"

  ## The following options are required for UDP checks. For TCP, they are
  ## optional. The plugin will send the given string to the server and then
  ## expect to receive the given 'expect' string back.
  ## string sent to the server
  # send = "ssh"
  ## expected string in answer
  # expect = "ssh"

  ## Uncomment to remove deprecated fields
  # fielddrop = ["result_type", "string_found"]
`,
				&Nginx{}: `[[inputs.nginx]]
  # An array of Nginx stub_status URI to gather stats.
  urls = []

  ## Optional TLS Config
  tls_ca = "/etc/telegraf/ca.pem"
  tls_cert = "/etc/telegraf/cert.cer"
  tls_key = "/etc/telegraf/key.key"
  ## Use TLS but skip chain & host verification
  insecure_skip_verify = false

  # HTTP response timeout (default: 5s)
  response_timeout = "5s"
`,
				&Processes{}: `[[inputs.processes]]
  # no configuration
`,
				&Procstat{}: `[[inputs.procstat]]
  ## PID file to monitor process
  pid_file = "/var/run/nginx.pid"
  ## executable name (ie, pgrep <exe>)
  # exe = ""
  ## pattern as argument for pgrep (ie, pgrep -f <pattern>)
  # pattern = "nginx"
  ## user as argument for pgrep (ie, pgrep -u <user>)
  # user = "nginx"
  ## Systemd unit name
  # systemd_unit = "nginx.service"
  ## CGroup name or path
  # cgroup = "systemd/system.slice/nginx.service"

  ## Windows service name
  # win_service = ""

  ## override for process_name
  ## This is optional; default is sourced from /proc/<pid>/status
  # process_name = "bar"

  ## Field name prefix
  # prefix = ""

  ## When true add the full cmdline as a tag.
  # cmdline_tag = false

  ## Mode to use when calculating CPU usage. Can be one of 'solaris' or 'irix'.
  # mode = "irix"

  ## Add the PID as a tag instead of as a field.  When collecting multiple
  ## processes with otherwise matching tags this setting should be enabled to
  ## ensure each process has a unique identity.
  ##
  ## Enabling this option may result in a large number of series, especially
  ## when processes have a short lifetime.
  # pid_tag = false

  ## Method to use when finding process IDs.  Can be one of 'pgrep', or
  ## 'native'.  The pgrep finder calls the pgrep executable in the PATH while
  ## the native finder performs the search directly in a manor dependent on the
  ## platform.  Default is 'pgrep'
  # pid_finder = "pgrep"
`,
				&Prometheus{}: `[[inputs.prometheus]]
  ## An array of urls to scrape metrics from.
  urls = []

  ## Metric version controls the mapping from Prometheus metrics into
  ## Telegraf metrics.  When using the prometheus_client output, use the same
  ## value in both plugins to ensure metrics are round-tripped without
  ## modification.
  ##
  ##   example: metric_version = 1;
  ##            metric_version = 2; recommended version
  # metric_version = 1

  ## Url tag name (tag containing scrapped url. optional, default is "url")
  # url_tag = "url"

  ## An array of Kubernetes services to scrape metrics from.
  # kubernetes_services = ["http://my-service-dns.my-namespace:9100/metrics"]

  ## Kubernetes config file to create client from.
  # kube_config = "/path/to/kubernetes.config"

  ## Scrape Kubernetes pods for the following prometheus annotations:
  ## - prometheus.io/scrape: Enable scraping for this pod
  ## - prometheus.io/scheme: If the metrics endpoint is secured then you will need to
  ##     set this to 'https' & most likely set the tls config.
  ## - prometheus.io/path: If the metrics path is not /metrics, define it with this annotation.
  ## - prometheus.io/port: If port is not 9102 use this annotation
  # monitor_kubernetes_pods = true
  ## Get the list of pods to scrape with either the scope of
  ## - cluster: the kubernetes watch api (default, no need to specify)
  ## - node: the local cadvisor api; for scalability. Note that the config node_ip or the environment variable NODE_IP must be set to the host IP.
  # pod_scrape_scope = "cluster"
  ## Only for node scrape scope: node IP of the node that telegraf is running on.
  ## Either this config or the environment variable NODE_IP must be set.
  # node_ip = "10.180.1.1"
  # 	## Only for node scrape scope: interval in seconds for how often to get updated pod list for scraping.
  # 	## Default is 60 seconds.
  # 	# pod_scrape_interval = 60
  ## Restricts Kubernetes monitoring to a single namespace
  ##   ex: monitor_kubernetes_pods_namespace = "default"
  # monitor_kubernetes_pods_namespace = ""
  # label selector to target pods which have the label
  # kubernetes_label_selector = "env=dev,app=nginx"
  # field selector to target pods
  # eg. To scrape pods on a specific node
  # kubernetes_field_selector = "spec.nodeName=$HOSTNAME"

  ## Use bearer token for authorization. ('bearer_token' takes priority)
  # bearer_token = "/path/to/bearer/token"
  ## OR
  # bearer_token_string = "abc_123"

  ## HTTP Basic Authentication username and password. ('bearer_token' and
  ## 'bearer_token_string' take priority)
  # username = ""
  # password = ""

  ## Specify timeout duration for slower prometheus clients (default is 3s)
  # response_timeout = "3s"

  ## Optional TLS Config
  # tls_ca = /path/to/cafile
  # tls_cert = /path/to/certfile
  # tls_key = /path/to/keyfile
  ## Use TLS but skip chain & host verification
  # insecure_skip_verify = false
`,
				&Redis{}: `[[inputs.redis]]
  ## specify servers via a url matching:
  ##  [protocol://][:password]@address[:port]
  ##  e.g.
  ##    tcp://localhost:6379
  ##    tcp://:password@192.168.99.100
  ##    unix:///var/run/redis.sock
  ##
  ## If no servers are specified, then localhost is used as the host.
  ## If no port is specified, 6379 is used
  servers = []

  ## Optional. Specify redis commands to retrieve values
  # [[inputs.redis.commands]]
  #   # The command to run where each argument is a separate element
  #   command = ["get", "sample-key"]
  #   # The field to store the result in
  #   field = "sample-key-value"
  #   # The type of the result
  #   # Can be "string", "integer", or "float"
  #   type = "string"

  ## specify server password
  # password = ""

  ## Optional TLS Config
  # tls_ca = "/etc/telegraf/ca.pem"
  # tls_cert = "/etc/telegraf/cert.pem"
  # tls_key = "/etc/telegraf/key.pem"
  ## Use TLS but skip chain & host verification
  # insecure_skip_verify = true
`,
				&SwapStats{}: `[[inputs.swap]]
  # no configuration
`,
				&Syslog{}: `[[inputs.syslog]]
  ## Specify an ip or hostname with port - eg., tcp://localhost:6514, tcp://10.0.0.1:6514
  ## Protocol, address and port to host the syslog receiver.
  ## If no host is specified, then localhost is used.
  ## If no port is specified, 6514 is used (RFC5425#section-4.1).
  server = ""

  ## TLS Config
  # tls_allowed_cacerts = ["/etc/telegraf/ca.pem"]
  # tls_cert = "/etc/telegraf/cert.pem"
  # tls_key = "/etc/telegraf/key.pem"

  ## Period between keep alive probes.
  ## 0 disables keep alive probes.
  ## Defaults to the OS configuration.
  ## Only applies to stream sockets (e.g. TCP).
  # keep_alive_period = "5m"

  ## Maximum number of concurrent connections (default = 0).
  ## 0 means unlimited.
  ## Only applies to stream sockets (e.g. TCP).
  # max_connections = 1024

  ## Read timeout is the maximum time allowed for reading a single message (default = 5s).
  ## 0 means unlimited.
  # read_timeout = "5s"

  ## The framing technique with which it is expected that messages are transported (default = "octet-counting").
  ## Whether the messages come using the octect-counting (RFC5425#section-4.3.1, RFC6587#section-3.4.1),
  ## or the non-transparent framing technique (RFC6587#section-3.4.2).
  ## Must be one of "octet-counting", "non-transparent".
  # framing = "octet-counting"

  ## The trailer to be expected in case of non-transparent framing (default = "LF").
  ## Must be one of "LF", or "NUL".
  # trailer = "LF"

  ## Whether to parse in best effort mode or not (default = false).
  ## By default best effort parsing is off.
  # best_effort = false

  ## Character to prepend to SD-PARAMs (default = "_").
  ## A syslog message can contain multiple parameters and multiple identifiers within structured data section.
  ## Eg., [id1 name1="val1" name2="val2"][id2 name1="val1" nameA="valA"]
  ## For each combination a field is created.
  ## Its name is created concatenating identifier, sdparam_separator, and parameter name.
  # sdparam_separator = "_"
`,
				&SystemStats{}: `[[inputs.system]]
  ## Uncomment to remove deprecated metrics.
  # fielddrop = ["uptime_format"]
`,
				&Tail{}: `[[inputs.tail]]
  ## File names or a pattern to tail.
  ## These accept standard unix glob matching rules, but with the addition of
  ## ** as a "super asterisk". ie:
  ##   "/var/log/**.log"  -> recursively find all .log files in /var/log
  ##   "/var/log/*/*.log" -> find all .log files with a parent dir in /var/log
  ##   "/var/log/apache.log" -> just tail the apache log file
  ##   "/var/log/log[!1-2]*  -> tail files without 1-2
  ##   "/var/log/log[^1-2]*  -> identical behavior as above
  ## See https://github.com/gobwas/glob for more examples
  ##
  files = []

  ## Read file from beginning.
  # from_beginning = false

  ## Whether file is a named pipe
  # pipe = false

  ## Method used to watch for file updates.  Can be either "inotify" or "poll".
  # watch_method = "inotify"

  ## Maximum lines of the file to process that have not yet be written by the
  ## output.  For best throughput set based on the number of metrics on each
  ## line and the size of the output's metric_batch_size.
  # max_undelivered_lines = 1000

  ## Character encoding to use when interpreting the file contents.  Invalid
  ## characters are replaced using the unicode replacement character.  When set
  ## to the empty string the data is not decoded to text.
  ##   ex: character_encoding = "utf-8"
  ##       character_encoding = "utf-16le"
  ##       character_encoding = "utf-16be"
  ##       character_encoding = ""
  # character_encoding = ""

  ## Data format to consume.
  ## Each data format has its own unique set of configuration options, read
  ## more about them here:
  ## https://github.com/influxdata/telegraf/blob/master/docs/DATA_FORMATS_INPUT.md
  data_format = "influx"

  ## Set the tag that will contain the path of the tailed file. If you don't want this tag, set it to an empty string.
  # path_tag = "path"

  ## multiline parser/codec
  ## https://www.elastic.co/guide/en/logstash/2.4/plugins-filters-multiline.html
  #[inputs.tail.multiline]
    ## The pattern should be a regexp which matches what you believe to be an
    ## indicator that the field is part of an event consisting of multiple lines of log data.
    #pattern = "^\s"

    ## This field must be either "previous" or "next".
    ## If a line matches the pattern, "previous" indicates that it belongs to the previous line,
    ## whereas "next" indicates that the line belongs to the next one.
    #match_which_line = "previous"

    ## The invert_match field can be true or false (defaults to false).
    ## If true, a message not matching the pattern will constitute a match of the multiline
    ## filter and the what will be applied. (vice-versa is also true)
    #invert_match = false

    ## After the specified timeout, this plugin sends a multiline event even if no new pattern
    ## is found to start a new event. The default timeout is 5s.
    #timeout = 5s
`,
			},
		},
		{
			name: "standard testing",
			plugins: map[telegrafPluginConfig]string{
				&Docker{
					Endpoint: "unix:///var/run/docker.sock",
				}: `[[inputs.docker]]
  ## Docker Endpoint
  ##   To use TCP, set endpoint = "tcp://[ip]:[port]"
  ##   To use environment variables (ie, docker-machine), set endpoint = "ENV"
  endpoint = "unix:///var/run/docker.sock"
  #
  ## Set to true to collect Swarm metrics(desired_replicas, running_replicas)
  gather_services = false
  #
  ## Only collect metrics for these containers, collect all if empty
  container_names = []
  #
  ## Set the source tag for the metrics to the container ID hostname, eg first 12 chars
  source_tag = false
  #
  ## Containers to include and exclude. Globs accepted.
  ## Note that an empty array for both will include all containers
  container_name_include = []
  container_name_exclude = []
  #
  ## Container states to include and exclude. Globs accepted.
  ## When empty only containers in the "running" state will be captured.
  ## example: container_state_include = ["created", "restarting", "running", "removing", "paused", "exited", "dead"]
  ## example: container_state_exclude = ["created", "restarting", "running", "removing", "paused", "exited", "dead"]
  # container_state_include = []
  # container_state_exclude = []
  #
  ## Timeout for docker list, info, and stats commands
  timeout = "5s"
  #
  ## Whether to report for each container per-device blkio (8:0, 8:1...),
  ## network (eth0, eth1, ...) and cpu (cpu0, cpu1, ...) stats or not.
  ## Usage of this setting is discouraged since it will be deprecated in favor of 'perdevice_include'.
  ## Default value is 'true' for backwards compatibility, please set it to 'false' so that 'perdevice_include' setting
  ## is honored.
  perdevice = true
  #
  ## Specifies for which classes a per-device metric should be issued
  ## Possible values are 'cpu' (cpu0, cpu1, ...), 'blkio' (8:0, 8:1, ...) and 'network' (eth0, eth1, ...)
  ## Please note that this setting has no effect if 'perdevice' is set to 'true'
  # perdevice_include = ["cpu"]
  #
  ## Whether to report for each container total blkio and network stats or not.
  ## Usage of this setting is discouraged since it will be deprecated in favor of 'total_include'.
  ## Default value is 'false' for backwards compatibility, please set it to 'true' so that 'total_include' setting
  ## is honored.
  total = false
  #
  ## Specifies for which classes a total metric should be issued. Total is an aggregated of the 'perdevice' values.
  ## Possible values are 'cpu', 'blkio' and 'network'
  ## Total 'cpu' is reported directly by Docker daemon, and 'network' and 'blkio' totals are aggregated by this plugin.
  ## Please note that this setting has no effect if 'total' is set to 'false'
  # total_include = ["cpu", "blkio", "network"]
  #
  ## Which environment variables should we use as a tag
  ##tag_env = ["JAVA_HOME", "HEAP_SIZE"]
  #
  ## docker labels to include and exclude as tags.  Globs accepted.
  ## Note that an empty array for both will include all labels as tags
  docker_label_include = []
  docker_label_exclude = []
  #
  ## Optional TLS Config
  # tls_ca = "/etc/telegraf/ca.pem"
  # tls_cert = "/etc/telegraf/cert.pem"
  # tls_key = "/etc/telegraf/key.pem"
  ## Use TLS but skip chain & host verification
  # insecure_skip_verify = false
`,
				&File{
					Files: []string{
						"/var/log/**.log",
						"/var/log/apache.log",
					},
				}: `[[inputs.file]]
  ## Files to parse each interval.  Accept standard unix glob matching rules,
  ## as well as ** to match recursive files and directories.
  files = ["/var/log/**.log", "/var/log/apache.log"]

  ## Name a tag containing the name of the file the data was parsed from.  Leave empty
  ## to disable.
  # file_tag = ""

  ## Character encoding to use when interpreting the file contents.  Invalid
  ## characters are replaced using the unicode replacement character.  When set
  ## to the empty string the data is not decoded to text.
  ##   ex: character_encoding = "utf-8"
  ##       character_encoding = "utf-16le"
  ##       character_encoding = "utf-16be"
  ##       character_encoding = ""
  # character_encoding = ""

  ## The dataformat to be read from files
  ## Each data format has its own unique set of configuration options, read
  ## more about them here:
  ## https://github.com/influxdata/telegraf/blob/master/docs/DATA_FORMATS_INPUT.md
  data_format = "influx"
`,
				&Kubernetes{URL: "http://1.1.1.1:10255"}: `[[inputs.kubernetes]]
  ## URL for the kubelet
  url = "http://1.1.1.1:10255"

  ## Use bearer token for authorization. ('bearer_token' takes priority)
  ## If both of these are empty, we'll use the default serviceaccount:
  ## at: /run/secrets/kubernetes.io/serviceaccount/token
  # bearer_token = "/path/to/bearer/token"
  ## OR
  # bearer_token_string = "abc_123"

  ## Pod labels to be added as tags.  An empty array for both include and
  ## exclude will include all labels.
  # label_include = []
  # label_exclude = ["*"]

  ## Set response_timeout (default 5 seconds)
  # response_timeout = "5s"

  ## Optional TLS Config
  # tls_ca = /path/to/cafile
  # tls_cert = /path/to/certfile
  # tls_key = /path/to/keyfile
  ## Use TLS but skip chain & host verification
  # insecure_skip_verify = false
`,
				&LogParserPlugin{
					Files: []string{
						"/var/log/**.log",
						"/var/log/apache.log",
					},
				}: `[[inputs.logparser]]
  ## Log files to parse.
  ## These accept standard unix glob matching rules, but with the addition of
  ## ** as a "super asterisk". ie:
  ##   /var/log/**.log     -> recursively find all .log files in /var/log
  ##   /var/log/*/*.log    -> find all .log files with a parent dir in /var/log
  ##   /var/log/apache.log -> only tail the apache log file
  files = ["/var/log/**.log", "/var/log/apache.log"]

  ## Read files that currently exist from the beginning. Files that are created
  ## while telegraf is running (and that match the "files" globs) will always
  ## be read from the beginning.
  from_beginning = false

  ## Method used to watch for file updates.  Can be either "inotify" or "poll".
  # watch_method = "inotify"

  ## Parse logstash-style "grok" patterns:
  [inputs.logparser.grok]
    ## This is a list of patterns to check the given log file(s) for.
    ## Note that adding patterns here increases processing time. The most
    ## efficient configuration is to have one pattern per logparser.
    ## Other common built-in patterns are:
    ##   %{COMMON_LOG_FORMAT}   (plain apache & nginx access logs)
    ##   %{COMBINED_LOG_FORMAT} (access logs + referrer & agent)
    patterns = ["%{COMBINED_LOG_FORMAT}"]

    ## Name of the outputted measurement name.
    measurement = "apache_access_log"

    ## Full path(s) to custom pattern files.
    custom_pattern_files = []

    ## Custom patterns can also be defined here. Put one pattern per line.
    custom_patterns = '''
    '''

    ## Timezone allows you to provide an override for timestamps that
    ## don't already include an offset
    ## e.g. 04/06/2016 12:41:45 data one two 5.43µs
    ##
    ## Default: "" which renders UTC
    ## Options are as follows:
    ##   1. Local             -- interpret based on machine localtime
    ##   2. "Canada/Eastern"  -- Unix TZ values like those found in https://en.wikipedia.org/wiki/List_of_tz_database_time_zones
    ##   3. UTC               -- or blank/unspecified, will return timestamp in UTC
    # timezone = "Canada/Eastern"

      ## When set to "disable", timestamp will not incremented if there is a
      ## duplicate.
    # unique_timestamp = "auto"
`,
				&Nginx{
					URLs: []string{
						"http://localhost/server_status",
						"http://192.168.1.1/server_status",
					},
				}: `[[inputs.nginx]]
  # An array of Nginx stub_status URI to gather stats.
  urls = ["http://localhost/server_status", "http://192.168.1.1/server_status"]

  ## Optional TLS Config
  tls_ca = "/etc/telegraf/ca.pem"
  tls_cert = "/etc/telegraf/cert.cer"
  tls_key = "/etc/telegraf/key.key"
  ## Use TLS but skip chain & host verification
  insecure_skip_verify = false

  # HTTP response timeout (default: 5s)
  response_timeout = "5s"
`,
				&Procstat{
					Exe: "finder",
				}: `[[inputs.procstat]]
  ## PID file to monitor process
  pid_file = "/var/run/nginx.pid"
  ## executable name (ie, pgrep <exe>)
  # exe = "finder"
  ## pattern as argument for pgrep (ie, pgrep -f <pattern>)
  # pattern = "nginx"
  ## user as argument for pgrep (ie, pgrep -u <user>)
  # user = "nginx"
  ## Systemd unit name
  # systemd_unit = "nginx.service"
  ## CGroup name or path
  # cgroup = "systemd/system.slice/nginx.service"

  ## Windows service name
  # win_service = ""

  ## override for process_name
  ## This is optional; default is sourced from /proc/<pid>/status
  # process_name = "bar"

  ## Field name prefix
  # prefix = ""

  ## When true add the full cmdline as a tag.
  # cmdline_tag = false

  ## Mode to use when calculating CPU usage. Can be one of 'solaris' or 'irix'.
  # mode = "irix"

  ## Add the PID as a tag instead of as a field.  When collecting multiple
  ## processes with otherwise matching tags this setting should be enabled to
  ## ensure each process has a unique identity.
  ##
  ## Enabling this option may result in a large number of series, especially
  ## when processes have a short lifetime.
  # pid_tag = false

  ## Method to use when finding process IDs.  Can be one of 'pgrep', or
  ## 'native'.  The pgrep finder calls the pgrep executable in the PATH while
  ## the native finder performs the search directly in a manor dependent on the
  ## platform.  Default is 'pgrep'
  # pid_finder = "pgrep"
`,
				&Prometheus{
					URLs: []string{
						"http://192.168.2.1:9090",
						"http://192.168.2.2:9090",
					},
				}: `[[inputs.prometheus]]
  ## An array of urls to scrape metrics from.
  urls = ["http://192.168.2.1:9090", "http://192.168.2.2:9090"]

  ## Metric version controls the mapping from Prometheus metrics into
  ## Telegraf metrics.  When using the prometheus_client output, use the same
  ## value in both plugins to ensure metrics are round-tripped without
  ## modification.
  ##
  ##   example: metric_version = 1;
  ##            metric_version = 2; recommended version
  # metric_version = 1

  ## Url tag name (tag containing scrapped url. optional, default is "url")
  # url_tag = "url"

  ## An array of Kubernetes services to scrape metrics from.
  # kubernetes_services = ["http://my-service-dns.my-namespace:9100/metrics"]

  ## Kubernetes config file to create client from.
  # kube_config = "/path/to/kubernetes.config"

  ## Scrape Kubernetes pods for the following prometheus annotations:
  ## - prometheus.io/scrape: Enable scraping for this pod
  ## - prometheus.io/scheme: If the metrics endpoint is secured then you will need to
  ##     set this to 'https' & most likely set the tls config.
  ## - prometheus.io/path: If the metrics path is not /metrics, define it with this annotation.
  ## - prometheus.io/port: If port is not 9102 use this annotation
  # monitor_kubernetes_pods = true
  ## Get the list of pods to scrape with either the scope of
  ## - cluster: the kubernetes watch api (default, no need to specify)
  ## - node: the local cadvisor api; for scalability. Note that the config node_ip or the environment variable NODE_IP must be set to the host IP.
  # pod_scrape_scope = "cluster"
  ## Only for node scrape scope: node IP of the node that telegraf is running on.
  ## Either this config or the environment variable NODE_IP must be set.
  # node_ip = "10.180.1.1"
  # 	## Only for node scrape scope: interval in seconds for how often to get updated pod list for scraping.
  # 	## Default is 60 seconds.
  # 	# pod_scrape_interval = 60
  ## Restricts Kubernetes monitoring to a single namespace
  ##   ex: monitor_kubernetes_pods_namespace = "default"
  # monitor_kubernetes_pods_namespace = ""
  # label selector to target pods which have the label
  # kubernetes_label_selector = "env=dev,app=nginx"
  # field selector to target pods
  # eg. To scrape pods on a specific node
  # kubernetes_field_selector = "spec.nodeName=$HOSTNAME"

  ## Use bearer token for authorization. ('bearer_token' takes priority)
  # bearer_token = "/path/to/bearer/token"
  ## OR
  # bearer_token_string = "abc_123"

  ## HTTP Basic Authentication username and password. ('bearer_token' and
  ## 'bearer_token_string' take priority)
  # username = ""
  # password = ""

  ## Specify timeout duration for slower prometheus clients (default is 3s)
  # response_timeout = "3s"

  ## Optional TLS Config
  # tls_ca = /path/to/cafile
  # tls_cert = /path/to/certfile
  # tls_key = /path/to/keyfile
  ## Use TLS but skip chain & host verification
  # insecure_skip_verify = false
`,
				&Redis{
					Servers: []string{
						"tcp://localhost:6379",
						"unix:///var/run/redis.sock",
					},
					Password: "somepassword123",
				}: `[[inputs.redis]]
  ## specify servers via a url matching:
  ##  [protocol://][:password]@address[:port]
  ##  e.g.
  ##    tcp://localhost:6379
  ##    tcp://:password@192.168.99.100
  ##    unix:///var/run/redis.sock
  ##
  ## If no servers are specified, then localhost is used as the host.
  ## If no port is specified, 6379 is used
  servers = ["tcp://localhost:6379", "unix:///var/run/redis.sock"]

  ## Optional. Specify redis commands to retrieve values
  # [[inputs.redis.commands]]
  #   # The command to run where each argument is a separate element
  #   command = ["get", "sample-key"]
  #   # The field to store the result in
  #   field = "sample-key-value"
  #   # The type of the result
  #   # Can be "string", "integer", or "float"
  #   type = "string"

  ## specify server password
  # password = "somepassword123"

  ## Optional TLS Config
  # tls_ca = "/etc/telegraf/ca.pem"
  # tls_cert = "/etc/telegraf/cert.pem"
  # tls_key = "/etc/telegraf/key.pem"
  ## Use TLS but skip chain & host verification
  # insecure_skip_verify = true
`,
				&Syslog{
					Address: "tcp://10.0.0.1:6514",
				}: `[[inputs.syslog]]
  ## Specify an ip or hostname with port - eg., tcp://localhost:6514, tcp://10.0.0.1:6514
  ## Protocol, address and port to host the syslog receiver.
  ## If no host is specified, then localhost is used.
  ## If no port is specified, 6514 is used (RFC5425#section-4.1).
  server = "tcp://10.0.0.1:6514"

  ## TLS Config
  # tls_allowed_cacerts = ["/etc/telegraf/ca.pem"]
  # tls_cert = "/etc/telegraf/cert.pem"
  # tls_key = "/etc/telegraf/key.pem"

  ## Period between keep alive probes.
  ## 0 disables keep alive probes.
  ## Defaults to the OS configuration.
  ## Only applies to stream sockets (e.g. TCP).
  # keep_alive_period = "5m"

  ## Maximum number of concurrent connections (default = 0).
  ## 0 means unlimited.
  ## Only applies to stream sockets (e.g. TCP).
  # max_connections = 1024

  ## Read timeout is the maximum time allowed for reading a single message (default = 5s).
  ## 0 means unlimited.
  # read_timeout = "5s"

  ## The framing technique with which it is expected that messages are transported (default = "octet-counting").
  ## Whether the messages come using the octect-counting (RFC5425#section-4.3.1, RFC6587#section-3.4.1),
  ## or the non-transparent framing technique (RFC6587#section-3.4.2).
  ## Must be one of "octet-counting", "non-transparent".
  # framing = "octet-counting"

  ## The trailer to be expected in case of non-transparent framing (default = "LF").
  ## Must be one of "LF", or "NUL".
  # trailer = "LF"

  ## Whether to parse in best effort mode or not (default = false).
  ## By default best effort parsing is off.
  # best_effort = false

  ## Character to prepend to SD-PARAMs (default = "_").
  ## A syslog message can contain multiple parameters and multiple identifiers within structured data section.
  ## Eg., [id1 name1="val1" name2="val2"][id2 name1="val1" nameA="valA"]
  ## For each combination a field is created.
  ## Its name is created concatenating identifier, sdparam_separator, and parameter name.
  # sdparam_separator = "_"
`,
				&Tail{
					Files: []string{"/var/log/**.log", "/var/log/apache.log"},
				}: `[[inputs.tail]]
  ## File names or a pattern to tail.
  ## These accept standard unix glob matching rules, but with the addition of
  ## ** as a "super asterisk". ie:
  ##   "/var/log/**.log"  -> recursively find all .log files in /var/log
  ##   "/var/log/*/*.log" -> find all .log files with a parent dir in /var/log
  ##   "/var/log/apache.log" -> just tail the apache log file
  ##   "/var/log/log[!1-2]*  -> tail files without 1-2
  ##   "/var/log/log[^1-2]*  -> identical behavior as above
  ## See https://github.com/gobwas/glob for more examples
  ##
  files = ["/var/log/**.log", "/var/log/apache.log"]

  ## Read file from beginning.
  # from_beginning = false

  ## Whether file is a named pipe
  # pipe = false

  ## Method used to watch for file updates.  Can be either "inotify" or "poll".
  # watch_method = "inotify"

  ## Maximum lines of the file to process that have not yet be written by the
  ## output.  For best throughput set based on the number of metrics on each
  ## line and the size of the output's metric_batch_size.
  # max_undelivered_lines = 1000

  ## Character encoding to use when interpreting the file contents.  Invalid
  ## characters are replaced using the unicode replacement character.  When set
  ## to the empty string the data is not decoded to text.
  ##   ex: character_encoding = "utf-8"
  ##       character_encoding = "utf-16le"
  ##       character_encoding = "utf-16be"
  ##       character_encoding = ""
  # character_encoding = ""

  ## Data format to consume.
  ## Each data format has its own unique set of configuration options, read
  ## more about them here:
  ## https://github.com/influxdata/telegraf/blob/master/docs/DATA_FORMATS_INPUT.md
  data_format = "influx"

  ## Set the tag that will contain the path of the tailed file. If you don't want this tag, set it to an empty string.
  # path_tag = "path"

  ## multiline parser/codec
  ## https://www.elastic.co/guide/en/logstash/2.4/plugins-filters-multiline.html
  #[inputs.tail.multiline]
    ## The pattern should be a regexp which matches what you believe to be an
    ## indicator that the field is part of an event consisting of multiple lines of log data.
    #pattern = "^\s"

    ## This field must be either "previous" or "next".
    ## If a line matches the pattern, "previous" indicates that it belongs to the previous line,
    ## whereas "next" indicates that the line belongs to the next one.
    #match_which_line = "previous"

    ## The invert_match field can be true or false (defaults to false).
    ## If true, a message not matching the pattern will constitute a match of the multiline
    ## filter and the what will be applied. (vice-versa is also true)
    #invert_match = false

    ## After the specified timeout, this plugin sends a multiline event even if no new pattern
    ## is found to start a new event. The default timeout is 5s.
    #timeout = 5s
`,
			},
		},
	}
	for _, c := range cases {
		for input, toml := range c.plugins {
			if toml != input.TOML() {
				t.Fatalf("%s failed want %s, got %v", c.name, toml, input.TOML())
			}
		}
	}
}

func TestDecodeTOML(t *testing.T) {
	cases := []struct {
		name    string
		want    telegrafPluginConfig
		wantErr error
		input   telegrafPluginConfig
		data    interface{}
	}{
		{
			name:  "cpu",
			want:  &CPUStats{},
			input: &CPUStats{},
		},
		{
			name:  "disk",
			want:  &DiskStats{},
			input: &DiskStats{},
		},
		{
			name:  "diskio",
			want:  &DiskIO{},
			input: &DiskIO{},
		},
		{
			name:    "docker bad data",
			want:    &Docker{},
			wantErr: errors.New("bad endpoint for docker input plugin"),
			input:   &Docker{},
			data:    map[string]int{},
		},
		{
			name: "docker",
			want: &Docker{
				Endpoint: "unix:///var/run/docker.sock",
			},
			input: &Docker{},
			data: map[string]interface{}{
				"endpoint": "unix:///var/run/docker.sock",
			},
		},
		{
			name:    "file empty",
			want:    &File{},
			wantErr: errors.New("bad files for file input plugin"),
			input:   &File{},
		},
		{
			name:    "file bad data not array",
			want:    &File{},
			wantErr: errors.New("not an array for file input plugin"),
			input:   &File{},
			data: map[string]interface{}{
				"files": "",
			},
		},
		{
			name: "file",
			want: &File{
				Files: []string{
					"/var/log/**.log",
					"/var/log/apache.log",
				},
			},
			input: &File{},
			data: map[string]interface{}{
				"files": []interface{}{
					"/var/log/**.log",
					"/var/log/apache.log",
				},
			},
		},
		{
			name:  "kernel",
			want:  &Kernel{},
			input: &Kernel{},
		},
		{
			name:    "kubernetes empty",
			want:    &Kubernetes{},
			wantErr: errors.New("bad url for kubernetes input plugin"),
			input:   &Kubernetes{},
		},
		{
			name: "kubernetes",
			want: &Kubernetes{
				URL: "http://1.1.1.1:10255",
			},
			input: &Kubernetes{},
			data: map[string]interface{}{
				"url": "http://1.1.1.1:10255",
			},
		},
		{
			name:    "logparser empty",
			want:    &LogParserPlugin{},
			wantErr: errors.New("bad files for logparser input plugin"),
			input:   &LogParserPlugin{},
		},
		{
			name:    "logparser file not array",
			want:    &LogParserPlugin{},
			wantErr: errors.New("files is not an array for logparser input plugin"),
			input:   &LogParserPlugin{},
			data: map[string]interface{}{
				"files": "ok",
			},
		},
		{
			name: "logparser",
			want: &LogParserPlugin{
				Files: []string{
					"/var/log/**.log",
					"/var/log/apache.log",
				},
			},
			input: &LogParserPlugin{},
			data: map[string]interface{}{
				"files": []interface{}{
					"/var/log/**.log",
					"/var/log/apache.log",
				},
			},
		},
		{
			name:  "mem",
			want:  &MemStats{},
			input: &MemStats{},
		},
		{
			name:  "net_response",
			want:  &NetResponse{},
			input: &NetResponse{},
		},
		{
			name:  "net",
			want:  &NetIOStats{},
			input: &NetIOStats{},
		},
		{
			name:    "nginx empty",
			want:    &Nginx{},
			wantErr: errors.New("bad urls for nginx input plugin"),
			input:   &Nginx{},
		},
		{
			name:    "nginx bad data not array",
			want:    &Nginx{},
			wantErr: errors.New("urls is not an array for nginx input plugin"),
			input:   &Nginx{},
			data: map[string]interface{}{
				"urls": "",
			},
		},
		{
			name: "nginx",
			want: &Nginx{
				URLs: []string{
					"http://localhost/server_status",
					"http://192.168.1.1/server_status",
				},
			},
			input: &Nginx{},
			data: map[string]interface{}{
				"urls": []interface{}{
					"http://localhost/server_status",
					"http://192.168.1.1/server_status",
				},
			},
		},
		{
			name:  "processes",
			want:  &Processes{},
			input: &Processes{},
		},
		{
			name:    "procstat empty",
			want:    &Procstat{},
			wantErr: errors.New("bad exe for procstat input plugin"),
			input:   &Procstat{},
		},
		{
			name: "procstat",
			want: &Procstat{
				Exe: "finder",
			},
			input: &Procstat{},
			data: map[string]interface{}{
				"exe": "finder",
			},
		},
		{
			name:    "prometheus empty",
			want:    &Prometheus{},
			wantErr: errors.New("bad urls for prometheus input plugin"),
			input:   &Prometheus{},
		},
		{
			name:    "prometheus bad data not array",
			want:    &Prometheus{},
			wantErr: errors.New("urls is not an array for prometheus input plugin"),
			input:   &Prometheus{},
			data: map[string]interface{}{
				"urls": "",
			},
		},
		{
			name: "prometheus",
			want: &Prometheus{
				URLs: []string{
					"http://192.168.2.1:9090",
					"http://192.168.2.2:9090",
				},
			},
			input: &Prometheus{},
			data: map[string]interface{}{
				"urls": []interface{}{
					"http://192.168.2.1:9090",
					"http://192.168.2.2:9090",
				},
			},
		},
		{
			name:    "redis empty",
			want:    &Redis{},
			wantErr: errors.New("bad servers for redis input plugin"),
			input:   &Redis{},
		},
		{
			name:    "redis bad data not array",
			want:    &Redis{},
			wantErr: errors.New("servers is not an array for redis input plugin"),
			input:   &Redis{},
			data: map[string]interface{}{
				"servers": "",
			},
		},
		{
			name: "redis without password",
			want: &Redis{
				Servers: []string{
					"http://192.168.2.1:9090",
					"http://192.168.2.2:9090",
				},
			},
			input: &Redis{},
			data: map[string]interface{}{
				"servers": []interface{}{
					"http://192.168.2.1:9090",
					"http://192.168.2.2:9090",
				},
			},
		},
		{
			name: "redis with password",
			want: &Redis{
				Servers: []string{
					"http://192.168.2.1:9090",
					"http://192.168.2.2:9090",
				},
				Password: "pass1",
			},
			input: &Redis{},
			data: map[string]interface{}{
				"servers": []interface{}{
					"http://192.168.2.1:9090",
					"http://192.168.2.2:9090",
				},
				"password": "pass1",
			},
		},
		{
			name:  "swap",
			want:  &SwapStats{},
			input: &SwapStats{},
		},
		{
			name:    "syslog empty",
			want:    &Syslog{},
			wantErr: errors.New("bad server for syslog input plugin"),
			input:   &Syslog{},
		},
		{
			name: "syslog",
			want: &Syslog{
				Address: "http://1.1.1.1:10255",
			},
			input: &Syslog{},
			data: map[string]interface{}{
				"server": "http://1.1.1.1:10255",
			},
		},
		{
			name:  "system",
			want:  &SystemStats{},
			input: &SystemStats{},
		},
		{
			name:    "tail empty",
			want:    &Tail{},
			wantErr: errors.New("bad files for tail input plugin"),
			input:   &Tail{},
		},
		{
			name:    "tail bad data not array",
			want:    &Tail{},
			wantErr: errors.New("not an array for tail input plugin"),
			input:   &Tail{},
			data: map[string]interface{}{
				"files": "",
			},
		},
		{
			name: "tail",
			want: &Tail{
				Files: []string{
					"http://192.168.2.1:9090",
					"http://192.168.2.2:9090",
				},
			},
			input: &Tail{},
			data: map[string]interface{}{
				"files": []interface{}{
					"http://192.168.2.1:9090",
					"http://192.168.2.2:9090",
				},
			},
		},
	}
	for _, c := range cases {
		err := c.input.UnmarshalTOML(c.data)
		if c.wantErr != nil && (err == nil || err.Error() != c.wantErr.Error()) {
			t.Fatalf("%s failed want err %s, got %v", c.name, c.wantErr.Error(), err)
		}
		if c.wantErr == nil && err != nil {
			t.Fatalf("%s failed want err nil, got %v", c.name, err)
		}
		if !reflect.DeepEqual(c.input, c.want) {
			t.Fatalf("%s failed want %v, got %v", c.name, c.want, c.input)
		}
	}
}
