package inputs

import (
	"errors"
	"reflect"
	"testing"

	"github.com/influxdata/platform/telegraf/plugins"
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
  ## If true, collect raw CPU time metrics.
  collect_cpu_time = false
  ## If true, compute and report the sum of all non-idle CPU states.
  report_active = false
`,
				&DiskStats{}: `[[inputs.disk]]
  ## By default stats will be gathered for all mount points.
  ## Set mount_points will restrict the stats to only the specified mount points.
  # mount_points = ["/"]
  ## Ignore mount points by filesystem type.
  ignore_fs = ["tmpfs", "devtmpfs", "devfs", "overlay", "aufs", "squashfs"]
`,
				&DiskIO{}: "[[inputs.diskio]]\n",
				&Docker{}: `[[inputs.docker]]	
  ## Docker Endpoint
  ##   To use TCP, set endpoint = "tcp://[ip]:[port]"
  ##   To use environment variables (ie, docker-machine), set endpoint = "ENV"
  ##   exp: unix:///var/run/docker.sock
  endpoint = ""

  ## Set to true to collect Swarm metrics(desired_replicas, running_replicas)
  gather_services = false

  ## Only collect metrics for these containers, collect all if empty
  container_names = []

  ## Containers to include and exclude. Globs accepted.
  ## Note that an empty array for both will include all containers
  container_name_include = []
  container_name_exclude = []

  ## Container states to include and exclude. Globs accepted.
  ## When empty only containers in the "running" state will be captured.
  # container_state_include = []
  # container_state_exclude = []

  ## Timeout for docker list, info, and stats commands
  timeout = "5s"

  ## Whether to report for each container per-device blkio (8:0, 8:1...) and
  ## network (eth0, eth1, ...) stats or not
  perdevice = true

  ## Whether to report for each container total blkio and network stats or not
  total = false
  
  ## Which environment variables should we use as a tag
  ##tag_env = ["JAVA_HOME", "HEAP_SIZE"]
  ## docker labels to include and exclude as tags.  Globs accepted.
  ## Note that an empty array for both will include all labels as tags
  docker_label_include = []
  docker_label_exclude = []
`,
				&File{}: `[[inputs.file]]	
  ## Files to parse each interval.
  ## These accept standard unix glob matching rules, but with the addition of
  ## ** as a "super asterisk". ie:
  ##   /var/log/**.log     -> recursively find all .log files in /var/log
  ##   /var/log/*/*.log    -> find all .log files with a parent dir in /var/log
  ##   /var/log/apache.log -> only read the apache log file
  files = []

  ## The dataformat to be read from files
  ## Each data format has its own unique set of configuration options, read
  ## more about them here:
  ## https://github.com/influxdata/telegraf/blob/master/docs/DATA_FORMATS_INPUT.md
  data_format = "influx"
`,
				&Kernel{}: "[[inputs.kernel]]\n",
				&Kubernetes{}: `[[inputs.kubernetes]]
  ## URL for the kubelet
  ## exp: http://1.1.1.1:10255
  url = ""	
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
`,
				&MemStats{}:   "[[inputs.mem]]\n",
				&NetIOStats{}: "[[inputs.net]]\n",
				&NetResponse{}: `[[inputs.net_response]]
  ## Protocol, must be "tcp" or "udp"
  ## NOTE: because the "udp" protocol does not respond to requests, it requires
  ## a send/expect string pair (see below).
  protocol = "tcp"
  ## Server address (default localhost)
  address = "localhost:80"
`,
				&Nginx{}: `[[inputs.nginx]]
  # An array of Nginx stub_status URI to gather stats.
  # exp http://localhost/server_status
  urls = []
`,
				&Processes{}: "[[inputs.processes]]\n",
				&Procstat{}: `[[inputs.procstat]]
  ## executable name (ie, pgrep <exe>)
  exe = ""
`,
				&Prometheus{}: `[[inputs.prometheus]]	
  ## An array of urls to scrape metrics from.
  urls = []
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

  ## specify server password
  # password = ""
`,
				&SwapStats{}: "[[inputs.swap]]\n",
				&Syslog{}: `[[inputs.syslog]]
  ## Specify an ip or hostname with port - eg., tcp://localhost:6514, tcp://10.0.0.1:6514
  ## Protocol, address and port to host the syslog receiver.
  ## If no host is specified, then localhost is used.
  ## If no port is specified, 6514 is used (RFC5425#section-4.1).
  server = ""
`,
				&SystemStats{}: "[[inputs.system]]\n",
				&Tail{}: `[[inputs.tail]]	
  ## files to tail.
  ## These accept standard unix glob matching rules, but with the addition of
  ## ** as a "super asterisk". ie:
  ##   "/var/log/**.log"  -> recursively find all .log files in /var/log
  ##   "/var/log/*/*.log" -> find all .log files with a parent dir in /var/log
  ##   "/var/log/apache.log" -> just tail the apache log file
  ##
  ## See https://github.com/gobwas/glob for more examples
  ##
  files = []

  ## Read file from beginning.
  from_beginning = false
  ## Whether file is a named pipe
  pipe = false
  ## Method used to watch for file updates.  Can be either "inotify" or "poll".
  # watch_method = "inotify"
  ## Data format to consume.
  ## Each data format has its own unique set of configuration options, read
  ## more about them here:
  ## https://github.com/influxdata/telegraf/blob/master/docs/DATA_FORMATS_INPUT.md
  data_format = "influx"
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
  ##   exp: unix:///var/run/docker.sock
  endpoint = "unix:///var/run/docker.sock"

  ## Set to true to collect Swarm metrics(desired_replicas, running_replicas)
  gather_services = false

  ## Only collect metrics for these containers, collect all if empty
  container_names = []

  ## Containers to include and exclude. Globs accepted.
  ## Note that an empty array for both will include all containers
  container_name_include = []
  container_name_exclude = []

  ## Container states to include and exclude. Globs accepted.
  ## When empty only containers in the "running" state will be captured.
  # container_state_include = []
  # container_state_exclude = []

  ## Timeout for docker list, info, and stats commands
  timeout = "5s"

  ## Whether to report for each container per-device blkio (8:0, 8:1...) and
  ## network (eth0, eth1, ...) stats or not
  perdevice = true

  ## Whether to report for each container total blkio and network stats or not
  total = false
  
  ## Which environment variables should we use as a tag
  ##tag_env = ["JAVA_HOME", "HEAP_SIZE"]
  ## docker labels to include and exclude as tags.  Globs accepted.
  ## Note that an empty array for both will include all labels as tags
  docker_label_include = []
  docker_label_exclude = []
`,
				&File{
					Files: []string{
						"/var/log/**.log",
						"/var/log/apache.log",
					},
				}: `[[inputs.file]]	
  ## Files to parse each interval.
  ## These accept standard unix glob matching rules, but with the addition of
  ## ** as a "super asterisk". ie:
  ##   /var/log/**.log     -> recursively find all .log files in /var/log
  ##   /var/log/*/*.log    -> find all .log files with a parent dir in /var/log
  ##   /var/log/apache.log -> only read the apache log file
  files = ["/var/log/**.log", "/var/log/apache.log"]

  ## The dataformat to be read from files
  ## Each data format has its own unique set of configuration options, read
  ## more about them here:
  ## https://github.com/influxdata/telegraf/blob/master/docs/DATA_FORMATS_INPUT.md
  data_format = "influx"
`,
				&Kubernetes{URL: "http://1.1.1.1:10255"}: `[[inputs.kubernetes]]
  ## URL for the kubelet
  ## exp: http://1.1.1.1:10255
  url = "http://1.1.1.1:10255"	
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
`,
				&Nginx{
					URLs: []string{
						"http://localhost/server_status",
						"http://192.168.1.1/server_status",
					},
				}: `[[inputs.nginx]]
  # An array of Nginx stub_status URI to gather stats.
  # exp http://localhost/server_status
  urls = ["http://localhost/server_status", "http://192.168.1.1/server_status"]
`,
				&Procstat{
					Exe: "finder",
				}: `[[inputs.procstat]]
  ## executable name (ie, pgrep <exe>)
  exe = "finder"
`,
				&Prometheus{
					URLs: []string{
						"http://192.168.2.1:9090",
						"http://192.168.2.2:9090",
					},
				}: `[[inputs.prometheus]]	
  ## An array of urls to scrape metrics from.
  urls = ["http://192.168.2.1:9090", "http://192.168.2.2:9090"]
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

  ## specify server password
  password = "somepassword123"
`,
				&Syslog{
					Address: "tcp://10.0.0.1:6514",
				}: `[[inputs.syslog]]
  ## Specify an ip or hostname with port - eg., tcp://localhost:6514, tcp://10.0.0.1:6514
  ## Protocol, address and port to host the syslog receiver.
  ## If no host is specified, then localhost is used.
  ## If no port is specified, 6514 is used (RFC5425#section-4.1).
  server = "tcp://10.0.0.1:6514"
`,
				&Tail{
					Files: []string{"/var/log/**.log", "/var/log/apache.log"},
				}: `[[inputs.tail]]	
  ## files to tail.
  ## These accept standard unix glob matching rules, but with the addition of
  ## ** as a "super asterisk". ie:
  ##   "/var/log/**.log"  -> recursively find all .log files in /var/log
  ##   "/var/log/*/*.log" -> find all .log files with a parent dir in /var/log
  ##   "/var/log/apache.log" -> just tail the apache log file
  ##
  ## See https://github.com/gobwas/glob for more examples
  ##
  files = ["/var/log/**.log", "/var/log/apache.log"]

  ## Read file from beginning.
  from_beginning = false
  ## Whether file is a named pipe
  pipe = false
  ## Method used to watch for file updates.  Can be either "inotify" or "poll".
  # watch_method = "inotify"
  ## Data format to consume.
  ## Each data format has its own unique set of configuration options, read
  ## more about them here:
  ## https://github.com/influxdata/telegraf/blob/master/docs/DATA_FORMATS_INPUT.md
  data_format = "influx"
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
