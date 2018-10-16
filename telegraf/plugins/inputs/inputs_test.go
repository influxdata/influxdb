package inputs

import (
	"testing"

	"github.com/influxdata/platform/telegraf/plugins"
)

// local plugin
type telegrafPluginConfig interface {
	TOML() string
	Type() plugins.Type
	PluginName() string
}

func TestType(t *testing.T) {
	b := baseInput(0)
	if b.Type() != plugins.Input {
		t.Fatalf("input plugins type should be input, got %s", b.Type())
	}
}

func TestTOML(t *testing.T) {
	cases := []struct {
		name    string
		plugins map[telegrafPluginConfig]string
	}{
		{
			name: "test empty plugins",
			plugins: map[telegrafPluginConfig]string{
				&CPUStats{}:  "[[inputs.cpu]]\n",
				&DiskStats{}: "[[inputs.disk]]\n",
				&DiskIO{}:    "[[inputs.diskio]]\n",
				&Docker{}: `[[inputs.docker]]	
  ## Docker Endpoint
  ##   To use TCP, set endpoint = "tcp://[ip]:[port]"
  ##   To use environment variables (ie, docker-machine), set endpoint = "ENV"
  ##   exp: unix:///var/run/docker.sock
  endpoint = ""
`,
				&File{}: `[[inputs.file]]	
  ## Files to parse each interval.
  ## These accept standard unix glob matching rules, but with the addition of
  ## ** as a "super asterisk". ie:
  ##   /var/log/**.log     -> recursively find all .log files in /var/log
  ##   /var/log/*/*.log    -> find all .log files with a parent dir in /var/log
  ##   /var/log/apache.log -> only read the apache log file
  files = []
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
`,
				&MemStats{}:    "[[inputs.mem]]\n",
				&NetIOStats{}:  "[[inputs.net]]\n",
				&NetResponse{}: "[[inputs.net_response]]\n",
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
