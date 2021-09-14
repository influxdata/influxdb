package influxdb

import (
	"encoding/json"
	"fmt"
	"sort"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/influxdata/influxdb/v2/kit/platform/errors"
	"github.com/influxdata/influxdb/v2/telegraf/plugins"
	"github.com/influxdata/influxdb/v2/telegraf/plugins/inputs"
	"github.com/influxdata/influxdb/v2/telegraf/plugins/outputs"
	"github.com/stretchr/testify/require"
)

var telegrafCmpOptions = cmp.Options{
	cmpopts.IgnoreUnexported(
		inputs.CPUStats{},
		inputs.Kernel{},
		inputs.Kubernetes{},
		inputs.File{},
		outputs.File{},
		outputs.InfluxDBV2{},
	),
	cmp.Transformer("Sort", func(in []*TelegrafConfig) []*TelegrafConfig {
		out := append([]*TelegrafConfig(nil), in...)
		sort.Slice(out, func(i, j int) bool {
			return out[i].ID > out[j].ID
		})
		return out
	}),
}

// tests backwards compatibillity with the current plugin aware system.
func TestTelegrafConfigJSONDecodeWithoutID(t *testing.T) {
	s := `{
		"name": "config 2",
		"agent": {
			"collectionInterval": 120000
		},
		"plugins": [
			{
				"name": "cpu",
				"type": "input",
				"comment": "cpu collect cpu metrics",
				"config": {}
			},
			{
				"name": "kernel",
				"type": "input"
			},
			{
				"name": "kubernetes",
				"type": "input",
				"config":{
					"url": "http://1.1.1.1:12"
				}
			},
			{
				"name": "influxdb_v2",
				"type": "output",
				"comment": "3",
				"config": {
					"urls": [
						"http://127.0.0.1:9999"
					],
					"token": "token1",
					"organization": "org",
					"bucket": "bucket"
				}
			}
		]
	}`
	want := &TelegrafConfig{
		Name: "config 2",
		Config: `# Configuration for telegraf agent
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
[[inputs.cpu]]
  ## Whether to report per-cpu stats or not
  percpu = true
  ## Whether to report total system cpu stats or not
  totalcpu = true
  ## If true, collect raw CPU time metrics
  collect_cpu_time = false
  ## If true, compute and report the sum of all non-idle CPU states
  report_active = false
[[inputs.kernel]]
  # no configuration
[[inputs.kubernetes]]
  ## URL for the kubelet
  url = "http://1.1.1.1:12"

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
[[outputs.influxdb_v2]]
  ## The URLs of the InfluxDB cluster nodes.
  ##
  ## Multiple URLs can be specified for a single cluster, only ONE of the
  ## urls will be written to each interval.
  ##   ex: urls = ["https://us-west-2-1.aws.cloud2.influxdata.com"]
  urls = ["http://127.0.0.1:9999"]

  ## Token for authentication.
  token = "token1"

  ## Organization is the name of the organization you wish to write to; must exist.
  organization = "org"

  ## Destination bucket to write into.
  bucket = "bucket"

  ## The value of this tag will be used to determine the bucket.  If this
  ## tag is not set the 'bucket' option is used as the default.
  # bucket_tag = ""

  ## If true, the bucket tag will not be added to the metric.
  # exclude_bucket_tag = false

  ## Timeout for HTTP messages.
  # timeout = "5s"

  ## Additional HTTP headers
  # http_headers = {"X-Special-Header" = "Special-Value"}

  ## HTTP Proxy override, if unset values the standard proxy environment
  ## variables are consulted to determine which proxy, if any, should be used.
  # http_proxy = "http://corporate.proxy:3128"

  ## HTTP User-Agent
  # user_agent = "telegraf"

  ## Content-Encoding for write request body, can be set to "gzip" to
  ## compress body or "identity" to apply no encoding.
  # content_encoding = "gzip"

  ## Enable or disable uint support for writing uints influxdb 2.0.
  # influx_uint_support = false

  ## Optional TLS Config for use on HTTP connections.
  # tls_ca = "/etc/telegraf/ca.pem"
  # tls_cert = "/etc/telegraf/cert.pem"
  # tls_key = "/etc/telegraf/key.pem"
  ## Use TLS but skip chain & host verification
  # insecure_skip_verify = false
`,
		Metadata: map[string]interface{}{"buckets": []string{"bucket"}},
	}
	got := new(TelegrafConfig)
	err := json.Unmarshal([]byte(s), got)
	if err != nil {
		t.Fatal("json decode error", err.Error())
	}
	if diff := cmp.Diff(got, want, telegrafCmpOptions...); diff != "" {
		t.Errorf("telegraf configs are different -got/+want\ndiff %s", diff)
	}
}

// tests forwards compatibillity with the new plugin unaware system.
func TestTelegrafConfigJSONDecodeTOML(t *testing.T) {
	s := `{
		"name": "config 2",
		"config": "# Configuration for telegraf agent\n[agent]\n  ## Default data collection interval for all inputs\n  interval = \"10s\"\n  ## Rounds collection interval to 'interval'\n  ## ie, if interval=\"10s\" then always collect on :00, :10, :20, etc.\n  round_interval = true\n\n  ## Telegraf will send metrics to outputs in batches of at most\n  ## metric_batch_size metrics.\n  ## This controls the size of writes that Telegraf sends to output plugins.\n  metric_batch_size = 1000\n\n  ## Maximum number of unwritten metrics per output.  Increasing this value\n  ## allows for longer periods of output downtime without dropping metrics at the\n  ## cost of higher maximum memory usage.\n  metric_buffer_limit = 10000\n\n  ## Collection jitter is used to jitter the collection by a random amount.\n  ## Each plugin will sleep for a random time within jitter before collecting.\n  ## This can be used to avoid many plugins querying things like sysfs at the\n  ## same time, which can have a measurable effect on the system.\n  collection_jitter = \"0s\"\n\n  ## Default flushing interval for all outputs. Maximum flush_interval will be\n  ## flush_interval + flush_jitter\n  flush_interval = \"10s\"\n  ## Jitter the flush interval by a random amount. This is primarily to avoid\n  ## large write spikes for users running a large number of telegraf instances.\n  ## ie, a jitter of 5s and interval 10s means flushes will happen every 10-15s\n  flush_jitter = \"0s\"\n\n  ## By default or when set to \"0s\", precision will be set to the same\n  ## timestamp order as the collection interval, with the maximum being 1s.\n  ##   ie, when interval = \"10s\", precision will be \"1s\"\n  ##       when interval = \"250ms\", precision will be \"1ms\"\n  ## Precision will NOT be used for service inputs. It is up to each individual\n  ## service input to set the timestamp at the appropriate precision.\n  ## Valid time units are \"ns\", \"us\" (or \"µs\"), \"ms\", \"s\".\n  precision = \"\"\n\n  ## Log at debug level.\n  # debug = false\n  ## Log only error level messages.\n  # quiet = false\n\n  ## Log target controls the destination for logs and can be one of \"file\",\n  ## \"stderr\" or, on Windows, \"eventlog\".  When set to \"file\", the output file\n  ## is determined by the \"logfile\" setting.\n  # logtarget = \"file\"\n\n  ## Name of the file to be logged to when using the \"file\" logtarget.  If set to\n  ## the empty string then logs are written to stderr.\n  # logfile = \"\"\n\n  ## The logfile will be rotated after the time interval specified.  When set\n  ## to 0 no time based rotation is performed.  Logs are rotated only when\n  ## written to, if there is no log activity rotation may be delayed.\n  # logfile_rotation_interval = \"0d\"\n\n  ## The logfile will be rotated when it becomes larger than the specified\n  ## size.  When set to 0 no size based rotation is performed.\n  # logfile_rotation_max_size = \"0MB\"\n\n  ## Maximum number of rotated archives to keep, any older logs are deleted.\n  ## If set to -1, no archives are removed.\n  # logfile_rotation_max_archives = 5\n\n  ## Pick a timezone to use when logging or type 'local' for local time.\n  ## Example: America/Chicago\n  # log_with_timezone = \"\"\n\n  ## Override default hostname, if empty use os.Hostname()\n  hostname = \"\"\n  ## If set to true, do no set the \"host\" tag in the telegraf agent.\n  omit_hostname = false\n[[inputs.cpu]]\n  ## Whether to report per-cpu stats or not\n  percpu = true\n  ## Whether to report total system cpu stats or not\n  totalcpu = true\n  ## If true, collect raw CPU time metrics\n  collect_cpu_time = false\n  ## If true, compute and report the sum of all non-idle CPU states\n  report_active = false\n[[inputs.kernel]]\n  # no configuration\n[[inputs.kubernetes]]\n  ## URL for the kubelet\n  url = \"http://1.1.1.1:12\"\n\n  ## Use bearer token for authorization. ('bearer_token' takes priority)\n  ## If both of these are empty, we'll use the default serviceaccount:\n  ## at: /run/secrets/kubernetes.io/serviceaccount/token\n  # bearer_token = \"/path/to/bearer/token\"\n  ## OR\n  # bearer_token_string = \"abc_123\"\n\n  ## Pod labels to be added as tags.  An empty array for both include and\n  ## exclude will include all labels.\n  # label_include = []\n  # label_exclude = [\"*\"]\n\n  ## Set response_timeout (default 5 seconds)\n  # response_timeout = \"5s\"\n\n  ## Optional TLS Config\n  # tls_ca = /path/to/cafile\n  # tls_cert = /path/to/certfile\n  # tls_key = /path/to/keyfile\n  ## Use TLS but skip chain & host verification\n  # insecure_skip_verify = false\n[[outputs.influxdb_v2]]\n  ## The URLs of the InfluxDB cluster nodes.\n  ##\n  ## Multiple URLs can be specified for a single cluster, only ONE of the\n  ## urls will be written to each interval.\n  ##   ex: urls = [\"https://us-west-2-1.aws.cloud2.influxdata.com\"]\n  urls = [\"url1\", \"url2\"]\n\n  ## Token for authentication.\n  token = \"token1\"\n\n  ## Organization is the name of the organization you wish to write to; must exist.\n  organization = \"org1\"\n\n  ## Destination bucket to write into.\n  bucket = \"bucket1\"\n\n  ## The value of this tag will be used to determine the bucket.  If this\n  ## tag is not set the 'bucket' option is used as the default.\n  # bucket_tag = \"\"\n\n  ## If true, the bucket tag will not be added to the metric.\n  # exclude_bucket_tag = false\n\n  ## Timeout for HTTP messages.\n  # timeout = \"5s\"\n\n  ## Additional HTTP headers\n  # http_headers = {\"X-Special-Header\" = \"Special-Value\"}\n\n  ## HTTP Proxy override, if unset values the standard proxy environment\n  ## variables are consulted to determine which proxy, if any, should be used.\n  # http_proxy = \"http://corporate.proxy:3128\"\n\n  ## HTTP User-Agent\n  # user_agent = \"telegraf\"\n\n  ## Content-Encoding for write request body, can be set to \"gzip\" to\n  ## compress body or \"identity\" to apply no encoding.\n  # content_encoding = \"gzip\"\n\n  ## Enable or disable uint support for writing uints influxdb 2.0.\n  # influx_uint_support = false\n\n  ## Optional TLS Config for use on HTTP connections.\n  # tls_ca = \"/etc/telegraf/ca.pem\"\n  # tls_cert = \"/etc/telegraf/cert.pem\"\n  # tls_key = \"/etc/telegraf/key.pem\"\n  ## Use TLS but skip chain & host verification\n  # insecure_skip_verify = false\n"
	}`

	want := &TelegrafConfig{
		Name: "config 2",
		Config: `# Configuration for telegraf agent
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
[[inputs.cpu]]
  ## Whether to report per-cpu stats or not
  percpu = true
  ## Whether to report total system cpu stats or not
  totalcpu = true
  ## If true, collect raw CPU time metrics
  collect_cpu_time = false
  ## If true, compute and report the sum of all non-idle CPU states
  report_active = false
[[inputs.kernel]]
  # no configuration
[[inputs.kubernetes]]
  ## URL for the kubelet
  url = "http://1.1.1.1:12"

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
[[outputs.influxdb_v2]]
  ## The URLs of the InfluxDB cluster nodes.
  ##
  ## Multiple URLs can be specified for a single cluster, only ONE of the
  ## urls will be written to each interval.
  ##   ex: urls = ["https://us-west-2-1.aws.cloud2.influxdata.com"]
  urls = ["url1", "url2"]

  ## Token for authentication.
  token = "token1"

  ## Organization is the name of the organization you wish to write to; must exist.
  organization = "org1"

  ## Destination bucket to write into.
  bucket = "bucket1"

  ## The value of this tag will be used to determine the bucket.  If this
  ## tag is not set the 'bucket' option is used as the default.
  # bucket_tag = ""

  ## If true, the bucket tag will not be added to the metric.
  # exclude_bucket_tag = false

  ## Timeout for HTTP messages.
  # timeout = "5s"

  ## Additional HTTP headers
  # http_headers = {"X-Special-Header" = "Special-Value"}

  ## HTTP Proxy override, if unset values the standard proxy environment
  ## variables are consulted to determine which proxy, if any, should be used.
  # http_proxy = "http://corporate.proxy:3128"

  ## HTTP User-Agent
  # user_agent = "telegraf"

  ## Content-Encoding for write request body, can be set to "gzip" to
  ## compress body or "identity" to apply no encoding.
  # content_encoding = "gzip"

  ## Enable or disable uint support for writing uints influxdb 2.0.
  # influx_uint_support = false

  ## Optional TLS Config for use on HTTP connections.
  # tls_ca = "/etc/telegraf/ca.pem"
  # tls_cert = "/etc/telegraf/cert.pem"
  # tls_key = "/etc/telegraf/key.pem"
  ## Use TLS but skip chain & host verification
  # insecure_skip_verify = false
`,
		Metadata: map[string]interface{}{"buckets": []string{"bucket1"}},
	}
	got := new(TelegrafConfig)
	err := json.Unmarshal([]byte(s), got)
	if err != nil {
		t.Fatal("json decode error", err.Error())
	}
	if diff := cmp.Diff(got, want, telegrafCmpOptions...); diff != "" {
		t.Errorf("telegraf configs are different -got/+want\ndiff %s", diff)
	}
}

func TestTelegrafConfigJSONCompatibleMode(t *testing.T) {
	id1, _ := platform.IDFromString("020f755c3c082000")
	id2, _ := platform.IDFromString("020f755c3c082222")
	id3, _ := platform.IDFromString("020f755c3c082223")
	cases := []struct {
		name    string
		src     []byte
		cfg     *TelegrafConfig
		expMeta map[string]interface{}
		err     error
	}{
		{
			name: "newest",
			src:  []byte(`{"id":"020f755c3c082000","orgID":"020f755c3c082222","plugins":[]}`),
			cfg: &TelegrafConfig{
				ID:     *id1,
				OrgID:  *id2,
				Config: "# Configuration for telegraf agent\n[agent]\n  ## Default data collection interval for all inputs\n  interval = \"10s\"\n  ## Rounds collection interval to 'interval'\n  ## ie, if interval=\"10s\" then always collect on :00, :10, :20, etc.\n  round_interval = true\n\n  ## Telegraf will send metrics to outputs in batches of at most\n  ## metric_batch_size metrics.\n  ## This controls the size of writes that Telegraf sends to output plugins.\n  metric_batch_size = 1000\n\n  ## Maximum number of unwritten metrics per output.  Increasing this value\n  ## allows for longer periods of output downtime without dropping metrics at the\n  ## cost of higher maximum memory usage.\n  metric_buffer_limit = 10000\n\n  ## Collection jitter is used to jitter the collection by a random amount.\n  ## Each plugin will sleep for a random time within jitter before collecting.\n  ## This can be used to avoid many plugins querying things like sysfs at the\n  ## same time, which can have a measurable effect on the system.\n  collection_jitter = \"0s\"\n\n  ## Default flushing interval for all outputs. Maximum flush_interval will be\n  ## flush_interval + flush_jitter\n  flush_interval = \"10s\"\n  ## Jitter the flush interval by a random amount. This is primarily to avoid\n  ## large write spikes for users running a large number of telegraf instances.\n  ## ie, a jitter of 5s and interval 10s means flushes will happen every 10-15s\n  flush_jitter = \"0s\"\n\n  ## By default or when set to \"0s\", precision will be set to the same\n  ## timestamp order as the collection interval, with the maximum being 1s.\n  ##   ie, when interval = \"10s\", precision will be \"1s\"\n  ##       when interval = \"250ms\", precision will be \"1ms\"\n  ## Precision will NOT be used for service inputs. It is up to each individual\n  ## service input to set the timestamp at the appropriate precision.\n  ## Valid time units are \"ns\", \"us\" (or \"µs\"), \"ms\", \"s\".\n  precision = \"\"\n\n  ## Log at debug level.\n  # debug = false\n  ## Log only error level messages.\n  # quiet = false\n\n  ## Log target controls the destination for logs and can be one of \"file\",\n  ## \"stderr\" or, on Windows, \"eventlog\".  When set to \"file\", the output file\n  ## is determined by the \"logfile\" setting.\n  # logtarget = \"file\"\n\n  ## Name of the file to be logged to when using the \"file\" logtarget.  If set to\n  ## the empty string then logs are written to stderr.\n  # logfile = \"\"\n\n  ## The logfile will be rotated after the time interval specified.  When set\n  ## to 0 no time based rotation is performed.  Logs are rotated only when\n  ## written to, if there is no log activity rotation may be delayed.\n  # logfile_rotation_interval = \"0d\"\n\n  ## The logfile will be rotated when it becomes larger than the specified\n  ## size.  When set to 0 no size based rotation is performed.\n  # logfile_rotation_max_size = \"0MB\"\n\n  ## Maximum number of rotated archives to keep, any older logs are deleted.\n  ## If set to -1, no archives are removed.\n  # logfile_rotation_max_archives = 5\n\n  ## Pick a timezone to use when logging or type 'local' for local time.\n  ## Example: America/Chicago\n  # log_with_timezone = \"\"\n\n  ## Override default hostname, if empty use os.Hostname()\n  hostname = \"\"\n  ## If set to true, do no set the \"host\" tag in the telegraf agent.\n  omit_hostname = false\n# Configuration for sending metrics to InfluxDB\n[[outputs.influxdb_v2]]\n  # alias=\"influxdb_v2\"\n  ## The URLs of the InfluxDB cluster nodes.\n  ##\n  ## Multiple URLs can be specified for a single cluster, only ONE of the\n  ## urls will be written to each interval.\n  ##   ex: urls = [\"https://us-west-2-1.aws.cloud2.influxdata.com\"]\n  urls = [\"http://127.0.0.1:9999\"]\n\n  ## Token for authentication.\n  token = \"\"\n\n  ## Organization is the name of the organization you wish to write to; must exist.\n  organization = \"\"\n\n  ## Destination bucket to write into.\n  bucket = \"\"\n\n  ## The value of this tag will be used to determine the bucket.  If this\n  ## tag is not set the 'bucket' option is used as the default.\n  # bucket_tag = \"\"\n\n  ## If true, the bucket tag will not be added to the metric.\n  # exclude_bucket_tag = false\n\n  ## Timeout for HTTP messages.\n  # timeout = \"5s\"\n\n  ## Additional HTTP headers\n  # http_headers = {\"X-Special-Header\" = \"Special-Value\"}\n\n  ## HTTP Proxy override, if unset values the standard proxy environment\n  ## variables are consulted to determine which proxy, if any, should be used.\n  # http_proxy = \"http://corporate.proxy:3128\"\n\n  ## HTTP User-Agent\n  # user_agent = \"telegraf\"\n\n  ## Content-Encoding for write request body, can be set to \"gzip\" to\n  ## compress body or \"identity\" to apply no encoding.\n  # content_encoding = \"gzip\"\n\n  ## Enable or disable uint support for writing uints influxdb 2.0.\n  # influx_uint_support = false\n\n  ## Optional TLS Config for use on HTTP connections.\n  # tls_ca = \"/etc/telegraf/ca.pem\"\n  # tls_cert = \"/etc/telegraf/cert.pem\"\n  # tls_key = \"/etc/telegraf/key.pem\"\n  ## Use TLS but skip chain & host verification\n  # insecure_skip_verify = false\n\n",
			},
			expMeta: map[string]interface{}{"buckets": []string{}},
		},
		{
			name: "old",
			src:  []byte(`{"id":"020f755c3c082000","organizationID":"020f755c3c082222","plugins":[]}`),
			cfg: &TelegrafConfig{
				ID:     *id1,
				OrgID:  *id2,
				Config: "# Configuration for telegraf agent\n[agent]\n  ## Default data collection interval for all inputs\n  interval = \"10s\"\n  ## Rounds collection interval to 'interval'\n  ## ie, if interval=\"10s\" then always collect on :00, :10, :20, etc.\n  round_interval = true\n\n  ## Telegraf will send metrics to outputs in batches of at most\n  ## metric_batch_size metrics.\n  ## This controls the size of writes that Telegraf sends to output plugins.\n  metric_batch_size = 1000\n\n  ## Maximum number of unwritten metrics per output.  Increasing this value\n  ## allows for longer periods of output downtime without dropping metrics at the\n  ## cost of higher maximum memory usage.\n  metric_buffer_limit = 10000\n\n  ## Collection jitter is used to jitter the collection by a random amount.\n  ## Each plugin will sleep for a random time within jitter before collecting.\n  ## This can be used to avoid many plugins querying things like sysfs at the\n  ## same time, which can have a measurable effect on the system.\n  collection_jitter = \"0s\"\n\n  ## Default flushing interval for all outputs. Maximum flush_interval will be\n  ## flush_interval + flush_jitter\n  flush_interval = \"10s\"\n  ## Jitter the flush interval by a random amount. This is primarily to avoid\n  ## large write spikes for users running a large number of telegraf instances.\n  ## ie, a jitter of 5s and interval 10s means flushes will happen every 10-15s\n  flush_jitter = \"0s\"\n\n  ## By default or when set to \"0s\", precision will be set to the same\n  ## timestamp order as the collection interval, with the maximum being 1s.\n  ##   ie, when interval = \"10s\", precision will be \"1s\"\n  ##       when interval = \"250ms\", precision will be \"1ms\"\n  ## Precision will NOT be used for service inputs. It is up to each individual\n  ## service input to set the timestamp at the appropriate precision.\n  ## Valid time units are \"ns\", \"us\" (or \"µs\"), \"ms\", \"s\".\n  precision = \"\"\n\n  ## Log at debug level.\n  # debug = false\n  ## Log only error level messages.\n  # quiet = false\n\n  ## Log target controls the destination for logs and can be one of \"file\",\n  ## \"stderr\" or, on Windows, \"eventlog\".  When set to \"file\", the output file\n  ## is determined by the \"logfile\" setting.\n  # logtarget = \"file\"\n\n  ## Name of the file to be logged to when using the \"file\" logtarget.  If set to\n  ## the empty string then logs are written to stderr.\n  # logfile = \"\"\n\n  ## The logfile will be rotated after the time interval specified.  When set\n  ## to 0 no time based rotation is performed.  Logs are rotated only when\n  ## written to, if there is no log activity rotation may be delayed.\n  # logfile_rotation_interval = \"0d\"\n\n  ## The logfile will be rotated when it becomes larger than the specified\n  ## size.  When set to 0 no size based rotation is performed.\n  # logfile_rotation_max_size = \"0MB\"\n\n  ## Maximum number of rotated archives to keep, any older logs are deleted.\n  ## If set to -1, no archives are removed.\n  # logfile_rotation_max_archives = 5\n\n  ## Pick a timezone to use when logging or type 'local' for local time.\n  ## Example: America/Chicago\n  # log_with_timezone = \"\"\n\n  ## Override default hostname, if empty use os.Hostname()\n  hostname = \"\"\n  ## If set to true, do no set the \"host\" tag in the telegraf agent.\n  omit_hostname = false\n# Configuration for sending metrics to InfluxDB\n[[outputs.influxdb_v2]]\n  # alias=\"influxdb_v2\"\n  ## The URLs of the InfluxDB cluster nodes.\n  ##\n  ## Multiple URLs can be specified for a single cluster, only ONE of the\n  ## urls will be written to each interval.\n  ##   ex: urls = [\"https://us-west-2-1.aws.cloud2.influxdata.com\"]\n  urls = [\"http://127.0.0.1:9999\"]\n\n  ## Token for authentication.\n  token = \"\"\n\n  ## Organization is the name of the organization you wish to write to; must exist.\n  organization = \"\"\n\n  ## Destination bucket to write into.\n  bucket = \"\"\n\n  ## The value of this tag will be used to determine the bucket.  If this\n  ## tag is not set the 'bucket' option is used as the default.\n  # bucket_tag = \"\"\n\n  ## If true, the bucket tag will not be added to the metric.\n  # exclude_bucket_tag = false\n\n  ## Timeout for HTTP messages.\n  # timeout = \"5s\"\n\n  ## Additional HTTP headers\n  # http_headers = {\"X-Special-Header\" = \"Special-Value\"}\n\n  ## HTTP Proxy override, if unset values the standard proxy environment\n  ## variables are consulted to determine which proxy, if any, should be used.\n  # http_proxy = \"http://corporate.proxy:3128\"\n\n  ## HTTP User-Agent\n  # user_agent = \"telegraf\"\n\n  ## Content-Encoding for write request body, can be set to \"gzip\" to\n  ## compress body or \"identity\" to apply no encoding.\n  # content_encoding = \"gzip\"\n\n  ## Enable or disable uint support for writing uints influxdb 2.0.\n  # influx_uint_support = false\n\n  ## Optional TLS Config for use on HTTP connections.\n  # tls_ca = \"/etc/telegraf/ca.pem\"\n  # tls_cert = \"/etc/telegraf/cert.pem\"\n  # tls_key = \"/etc/telegraf/key.pem\"\n  ## Use TLS but skip chain & host verification\n  # insecure_skip_verify = false\n\n",
			},
			expMeta: map[string]interface{}{"buckets": []string{}},
		},
		{
			name: "conflict",
			src:  []byte(`{"id":"020f755c3c082000","organizationID":"020f755c3c082222","orgID":"020f755c3c082223","plugins":[]}`),
			cfg: &TelegrafConfig{
				ID:     *id1,
				OrgID:  *id3,
				Config: "# Configuration for telegraf agent\n[agent]\n  ## Default data collection interval for all inputs\n  interval = \"10s\"\n  ## Rounds collection interval to 'interval'\n  ## ie, if interval=\"10s\" then always collect on :00, :10, :20, etc.\n  round_interval = true\n\n  ## Telegraf will send metrics to outputs in batches of at most\n  ## metric_batch_size metrics.\n  ## This controls the size of writes that Telegraf sends to output plugins.\n  metric_batch_size = 1000\n\n  ## Maximum number of unwritten metrics per output.  Increasing this value\n  ## allows for longer periods of output downtime without dropping metrics at the\n  ## cost of higher maximum memory usage.\n  metric_buffer_limit = 10000\n\n  ## Collection jitter is used to jitter the collection by a random amount.\n  ## Each plugin will sleep for a random time within jitter before collecting.\n  ## This can be used to avoid many plugins querying things like sysfs at the\n  ## same time, which can have a measurable effect on the system.\n  collection_jitter = \"0s\"\n\n  ## Default flushing interval for all outputs. Maximum flush_interval will be\n  ## flush_interval + flush_jitter\n  flush_interval = \"10s\"\n  ## Jitter the flush interval by a random amount. This is primarily to avoid\n  ## large write spikes for users running a large number of telegraf instances.\n  ## ie, a jitter of 5s and interval 10s means flushes will happen every 10-15s\n  flush_jitter = \"0s\"\n\n  ## By default or when set to \"0s\", precision will be set to the same\n  ## timestamp order as the collection interval, with the maximum being 1s.\n  ##   ie, when interval = \"10s\", precision will be \"1s\"\n  ##       when interval = \"250ms\", precision will be \"1ms\"\n  ## Precision will NOT be used for service inputs. It is up to each individual\n  ## service input to set the timestamp at the appropriate precision.\n  ## Valid time units are \"ns\", \"us\" (or \"µs\"), \"ms\", \"s\".\n  precision = \"\"\n\n  ## Log at debug level.\n  # debug = false\n  ## Log only error level messages.\n  # quiet = false\n\n  ## Log target controls the destination for logs and can be one of \"file\",\n  ## \"stderr\" or, on Windows, \"eventlog\".  When set to \"file\", the output file\n  ## is determined by the \"logfile\" setting.\n  # logtarget = \"file\"\n\n  ## Name of the file to be logged to when using the \"file\" logtarget.  If set to\n  ## the empty string then logs are written to stderr.\n  # logfile = \"\"\n\n  ## The logfile will be rotated after the time interval specified.  When set\n  ## to 0 no time based rotation is performed.  Logs are rotated only when\n  ## written to, if there is no log activity rotation may be delayed.\n  # logfile_rotation_interval = \"0d\"\n\n  ## The logfile will be rotated when it becomes larger than the specified\n  ## size.  When set to 0 no size based rotation is performed.\n  # logfile_rotation_max_size = \"0MB\"\n\n  ## Maximum number of rotated archives to keep, any older logs are deleted.\n  ## If set to -1, no archives are removed.\n  # logfile_rotation_max_archives = 5\n\n  ## Pick a timezone to use when logging or type 'local' for local time.\n  ## Example: America/Chicago\n  # log_with_timezone = \"\"\n\n  ## Override default hostname, if empty use os.Hostname()\n  hostname = \"\"\n  ## If set to true, do no set the \"host\" tag in the telegraf agent.\n  omit_hostname = false\n# Configuration for sending metrics to InfluxDB\n[[outputs.influxdb_v2]]\n  # alias=\"influxdb_v2\"\n  ## The URLs of the InfluxDB cluster nodes.\n  ##\n  ## Multiple URLs can be specified for a single cluster, only ONE of the\n  ## urls will be written to each interval.\n  ##   ex: urls = [\"https://us-west-2-1.aws.cloud2.influxdata.com\"]\n  urls = [\"http://127.0.0.1:9999\"]\n\n  ## Token for authentication.\n  token = \"\"\n\n  ## Organization is the name of the organization you wish to write to; must exist.\n  organization = \"\"\n\n  ## Destination bucket to write into.\n  bucket = \"\"\n\n  ## The value of this tag will be used to determine the bucket.  If this\n  ## tag is not set the 'bucket' option is used as the default.\n  # bucket_tag = \"\"\n\n  ## If true, the bucket tag will not be added to the metric.\n  # exclude_bucket_tag = false\n\n  ## Timeout for HTTP messages.\n  # timeout = \"5s\"\n\n  ## Additional HTTP headers\n  # http_headers = {\"X-Special-Header\" = \"Special-Value\"}\n\n  ## HTTP Proxy override, if unset values the standard proxy environment\n  ## variables are consulted to determine which proxy, if any, should be used.\n  # http_proxy = \"http://corporate.proxy:3128\"\n\n  ## HTTP User-Agent\n  # user_agent = \"telegraf\"\n\n  ## Content-Encoding for write request body, can be set to \"gzip\" to\n  ## compress body or \"identity\" to apply no encoding.\n  # content_encoding = \"gzip\"\n\n  ## Enable or disable uint support for writing uints influxdb 2.0.\n  # influx_uint_support = false\n\n  ## Optional TLS Config for use on HTTP connections.\n  # tls_ca = \"/etc/telegraf/ca.pem\"\n  # tls_cert = \"/etc/telegraf/cert.pem\"\n  # tls_key = \"/etc/telegraf/key.pem\"\n  ## Use TLS but skip chain & host verification\n  # insecure_skip_verify = false\n\n",
			},
			expMeta: map[string]interface{}{"buckets": []string{}},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := new(TelegrafConfig)
			err := json.Unmarshal(c.src, got)
			if diff := cmp.Diff(err, c.err); diff != "" {
				t.Fatalf("%s decode failed, got err: %v, should be %v", c.name, err, c.err)
			}

			c.cfg.Metadata = c.expMeta
			// todo:this might not set buckets but should
			if diff := cmp.Diff(got, c.cfg, telegrafCmpOptions...); c.err == nil && diff != "" {
				t.Errorf("failed %s, telegraf configs are different -got/+want\ndiff %s", c.name, diff)
			}
		})
	}
}

func TestTelegrafConfigJSON(t *testing.T) {
	id1, _ := platform.IDFromString("020f755c3c082000")
	id2, _ := platform.IDFromString("020f755c3c082222")
	cases := []struct {
		name   string
		expect *TelegrafConfig
		cfg    string
		err    error
	}{
		{
			name: "regular config",
			cfg: fmt.Sprintf(`{
				"ID":    "%v",
				"OrgID": "%v",
				"Name":  "n1",
				"Agent": {
					"Interval": 4000
				},
				"Plugins": [
					{
						"name": "file",
						"type": "input",
						"comment": "comment1",
						"config": {
							"files": ["f1", "f2"]
						}
					},
					{
						"name": "cpu",
						"type": "input",
						"comment": "comment2",
						"config": {}
					},
					{
						"name": "file",
						"type": "output",
						"comment": "comment3",
						"config": {
							"files": [
								{
									"type": "stdout"
								}
							]
						}
					},
					{
						"name": "influxdb_v2",
						"type": "output",
						"comment": "comment4",
						"config": {
							"URLs":  ["url1", "url2"],
							"Token": "tok1"
						}
					}
				]
			}`, id1, id2),
			expect: &TelegrafConfig{
				ID:       *id1,
				OrgID:    *id2,
				Name:     "n1",
				Config:   "# Configuration for telegraf agent\n[agent]\n  ## Default data collection interval for all inputs\n  interval = \"10s\"\n  ## Rounds collection interval to 'interval'\n  ## ie, if interval=\"10s\" then always collect on :00, :10, :20, etc.\n  round_interval = true\n\n  ## Telegraf will send metrics to outputs in batches of at most\n  ## metric_batch_size metrics.\n  ## This controls the size of writes that Telegraf sends to output plugins.\n  metric_batch_size = 1000\n\n  ## Maximum number of unwritten metrics per output.  Increasing this value\n  ## allows for longer periods of output downtime without dropping metrics at the\n  ## cost of higher maximum memory usage.\n  metric_buffer_limit = 10000\n\n  ## Collection jitter is used to jitter the collection by a random amount.\n  ## Each plugin will sleep for a random time within jitter before collecting.\n  ## This can be used to avoid many plugins querying things like sysfs at the\n  ## same time, which can have a measurable effect on the system.\n  collection_jitter = \"0s\"\n\n  ## Default flushing interval for all outputs. Maximum flush_interval will be\n  ## flush_interval + flush_jitter\n  flush_interval = \"10s\"\n  ## Jitter the flush interval by a random amount. This is primarily to avoid\n  ## large write spikes for users running a large number of telegraf instances.\n  ## ie, a jitter of 5s and interval 10s means flushes will happen every 10-15s\n  flush_jitter = \"0s\"\n\n  ## By default or when set to \"0s\", precision will be set to the same\n  ## timestamp order as the collection interval, with the maximum being 1s.\n  ##   ie, when interval = \"10s\", precision will be \"1s\"\n  ##       when interval = \"250ms\", precision will be \"1ms\"\n  ## Precision will NOT be used for service inputs. It is up to each individual\n  ## service input to set the timestamp at the appropriate precision.\n  ## Valid time units are \"ns\", \"us\" (or \"µs\"), \"ms\", \"s\".\n  precision = \"\"\n\n  ## Log at debug level.\n  # debug = false\n  ## Log only error level messages.\n  # quiet = false\n\n  ## Log target controls the destination for logs and can be one of \"file\",\n  ## \"stderr\" or, on Windows, \"eventlog\".  When set to \"file\", the output file\n  ## is determined by the \"logfile\" setting.\n  # logtarget = \"file\"\n\n  ## Name of the file to be logged to when using the \"file\" logtarget.  If set to\n  ## the empty string then logs are written to stderr.\n  # logfile = \"\"\n\n  ## The logfile will be rotated after the time interval specified.  When set\n  ## to 0 no time based rotation is performed.  Logs are rotated only when\n  ## written to, if there is no log activity rotation may be delayed.\n  # logfile_rotation_interval = \"0d\"\n\n  ## The logfile will be rotated when it becomes larger than the specified\n  ## size.  When set to 0 no size based rotation is performed.\n  # logfile_rotation_max_size = \"0MB\"\n\n  ## Maximum number of rotated archives to keep, any older logs are deleted.\n  ## If set to -1, no archives are removed.\n  # logfile_rotation_max_archives = 5\n\n  ## Pick a timezone to use when logging or type 'local' for local time.\n  ## Example: America/Chicago\n  # log_with_timezone = \"\"\n\n  ## Override default hostname, if empty use os.Hostname()\n  hostname = \"\"\n  ## If set to true, do no set the \"host\" tag in the telegraf agent.\n  omit_hostname = false\n[[inputs.file]]\n  ## Files to parse each interval.  Accept standard unix glob matching rules,\n  ## as well as ** to match recursive files and directories.\n  files = [\"f1\", \"f2\"]\n\n  ## Name a tag containing the name of the file the data was parsed from.  Leave empty\n  ## to disable.\n  # file_tag = \"\"\n\n  ## Character encoding to use when interpreting the file contents.  Invalid\n  ## characters are replaced using the unicode replacement character.  When set\n  ## to the empty string the data is not decoded to text.\n  ##   ex: character_encoding = \"utf-8\"\n  ##       character_encoding = \"utf-16le\"\n  ##       character_encoding = \"utf-16be\"\n  ##       character_encoding = \"\"\n  # character_encoding = \"\"\n\n  ## The dataformat to be read from files\n  ## Each data format has its own unique set of configuration options, read\n  ## more about them here:\n  ## https://github.com/influxdata/telegraf/blob/master/docs/DATA_FORMATS_INPUT.md\n  data_format = \"influx\"\n[[inputs.cpu]]\n  ## Whether to report per-cpu stats or not\n  percpu = true\n  ## Whether to report total system cpu stats or not\n  totalcpu = true\n  ## If true, collect raw CPU time metrics\n  collect_cpu_time = false\n  ## If true, compute and report the sum of all non-idle CPU states\n  report_active = false\n[[outputs.file]]\n  ## Files to write to, \"stdout\" is a specially handled file.\n  files = [\"stdout\"]\n\n  ## Use batch serialization format instead of line based delimiting.  The\n  ## batch format allows for the production of non line based output formats and\n  ## may more efficiently encode metric groups.\n  # use_batch_format = false\n\n  ## The file will be rotated after the time interval specified.  When set\n  ## to 0 no time based rotation is performed.\n  # rotation_interval = \"0d\"\n\n  ## The logfile will be rotated when it becomes larger than the specified\n  ## size.  When set to 0 no size based rotation is performed.\n  # rotation_max_size = \"0MB\"\n\n  ## Maximum number of rotated archives to keep, any older logs are deleted.\n  ## If set to -1, no archives are removed.\n  # rotation_max_archives = 5\n\n  ## Data format to output.\n  ## Each data format has its own unique set of configuration options, read\n  ## more about them here:\n  ## https://github.com/influxdata/telegraf/blob/master/docs/DATA_FORMATS_OUTPUT.md\n  data_format = \"influx\"\n[[outputs.influxdb_v2]]\n  ## The URLs of the InfluxDB cluster nodes.\n  ##\n  ## Multiple URLs can be specified for a single cluster, only ONE of the\n  ## urls will be written to each interval.\n  ##   ex: urls = [\"https://us-west-2-1.aws.cloud2.influxdata.com\"]\n  urls = [\"url1\", \"url2\"]\n\n  ## Token for authentication.\n  token = \"tok1\"\n\n  ## Organization is the name of the organization you wish to write to; must exist.\n  organization = \"\"\n\n  ## Destination bucket to write into.\n  bucket = \"\"\n\n  ## The value of this tag will be used to determine the bucket.  If this\n  ## tag is not set the 'bucket' option is used as the default.\n  # bucket_tag = \"\"\n\n  ## If true, the bucket tag will not be added to the metric.\n  # exclude_bucket_tag = false\n\n  ## Timeout for HTTP messages.\n  # timeout = \"5s\"\n\n  ## Additional HTTP headers\n  # http_headers = {\"X-Special-Header\" = \"Special-Value\"}\n\n  ## HTTP Proxy override, if unset values the standard proxy environment\n  ## variables are consulted to determine which proxy, if any, should be used.\n  # http_proxy = \"http://corporate.proxy:3128\"\n\n  ## HTTP User-Agent\n  # user_agent = \"telegraf\"\n\n  ## Content-Encoding for write request body, can be set to \"gzip\" to\n  ## compress body or \"identity\" to apply no encoding.\n  # content_encoding = \"gzip\"\n\n  ## Enable or disable uint support for writing uints influxdb 2.0.\n  # influx_uint_support = false\n\n  ## Optional TLS Config for use on HTTP connections.\n  # tls_ca = \"/etc/telegraf/ca.pem\"\n  # tls_cert = \"/etc/telegraf/cert.pem\"\n  # tls_key = \"/etc/telegraf/key.pem\"\n  ## Use TLS but skip chain & host verification\n  # insecure_skip_verify = false\n",
				Metadata: map[string]interface{}{"buckets": []string{}},
			},
		},
		{
			name: "unsupported plugin type",
			cfg: fmt.Sprintf(`{
				"ID":    "%v",
				"OrgID": "%v",
				"Name":  "n1",
				"Plugins": [
					{
						"name": "bad_type",
						"type": "aggregator",
						"Comment": "comment3",
						"Config": {
							"Field": "f1"
						}
					}
				]
			}`, id1, id2),
			err: &errors.Error{
				Code: errors.EInvalid,
				Msg:  fmt.Sprintf(ErrUnsupportTelegrafPluginType, "aggregator"),
				Op:   "unmarshal telegraf config raw plugin",
			},
		},
		{
			name: "unsupported plugin",
			cfg: fmt.Sprintf(`{
				"ID":    "%v",
				"OrgID": "%v",
				"Name":  "n1",
				"Plugins": [
					{
						"name": "kafka",
						"type": "output",
						"Config": {
							"Field": "f2"
						}
					}
				]
			}`, id1, id2),
			err: &errors.Error{
				Code: errors.EInvalid,
				Msg:  fmt.Sprintf(ErrUnsupportTelegrafPluginName, "kafka", plugins.Output),
				Op:   "unmarshal telegraf config raw plugin",
			},
		},
	}
	for _, c := range cases {
		got := new(TelegrafConfig)
		err := json.Unmarshal([]byte(c.cfg), got)
		if diff := cmp.Diff(err, c.err); diff != "" {
			t.Fatalf("%s decode failed, got err: %v, should be %v", c.name, err, c.err)
		}

		if diff := cmp.Diff(got, c.expect, telegrafCmpOptions...); c.err == nil && diff != "" {
			t.Errorf("failed %s, telegraf configs are different -got/+want\ndiff %s", c.name, diff)
		}
	}
}

func TestLegacyStruct(t *testing.T) {
	id1, _ := platform.IDFromString("020f755c3c082000")

	telConfOld := fmt.Sprintf(`{
		"id":   "%v",
		"name": "n1",
		"agent": {
			"interval": 10000
		},
		"plugins": [
			{
				"name": "file",
				"type": "input",
				"comment": "comment1",
				"config": {
					"files": ["f1", "f2"]
				}
			},
			{
				"name":  "cpu",
				"type": "input",
				"comment": "comment2"
			},
			{
				"name": "file",
				"type": "output",
				"comment": "comment3",
				"config": {"files": [
					{"type": "stdout"}
				]}
			},
			{
				"name": "influxdb_v2",
				"type": "output",
				"comment": "comment4",
				"config": {
					"urls": [
						"url1",
						"url2"
					],
					"token":        "token1",
					"organization": "org1",
					"bucket":       "bucket1"
				}
			}
		]
	}`, id1)
	want := `# Configuration for telegraf agent
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
[[inputs.file]]
  ## Files to parse each interval.  Accept standard unix glob matching rules,
  ## as well as ** to match recursive files and directories.
  files = ["f1", "f2"]

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
[[inputs.cpu]]
  ## Whether to report per-cpu stats or not
  percpu = true
  ## Whether to report total system cpu stats or not
  totalcpu = true
  ## If true, collect raw CPU time metrics
  collect_cpu_time = false
  ## If true, compute and report the sum of all non-idle CPU states
  report_active = false
[[outputs.file]]
  ## Files to write to, "stdout" is a specially handled file.
  files = ["stdout"]

  ## Use batch serialization format instead of line based delimiting.  The
  ## batch format allows for the production of non line based output formats and
  ## may more efficiently encode metric groups.
  # use_batch_format = false

  ## The file will be rotated after the time interval specified.  When set
  ## to 0 no time based rotation is performed.
  # rotation_interval = "0d"

  ## The logfile will be rotated when it becomes larger than the specified
  ## size.  When set to 0 no size based rotation is performed.
  # rotation_max_size = "0MB"

  ## Maximum number of rotated archives to keep, any older logs are deleted.
  ## If set to -1, no archives are removed.
  # rotation_max_archives = 5

  ## Data format to output.
  ## Each data format has its own unique set of configuration options, read
  ## more about them here:
  ## https://github.com/influxdata/telegraf/blob/master/docs/DATA_FORMATS_OUTPUT.md
  data_format = "influx"
[[outputs.influxdb_v2]]
  ## The URLs of the InfluxDB cluster nodes.
  ##
  ## Multiple URLs can be specified for a single cluster, only ONE of the
  ## urls will be written to each interval.
  ##   ex: urls = ["https://us-west-2-1.aws.cloud2.influxdata.com"]
  urls = ["url1", "url2"]

  ## Token for authentication.
  token = "token1"

  ## Organization is the name of the organization you wish to write to; must exist.
  organization = "org1"

  ## Destination bucket to write into.
  bucket = "bucket1"

  ## The value of this tag will be used to determine the bucket.  If this
  ## tag is not set the 'bucket' option is used as the default.
  # bucket_tag = ""

  ## If true, the bucket tag will not be added to the metric.
  # exclude_bucket_tag = false

  ## Timeout for HTTP messages.
  # timeout = "5s"

  ## Additional HTTP headers
  # http_headers = {"X-Special-Header" = "Special-Value"}

  ## HTTP Proxy override, if unset values the standard proxy environment
  ## variables are consulted to determine which proxy, if any, should be used.
  # http_proxy = "http://corporate.proxy:3128"

  ## HTTP User-Agent
  # user_agent = "telegraf"

  ## Content-Encoding for write request body, can be set to "gzip" to
  ## compress body or "identity" to apply no encoding.
  # content_encoding = "gzip"

  ## Enable or disable uint support for writing uints influxdb 2.0.
  # influx_uint_support = false

  ## Optional TLS Config for use on HTTP connections.
  # tls_ca = "/etc/telegraf/ca.pem"
  # tls_cert = "/etc/telegraf/cert.pem"
  # tls_key = "/etc/telegraf/key.pem"
  ## Use TLS but skip chain & host verification
  # insecure_skip_verify = false
`
	tc := &TelegrafConfig{}
	require.NoError(t, json.Unmarshal([]byte(telConfOld), tc))

	if tc.Config != want {
		t.Fatalf("telegraf config's toml is incorrect, got %+v", tc.Config)
	}
}

func TestCountPlugins(t *testing.T) {
	tc := TelegrafConfig{
		Name: "test",
		Config: `
[[inputs.file]]
  some = "config"
[[inputs.file]]
  some = "config"
[[outputs.influxdb_v2]]
  some = "config"
[[inputs.cpu]]
  some = "config"
[[outputs.stuff]]
  some = "config"
[[aggregators.thing]]
  some = "config"
[[processors.thing]]
  some = "config"
[[serializers.thing]]
  some = "config"
[[inputs.file]]
  some = "config"
`,
	}

	pCount := tc.CountPlugins()

	require.Equal(t, 6, len(pCount))

	require.Equal(t, float64(3), pCount["inputs.file"])
}
