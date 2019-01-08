package influxdb

import (
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"testing"

	"github.com/BurntSushi/toml"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/influxdata/influxdb/telegraf/plugins"
	"github.com/influxdata/influxdb/telegraf/plugins/inputs"
	"github.com/influxdata/influxdb/telegraf/plugins/outputs"
)

var telegrafCmpOptions = cmp.Options{
	cmpopts.IgnoreUnexported(
		inputs.CPUStats{},
		inputs.Kubernetes{},
		inputs.File{},
		outputs.File{},
		outputs.InfluxDBV2{},
		unsupportedPlugin{},
	),
	cmp.Transformer("Sort", func(in []*TelegrafConfig) []*TelegrafConfig {
		out := append([]*TelegrafConfig(nil), in...)
		sort.Slice(out, func(i, j int) bool {
			return out[i].ID > out[j].ID
		})
		return out
	}),
}

type unsupportedPluginType struct {
	Field string `json:"field"`
}

func (u *unsupportedPluginType) TOML() string {
	return ""
}

func (u *unsupportedPluginType) PluginName() string {
	return "bad_type"
}

func (u *unsupportedPluginType) Type() plugins.Type {
	return plugins.Aggregator
}

func (u *unsupportedPluginType) UnmarshalTOML(data interface{}) error {
	return nil
}

type unsupportedPlugin struct {
	Field string `json:"field"`
}

func (u *unsupportedPlugin) TOML() string {
	return ""
}

func (u *unsupportedPlugin) PluginName() string {
	return "kafka"
}

func (u *unsupportedPlugin) Type() plugins.Type {
	return plugins.Output
}

func (u *unsupportedPlugin) UnmarshalTOML(data interface{}) error {
	return nil
}

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
				"config":{}
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
		Agent: TelegrafAgentConfig{
			Interval: 120000,
		},
		Plugins: []TelegrafPlugin{
			{
				Comment: "cpu collect cpu metrics",
				Config:  &inputs.CPUStats{},
			},
			{
				Config: &inputs.Kubernetes{
					URL: "http://1.1.1.1:12",
				},
			},
			{
				Comment: "3",
				Config: &outputs.InfluxDBV2{
					URLs: []string{
						"http://127.0.0.1:9999",
					},
					Token:        "token1",
					Organization: "org",
					Bucket:       "bucket",
				},
			},
		},
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

func TestTelegrafConfigJSON(t *testing.T) {
	id1, _ := IDFromString("020f755c3c082000")
	id2, _ := IDFromString("020f755c3c082222")
	cases := []struct {
		name string
		cfg  *TelegrafConfig
		err  error
	}{
		{
			name: "regular config",
			cfg: &TelegrafConfig{
				ID:             *id1,
				OrganizationID: *id2,
				Name:           "n1",
				Agent: TelegrafAgentConfig{
					Interval: 4000,
				},
				Plugins: []TelegrafPlugin{
					{
						Comment: "comment1",
						Config: &inputs.File{
							Files: []string{"f1", "f2"},
						},
					},
					{
						Comment: "comment2",
						Config:  &inputs.CPUStats{},
					},
					{
						Comment: "comment3",
						Config: &outputs.File{Files: []outputs.FileConfig{
							{Typ: "stdout"},
						}},
					},
					{
						Comment: "comment4",
						Config: &outputs.InfluxDBV2{
							URLs:  []string{"url1", "url2"},
							Token: "tok1",
						},
					},
				},
			},
		},
		{
			name: "unsupported plugin type",
			cfg: &TelegrafConfig{
				ID:             *id1,
				OrganizationID: *id2,
				Name:           "n1",
				Plugins: []TelegrafPlugin{
					{
						Comment: "comment3",
						Config: &unsupportedPluginType{
							Field: "f1",
						},
					},
				},
			},
			err: &Error{
				Code: EInvalid,
				Msg:  fmt.Sprintf(ErrUnsupportTelegrafPluginType, "aggregator"),
				Op:   "unmarshal telegraf config raw plugin",
			},
		},
		{
			name: "unsupported plugin",
			cfg: &TelegrafConfig{
				ID:             *id1,
				OrganizationID: *id2,
				Name:           "n1",
				Plugins: []TelegrafPlugin{
					{
						Config: &unsupportedPlugin{
							Field: "f2",
						},
					},
				},
			},
			err: &Error{
				Code: EInvalid,
				Msg:  fmt.Sprintf(ErrUnsupportTelegrafPluginName, "kafka", plugins.Output),
				Op:   "unmarshal telegraf config raw plugin",
			},
		},
	}
	for _, c := range cases {
		result, err := json.Marshal(c.cfg)
		// encode testing
		if err != nil {
			t.Fatalf("%s encode failed, got: %v, should be nil", c.name, err)
		}
		got := new(TelegrafConfig)
		err = json.Unmarshal([]byte(result), got)
		if diff := cmp.Diff(err, c.err); diff != "" {
			t.Fatalf("%s decode failed, got err: %v, should be %v", c.name, err, c.err)
		}

		if diff := cmp.Diff(got, c.cfg, telegrafCmpOptions...); c.err == nil && diff != "" {
			t.Errorf("failed %s, telegraf configs are different -got/+want\ndiff %s", c.name, diff)
		}
	}
}

func TestTOML(t *testing.T) {
	id1, _ := IDFromString("020f755c3c082000")

	tc := &TelegrafConfig{
		ID:   *id1,
		Name: "n1",
		Agent: TelegrafAgentConfig{
			Interval: 4000,
		},
		Plugins: []TelegrafPlugin{
			{
				Comment: "comment1",
				Config: &inputs.File{
					Files: []string{"f1", "f2"},
				},
			},
			{
				Comment: "comment2",
				Config:  &inputs.CPUStats{},
			},
			{
				Comment: "comment3",
				Config: &outputs.File{Files: []outputs.FileConfig{
					{Typ: "stdout"},
				}},
			},
			{
				Comment: "comment4",
				Config: &outputs.InfluxDBV2{
					URLs: []string{
						"url1",
						"url2",
					},
					Token:        "token1",
					Organization: "org1",
					Bucket:       "bucket1",
				},
			},
		},
	}
	want := `# Configuration for telegraf agent
[agent]
  ## Default data collection interval for all inputs
  interval = "4s"
  ## Rounds collection interval to 'interval'
  ## ie, if interval="10s" then always collect on :00, :10, :20, etc.
  round_interval = true

  ## Telegraf will send metrics to outputs in batches of at most
  ## metric_batch_size metrics.
  ## This controls the size of writes that Telegraf sends to output plugins.
  metric_batch_size = 1000

  ## For failed writes, telegraf will cache metric_buffer_limit metrics for each
  ## output, and will flush this buffer on a successful write. Oldest metrics
  ## are dropped first when this buffer fills.
  ## This buffer only fills when writes fail to output plugin(s).
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

  ## Logging configuration:
  ## Run telegraf with debug log messages.
  debug = false
  ## Run telegraf in quiet mode (error log messages only).
  quiet = false
  ## Specify the log file name. The empty string means to log to stderr.
  logfile = ""

  ## Override default hostname, if empty use os.Hostname()
  hostname = ""
  ## If set to true, do no set the "host" tag in the telegraf agent.
  omit_hostname = false
[[inputs.file]]	
  ## Files to parse each interval.
  ## These accept standard unix glob matching rules, but with the addition of
  ## ** as a "super asterisk". ie:
  ##   /var/log/**.log     -> recursively find all .log files in /var/log
  ##   /var/log/*/*.log    -> find all .log files with a parent dir in /var/log
  ##   /var/log/apache.log -> only read the apache log file
  files = ["f1", "f2"]

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
  ## If true, collect raw CPU time metrics.
  collect_cpu_time = false
  ## If true, compute and report the sum of all non-idle CPU states.
  report_active = false
[[outputs.file]]
  ## Files to write to, "stdout" is a specially handled file.
  files = ["stdout"]
[[outputs.influxdb_v2]]	
  ## The URLs of the InfluxDB cluster nodes.
  ##
  ## Multiple URLs can be specified for a single cluster, only ONE of the
  ## urls will be written to each interval.
  ## urls exp: http://127.0.0.1:9999
  urls = ["url1", "url2"]

  ## Token for authentication.
  token = "token1"

  ## Organization is the name of the organization you wish to write to; must exist.
  organization = "org1"

  ## Destination bucket to write into.
  bucket = "bucket1"
`
	if result := tc.TOML(); result != want {
		t.Fatalf("telegraf config's toml is incorrect, got %s", result)
	}
	tcr := new(TelegrafConfig)
	err := toml.Unmarshal([]byte(want), tcr)
	if err != nil {
		t.Fatalf("telegraf toml parsing issue %s", err.Error())
	}
	if reflect.DeepEqual(tcr, tc) {
		t.Fatalf("telegraf toml parsing issue, want %q, got %q", tc, tcr)
	}
}
