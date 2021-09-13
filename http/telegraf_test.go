package http

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	platform "github.com/influxdata/influxdb/v2"
	platform2 "github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/influxdata/influxdb/v2/mock"
	"go.uber.org/zap/zaptest"
)

// NewMockTelegrafBackend returns a TelegrafBackend with mock services.
func NewMockTelegrafBackend(t *testing.T) *TelegrafBackend {
	return &TelegrafBackend{
		log: zaptest.NewLogger(t),

		TelegrafService:            &mock.TelegrafConfigStore{},
		UserResourceMappingService: mock.NewUserResourceMappingService(),
		LabelService:               mock.NewLabelService(),
		UserService:                mock.NewUserService(),
		OrganizationService:        mock.NewOrganizationService(),
	}
}

func TestTelegrafHandler_handleGetTelegrafs(t *testing.T) {
	type wants struct {
		statusCode  int
		contentType string
		body        string
	}
	tests := []struct {
		name  string
		svc   *mock.TelegrafConfigStore
		r     *http.Request
		wants wants
	}{
		{
			name: "get telegraf configs by organization id",
			r:    httptest.NewRequest("GET", "http://any.url/api/v2/telegrafs?orgID=0000000000000002", nil),
			svc: &mock.TelegrafConfigStore{
				FindTelegrafConfigsF: func(ctx context.Context, filter platform.TelegrafConfigFilter, opt ...platform.FindOptions) ([]*platform.TelegrafConfig, int, error) {
					if filter.OrgID != nil && *filter.OrgID == platform2.ID(2) {
						return []*platform.TelegrafConfig{
							{
								ID:          platform2.ID(1),
								OrgID:       platform2.ID(2),
								Name:        "tc1",
								Description: "",
								Config:      "[[inputs.cpu]]\n",
							},
						}, 1, nil
					}

					return []*platform.TelegrafConfig{}, 0, fmt.Errorf("not found")
				},
			},
			wants: wants{
				statusCode:  http.StatusOK,
				contentType: "application/json; charset=utf-8",
				body: `{
					"configurations": [
						{
							"id": "0000000000000001",
							"orgID": "0000000000000002",
							"name": "tc1",
							"config": "[[inputs.cpu]]\n",
							"labels": [],
							"links": {
								"self": "/api/v2/telegrafs/0000000000000001",
								"labels": "/api/v2/telegrafs/0000000000000001/labels",
								"members": "/api/v2/telegrafs/0000000000000001/members",
								"owners": "/api/v2/telegrafs/0000000000000001/owners"
							}
						}
					]
				}`,
			},
		},
		{
			name: "get telegraf configs by organization name",
			r:    httptest.NewRequest("GET", "http://any.url/api/v2/telegrafs?org=tc1", nil),
			svc: &mock.TelegrafConfigStore{
				FindTelegrafConfigsF: func(ctx context.Context, filter platform.TelegrafConfigFilter, opt ...platform.FindOptions) ([]*platform.TelegrafConfig, int, error) {
					if filter.Organization != nil && *filter.Organization == "tc1" {
						return []*platform.TelegrafConfig{
							{
								ID:          platform2.ID(1),
								OrgID:       platform2.ID(2),
								Name:        "tc1",
								Description: "",
								Config:      "[[inputs.cpu]]\n",
							},
						}, 1, nil
					}

					return []*platform.TelegrafConfig{}, 0, fmt.Errorf("not found")
				},
			},
			wants: wants{
				statusCode:  http.StatusOK,
				contentType: "application/json; charset=utf-8",
				body: `{
					"configurations": [
						{
							"id": "0000000000000001",
							"orgID": "0000000000000002",
							"name": "tc1",
							"config": "[[inputs.cpu]]\n",
							"labels": [],
							"links": {
								"self": "/api/v2/telegrafs/0000000000000001",
								"labels": "/api/v2/telegrafs/0000000000000001/labels",
								"members": "/api/v2/telegrafs/0000000000000001/members",
								"owners": "/api/v2/telegrafs/0000000000000001/owners"
							}
						}
					]
				}`,
			},
		},
		{
			name: "return CPU plugin for telegraf",
			r:    httptest.NewRequest("GET", "http://any.url/api/v2/telegrafs", nil),
			svc: &mock.TelegrafConfigStore{
				FindTelegrafConfigsF: func(ctx context.Context, filter platform.TelegrafConfigFilter, opt ...platform.FindOptions) ([]*platform.TelegrafConfig, int, error) {
					return []*platform.TelegrafConfig{
						{
							ID:          platform2.ID(1),
							OrgID:       platform2.ID(2),
							Name:        "my config",
							Description: "my description",
							Config:      "[[inputs.cpu]]\n[[outputs.influxdb_v2]]\n",
						},
					}, 1, nil
				},
			},
			wants: wants{
				statusCode:  http.StatusOK,
				contentType: "application/json; charset=utf-8",
				body: `{
					"configurations": [
						{
							"id": "0000000000000001",
							"orgID": "0000000000000002",
							"name": "my config",
							"description": "my description",
							"config": "[[inputs.cpu]]\n[[outputs.influxdb_v2]]\n",
							"labels": [],
							"links": {
								"self": "/api/v2/telegrafs/0000000000000001",
								"labels": "/api/v2/telegrafs/0000000000000001/labels",
								"members": "/api/v2/telegrafs/0000000000000001/members",
								"owners": "/api/v2/telegrafs/0000000000000001/owners"
							}
						}
					]
				}`,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := httptest.NewRecorder()
			telegrafBackend := NewMockTelegrafBackend(t)
			telegrafBackend.TelegrafService = tt.svc
			h := NewTelegrafHandler(zaptest.NewLogger(t), telegrafBackend)
			h.ServeHTTP(w, tt.r)

			res := w.Result()
			content := res.Header.Get("Content-Type")
			body, _ := ioutil.ReadAll(res.Body)

			if res.StatusCode != tt.wants.statusCode {
				t.Errorf("%q. handleGetTelegrafs() = %v, want %v", tt.name, res.StatusCode, tt.wants.statusCode)
				t.Logf("headers: %v", res.Header)
			}
			if tt.wants.contentType != "" && content != tt.wants.contentType {
				t.Errorf("%q. handleGetTelegrafs() = %v, want %v", tt.name, content, tt.wants.contentType)
			}
			if tt.wants.body != "" {
				if eq, diff, err := jsonEqual(string(body), tt.wants.body); err != nil {
					t.Errorf("%q, handleGetTelegrafs(). error unmarshalling json %v", tt.name, err)
				} else if !eq {
					t.Errorf("%q. handleGetTelegrafs() = ***%s***", tt.name, diff)
				}
			}
		})
	}
}

func TestTelegrafHandler_handleGetTelegraf(t *testing.T) {
	type wants struct {
		statusCode  int
		contentType string
		body        string
	}
	tests := []struct {
		name         string
		svc          *mock.TelegrafConfigStore
		r            *http.Request
		acceptHeader string
		wants        wants
	}{
		{
			name:         "return JSON telegraf config",
			r:            httptest.NewRequest("GET", "http://any.url/api/v2/telegrafs/0000000000000001", nil),
			acceptHeader: "application/json",
			svc: &mock.TelegrafConfigStore{
				FindTelegrafConfigByIDF: func(ctx context.Context, id platform2.ID) (*platform.TelegrafConfig, error) {
					return &platform.TelegrafConfig{
						ID:          platform2.ID(1),
						OrgID:       platform2.ID(2),
						Name:        "my config",
						Description: "",
						Config:      "[[inputs.cpu]]\n[[outputs.influxdb_v2]]\n",
					}, nil
				},
			},
			wants: wants{
				statusCode:  http.StatusOK,
				contentType: "application/json; charset=utf-8",
				body: `{
					"id": "0000000000000001",
					"orgID": "0000000000000002",
					"name": "my config",
					"config": "[[inputs.cpu]]\n[[outputs.influxdb_v2]]\n",
					"labels": [],
					"links": {
						"self": "/api/v2/telegrafs/0000000000000001",
						"labels": "/api/v2/telegrafs/0000000000000001/labels",
						"members": "/api/v2/telegrafs/0000000000000001/members",
						"owners": "/api/v2/telegrafs/0000000000000001/owners"
					}
				}`,
			},
		},
		{
			name:         "return JSON telegraf config using fuzzy accept header matching",
			r:            httptest.NewRequest("GET", "http://any.url/api/v2/telegrafs/0000000000000001", nil),
			acceptHeader: "application/json, text/plain, */*",
			svc: &mock.TelegrafConfigStore{
				FindTelegrafConfigByIDF: func(ctx context.Context, id platform2.ID) (*platform.TelegrafConfig, error) {
					return &platform.TelegrafConfig{
						ID:          platform2.ID(1),
						OrgID:       platform2.ID(2),
						Name:        "my config",
						Description: "",
						Config:      "[[inputs.cpu]]\n[[outputs.influxdb_v2]]\n",
					}, nil
				},
			},
			wants: wants{
				statusCode:  http.StatusOK,
				contentType: "application/json; charset=utf-8",
				body: `{
					"id": "0000000000000001",
					"orgID": "0000000000000002",
					"name": "my config",
					"config": "[[inputs.cpu]]\n[[outputs.influxdb_v2]]\n",
					"labels": [],
					"links": {
						"self": "/api/v2/telegrafs/0000000000000001",
						"labels": "/api/v2/telegrafs/0000000000000001/labels",
						"members": "/api/v2/telegrafs/0000000000000001/members",
						"owners": "/api/v2/telegrafs/0000000000000001/owners"
					}
				}`,
			},
		},
		{
			name:         "return TOML telegraf config with accept header application/toml",
			r:            httptest.NewRequest("GET", "http://any.url/api/v2/telegrafs/0000000000000001", nil),
			acceptHeader: "application/toml",
			svc: &mock.TelegrafConfigStore{
				FindTelegrafConfigByIDF: func(ctx context.Context, id platform2.ID) (*platform.TelegrafConfig, error) {
					return &platform.TelegrafConfig{
						ID:    platform2.ID(1),
						OrgID: platform2.ID(2),
						Name:  "my config",
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
[[inputs.cpu]]
  ## Whether to report per-cpu stats or not
  percpu = true
  ## Whether to report total system cpu stats or not
  totalcpu = true
  ## If true, collect raw CPU time metrics.
  collect_cpu_time = false
  ## If true, compute and report the sum of all non-idle CPU states.
  report_active = false
[[outputs.influxdb_v2]]
  ## The URLs of the InfluxDB cluster nodes.
  ##
  ## Multiple URLs can be specified for a single cluster, only ONE of the
  ## urls will be written to each interval.
  ## urls exp: http://127.0.0.1:8086
  urls = ["http://127.0.0.1:8086"]

  ## Token for authentication.
  token = "no_more_secrets"

  ## Organization is the name of the organization you wish to write to; must exist.
  organization = "my_org"

  ## Destination bucket to write into.
  bucket = "my_bucket"
`,
					}, nil
				},
			},
			wants: wants{
				statusCode:  http.StatusOK,
				contentType: "application/toml; charset=utf-8",
				body: `# Configuration for telegraf agent
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
[[inputs.cpu]]
  ## Whether to report per-cpu stats or not
  percpu = true
  ## Whether to report total system cpu stats or not
  totalcpu = true
  ## If true, collect raw CPU time metrics.
  collect_cpu_time = false
  ## If true, compute and report the sum of all non-idle CPU states.
  report_active = false
[[outputs.influxdb_v2]]
  ## The URLs of the InfluxDB cluster nodes.
  ##
  ## Multiple URLs can be specified for a single cluster, only ONE of the
  ## urls will be written to each interval.
  ## urls exp: http://127.0.0.1:8086
  urls = ["http://127.0.0.1:8086"]

  ## Token for authentication.
  token = "no_more_secrets"

  ## Organization is the name of the organization you wish to write to; must exist.
  organization = "my_org"

  ## Destination bucket to write into.
  bucket = "my_bucket"
`,
			},
		},
		{
			name: "return TOML telegraf config with no accept header",
			r:    httptest.NewRequest("GET", "http://any.url/api/v2/telegrafs/0000000000000001", nil),
			svc: &mock.TelegrafConfigStore{
				FindTelegrafConfigByIDF: func(ctx context.Context, id platform2.ID) (*platform.TelegrafConfig, error) {
					return &platform.TelegrafConfig{
						ID:    platform2.ID(1),
						OrgID: platform2.ID(2),
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
[[inputs.cpu]]
  ## Whether to report per-cpu stats or not
  percpu = true
  ## Whether to report total system cpu stats or not
  totalcpu = true
  ## If true, collect raw CPU time metrics.
  collect_cpu_time = false
  ## If true, compute and report the sum of all non-idle CPU states.
  report_active = false
[[outputs.influxdb_v2]]
  ## The URLs of the InfluxDB cluster nodes.
  ##
  ## Multiple URLs can be specified for a single cluster, only ONE of the
  ## urls will be written to each interval.
  ## urls exp: http://127.0.0.1:8086
  urls = ["http://127.0.0.1:8086"]

  ## Token for authentication.
  token = "no_more_secrets"

  ## Organization is the name of the organization you wish to write to; must exist.
  organization = "my_org"

  ## Destination bucket to write into.
  bucket = "my_bucket"
`,
					}, nil
				},
			},
			wants: wants{
				statusCode:  http.StatusOK,
				contentType: "application/toml; charset=utf-8",
				body: `# Configuration for telegraf agent
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
[[inputs.cpu]]
  ## Whether to report per-cpu stats or not
  percpu = true
  ## Whether to report total system cpu stats or not
  totalcpu = true
  ## If true, collect raw CPU time metrics.
  collect_cpu_time = false
  ## If true, compute and report the sum of all non-idle CPU states.
  report_active = false
[[outputs.influxdb_v2]]
  ## The URLs of the InfluxDB cluster nodes.
  ##
  ## Multiple URLs can be specified for a single cluster, only ONE of the
  ## urls will be written to each interval.
  ## urls exp: http://127.0.0.1:8086
  urls = ["http://127.0.0.1:8086"]

  ## Token for authentication.
  token = "no_more_secrets"

  ## Organization is the name of the organization you wish to write to; must exist.
  organization = "my_org"

  ## Destination bucket to write into.
  bucket = "my_bucket"
`,
			},
		},
		{
			name:         "return TOML telegraf config with application/octet-stream",
			r:            httptest.NewRequest("GET", "http://any.url/api/v2/telegrafs/0000000000000001", nil),
			acceptHeader: "application/octet-stream",
			svc: &mock.TelegrafConfigStore{
				FindTelegrafConfigByIDF: func(ctx context.Context, id platform2.ID) (*platform.TelegrafConfig, error) {
					return &platform.TelegrafConfig{
						ID:    platform2.ID(1),
						OrgID: platform2.ID(2),
						Name:  "my config",
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
[[inputs.cpu]]
  ## Whether to report per-cpu stats or not
  percpu = true
  ## Whether to report total system cpu stats or not
  totalcpu = true
  ## If true, collect raw CPU time metrics.
  collect_cpu_time = false
  ## If true, compute and report the sum of all non-idle CPU states.
  report_active = false
[[outputs.influxdb_v2]]
  ## The URLs of the InfluxDB cluster nodes.
  ##
  ## Multiple URLs can be specified for a single cluster, only ONE of the
  ## urls will be written to each interval.
  ## urls exp: http://127.0.0.1:8086
  urls = ["http://127.0.0.1:8086"]

  ## Token for authentication.
  token = "no_more_secrets"

  ## Organization is the name of the organization you wish to write to; must exist.
  organization = "my_org"

  ## Destination bucket to write into.
  bucket = "my_bucket"
`,
					}, nil
				},
			},
			wants: wants{
				statusCode:  http.StatusOK,
				contentType: "application/octet-stream",
				body: `# Configuration for telegraf agent
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
[[inputs.cpu]]
  ## Whether to report per-cpu stats or not
  percpu = true
  ## Whether to report total system cpu stats or not
  totalcpu = true
  ## If true, collect raw CPU time metrics.
  collect_cpu_time = false
  ## If true, compute and report the sum of all non-idle CPU states.
  report_active = false
[[outputs.influxdb_v2]]
  ## The URLs of the InfluxDB cluster nodes.
  ##
  ## Multiple URLs can be specified for a single cluster, only ONE of the
  ## urls will be written to each interval.
  ## urls exp: http://127.0.0.1:8086
  urls = ["http://127.0.0.1:8086"]

  ## Token for authentication.
  token = "no_more_secrets"

  ## Organization is the name of the organization you wish to write to; must exist.
  organization = "my_org"

  ## Destination bucket to write into.
  bucket = "my_bucket"
`,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.r.Header.Set("Accept", tt.acceptHeader)
			w := httptest.NewRecorder()
			telegrafBackend := NewMockTelegrafBackend(t)
			telegrafBackend.TelegrafService = tt.svc
			h := NewTelegrafHandler(zaptest.NewLogger(t), telegrafBackend)

			h.ServeHTTP(w, tt.r)

			res := w.Result()
			content := res.Header.Get("Content-Type")
			body, _ := ioutil.ReadAll(res.Body)

			if res.StatusCode != tt.wants.statusCode {
				t.Errorf("%q. handleGetTelegraf() = %v, want %v", tt.name, res.StatusCode, tt.wants.statusCode)
				t.Logf("headers: %v", res.Header)
			}
			if tt.wants.contentType != "" && content != tt.wants.contentType {
				t.Errorf("%q. handleGetTelegraf() = %v, want %v", tt.name, content, tt.wants.contentType)
				return
			}

			if strings.Contains(tt.wants.contentType, "application/json") {
				if eq, diff, _ := jsonEqual(string(body), tt.wants.body); tt.wants.body != "" && !eq {
					t.Errorf("%q. handleGetTelegraf() = ***%s***", tt.name, diff)
				}
			} else if string(body) != tt.wants.body {
				t.Errorf("%q. handleGetTelegraf() = \n***%v***\nwant\n***%v***", tt.name, string(body), tt.wants.body)
			}
		})
	}
}

func Test_newTelegrafResponses(t *testing.T) {
	type args struct {
		tcs []*platform.TelegrafConfig
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			args: args{
				tcs: []*platform.TelegrafConfig{
					{
						ID:          platform2.ID(1),
						OrgID:       platform2.ID(2),
						Name:        "my config",
						Description: "",
						Config:      "[[inputs.cpu]]\n[[outputs.influxdb_v2]]\n",
					},
				},
			},
			want: `{
				"configurations": [
					{
						"id": "0000000000000001",
						"orgID": "0000000000000002",
						"name": "my config",
						"config": "[[inputs.cpu]]\n[[outputs.influxdb_v2]]\n",
						"labels": [],
						"links": {
							"self": "/api/v2/telegrafs/0000000000000001",
							"labels": "/api/v2/telegrafs/0000000000000001/labels",
							"members": "/api/v2/telegrafs/0000000000000001/members",
							"owners": "/api/v2/telegrafs/0000000000000001/owners"
						}
					}
				]
			}`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			res := newTelegrafResponses(ctx, tt.args.tcs, mock.NewLabelService())
			got, err := json.Marshal(res)
			if err != nil {
				t.Fatalf("newTelegrafResponses() JSON marshal %v", err)
			}
			if eq, diff, _ := jsonEqual(string(got), tt.want); tt.want != "" && !eq {
				t.Errorf("%q. newTelegrafResponses() = ***%s***", tt.name, diff)
			}
		})
	}
}

func Test_newTelegrafResponse(t *testing.T) {
	type args struct {
		tc *platform.TelegrafConfig
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			args: args{
				tc: &platform.TelegrafConfig{
					ID:          platform2.ID(1),
					OrgID:       platform2.ID(2),
					Name:        "my config",
					Description: "my description",
					Config:      "[[inputs.cpu]]\n[[outputs.influxdb_v2]]\n",
				},
			},
			want: `{
				"id": "0000000000000001",
				"orgID": "0000000000000002",
				"name": "my config",
				"description": "my description",
				"config": "[[inputs.cpu]]\n[[outputs.influxdb_v2]]\n",
				"labels": [],
				"links": {
					"self": "/api/v2/telegrafs/0000000000000001",
					"labels": "/api/v2/telegrafs/0000000000000001/labels",
					"members": "/api/v2/telegrafs/0000000000000001/members",
					"owners": "/api/v2/telegrafs/0000000000000001/owners"
				}
			}`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			res := newTelegrafResponse(tt.args.tc, []*platform.Label{})
			got, err := json.Marshal(res)
			if err != nil {
				t.Fatalf("newTelegrafResponse() JSON marshal %v", err)
			}
			if eq, diff, _ := jsonEqual(string(got), tt.want); tt.want != "" && !eq {
				fmt.Println(string(got))
				t.Errorf("%q. newTelegrafResponse() = ***%s***", tt.name, diff)
			}
		})
	}
}
