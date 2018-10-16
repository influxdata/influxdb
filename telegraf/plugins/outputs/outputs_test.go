package outputs

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
	b := baseOutput(0)
	if b.Type() != plugins.Output {
		t.Fatalf("output plugins type should be output, got %s", b.Type())
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
				&File{}: `[[outputs.file]]
  ## Files to write to, "stdout" is a specially handled file.
  files = []
`,
				&InfluxDBV2{}: `[[outputs.influxdb_v2]]	
  ## The URLs of the InfluxDB cluster nodes.
  ##
  ## Multiple URLs can be specified for a single cluster, only ONE of the
  ## urls will be written to each interval.
  ## urls exp: http://127.0.0.1:9999
  urls = []

  ## Token for authentication.
  token = ""

  ## Organization is the name of the organization you wish to write to; must exist.
  organization = ""

  ## Destination bucket to write into.
  bucket = ""
`,
			},
		},
		{
			name: "standard testing",
			plugins: map[telegrafPluginConfig]string{
				&File{
					Files: []FileConfig{
						{Typ: "stdout"},
						{Path: "/tmp/out.txt"},
					},
				}: `[[outputs.file]]
  ## Files to write to, "stdout" is a specially handled file.
  files = ["stdout", "/tmp/out.txt"]
`,
				&InfluxDBV2{
					URLs: []string{
						"http://192.168.1.10:9999",
						"http://192.168.1.11:9999",
					},
					Token:        "tok1",
					Organization: "org1",
					Bucket:       "bucket1",
				}: `[[outputs.influxdb_v2]]	
  ## The URLs of the InfluxDB cluster nodes.
  ##
  ## Multiple URLs can be specified for a single cluster, only ONE of the
  ## urls will be written to each interval.
  ## urls exp: http://127.0.0.1:9999
  urls = ["http://192.168.1.10:9999", "http://192.168.1.11:9999"]

  ## Token for authentication.
  token = "tok1"

  ## Organization is the name of the organization you wish to write to; must exist.
  organization = "org1"

  ## Destination bucket to write into.
  bucket = "bucket1"
`,
			},
		},
	}
	for _, c := range cases {
		for output, toml := range c.plugins {
			if toml != output.TOML() {
				t.Fatalf("%s failed want %s, got %v", c.name, toml, output.TOML())
			}
		}
	}
}
