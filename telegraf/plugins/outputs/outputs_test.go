package outputs

import (
	"errors"
	"reflect"
	"testing"

	"github.com/influxdata/influxdb/telegraf/plugins"
)

// local plugin
type telegrafPluginConfig interface {
	TOML() string
	Type() plugins.Type
	PluginName() string
	UnmarshalTOML(data interface{}) error
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

func TestDecodeTOML(t *testing.T) {
	cases := []struct {
		name    string
		want    telegrafPluginConfig
		wantErr error
		output  telegrafPluginConfig
		data    interface{}
	}{
		{
			name:    "file empty",
			want:    &File{},
			wantErr: errors.New("bad files for file output plugin"),
			output:  &File{},
		},
		{
			name:    "file bad data not array",
			want:    &File{},
			wantErr: errors.New("not an array for file output plugin"),
			output:  &File{},
			data: map[string]interface{}{
				"files": "",
			},
		},
		{
			name: "file",
			want: &File{
				Files: []FileConfig{
					{Path: "/tmp/out.txt"},
					{Typ: "stdout"},
				},
			},
			output: &File{},
			data: map[string]interface{}{
				"files": []interface{}{
					"/tmp/out.txt",
					"stdout",
				},
			},
		},
		{
			name:    "influxdb_v2 empty",
			want:    &InfluxDBV2{},
			wantErr: errors.New("bad urls for influxdb_v2 output plugin"),
			output:  &InfluxDBV2{},
		},
		{
			name:    "influxdb_v2 bad urls",
			want:    &InfluxDBV2{},
			wantErr: errors.New("urls is not an array for influxdb_v2 output plugin"),
			output:  &InfluxDBV2{},
			data: map[string]interface{}{
				"urls": "",
			},
		},
		{
			name: "influxdb_v2 missing token",
			want: &InfluxDBV2{
				URLs: []string{
					"http://localhost:9999",
					"http://192.168.0.1:9999",
				},
			},
			wantErr: errors.New("token is missing for influxdb_v2 output plugin"),
			output:  &InfluxDBV2{},
			data: map[string]interface{}{
				"urls": []interface{}{
					"http://localhost:9999",
					"http://192.168.0.1:9999",
				},
			},
		},
		{
			name: "influxdb_v2 missing org",
			want: &InfluxDBV2{
				URLs: []string{
					"http://localhost:9999",
					"http://192.168.0.1:9999",
				},
				Token: "token1",
			},
			wantErr: errors.New("organization is missing for influxdb_v2 output plugin"),
			output:  &InfluxDBV2{},
			data: map[string]interface{}{
				"urls": []interface{}{
					"http://localhost:9999",
					"http://192.168.0.1:9999",
				},
				"token": "token1",
			},
		},
		{
			name: "influxdb_v2 missing bucket",
			want: &InfluxDBV2{
				URLs: []string{
					"http://localhost:9999",
					"http://192.168.0.1:9999",
				},
				Token:        "token1",
				Organization: "org1",
			},
			wantErr: errors.New("bucket is missing for influxdb_v2 output plugin"),
			output:  &InfluxDBV2{},
			data: map[string]interface{}{
				"urls": []interface{}{
					"http://localhost:9999",
					"http://192.168.0.1:9999",
				},
				"token":        "token1",
				"organization": "org1",
			},
		},
		{
			name: "influxdb_v2",
			want: &InfluxDBV2{
				URLs: []string{
					"http://localhost:9999",
					"http://192.168.0.1:9999",
				},
				Token:        "token1",
				Organization: "org1",
				Bucket:       "bucket1",
			},
			output: &InfluxDBV2{},
			data: map[string]interface{}{
				"urls": []interface{}{
					"http://localhost:9999",
					"http://192.168.0.1:9999",
				},
				"token":        "token1",
				"organization": "org1",
				"bucket":       "bucket1",
			},
		},
	}
	for _, c := range cases {
		err := c.output.UnmarshalTOML(c.data)
		if c.wantErr != nil && (err == nil || err.Error() != c.wantErr.Error()) {
			t.Fatalf("%s failed want err %s, got %v", c.name, c.wantErr.Error(), err)
		}
		if c.wantErr == nil && err != nil {
			t.Fatalf("%s failed want err nil, got %v", c.name, err)
		}
		if !reflect.DeepEqual(c.output, c.want) {
			t.Fatalf("%s failed want %v, got %v", c.name, c.want, c.output)
		}
	}
}
