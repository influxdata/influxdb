package platform

import (
	"encoding/json"
	"fmt"
	"reflect"
	"testing"

	"github.com/influxdata/platform/telegraf/plugins"
	"github.com/influxdata/platform/telegraf/plugins/outputs"

	"github.com/google/go-cmp/cmp"
	"github.com/influxdata/platform/telegraf/plugins/inputs"
)

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

func TestTelegrafConfigJSON(t *testing.T) {
	id1, _ := IDFromString("020f755c3c082000")
	id2, _ := IDFromString("020f755c3c082002")
	cases := []struct {
		name string
		cfg  *TelegrafConfig
		err  error
		json string
	}{
		{
			name: "regular config",
			cfg: &TelegrafConfig{
				ID:        *id1,
				Name:      "n1",
				LastModBy: *id2,
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
				},
			},
		},
		{
			name: "unsupported plugin type",
			cfg: &TelegrafConfig{
				ID:        *id1,
				Name:      "n1",
				LastModBy: *id2,
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
				ID:        *id1,
				Name:      "n1",
				LastModBy: *id2,
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
			t.Fatalf("%s encode failed, want cfg: %v, should be nil", c.name, err)
		}
		got := new(TelegrafConfig)
		err = json.Unmarshal([]byte(result), got)
		if diff := cmp.Diff(err, c.err); diff != "" {
			t.Fatalf("%s decode failed, got err: %v, should be %v", c.name, err, c.err)
		}

		// use DeepEqual to ignore unexported field panic
		if c.err == nil && !reflect.DeepEqual(got, c.cfg) {
			t.Fatalf("%s decode failed, want: %v, got: %v", c.name, c.cfg, got)
		}
	}
}
