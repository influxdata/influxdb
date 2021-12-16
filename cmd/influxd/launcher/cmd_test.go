package launcher

import (
	"strings"
	"testing"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
)

func TestInvalidFlags(t *testing.T) {
	t.Parallel()

	v2config := `
bolt-path = "/db/.influxdbv2/influxd.bolt"
engine-path = "/db/.influxdbv2/engine"
http-bind-address = ":8086"
`

	v1config := `
reporting-disabled = false

# Bind address to use for the RPC service for backup and restore.
bind-address = "127.0.0.1:8088"

[http]
  flux-enabled = false

[data]
  index-version = "inmem"`

	tests := []struct {
		name   string
		config string
		want   []string
	}{
		{
			name:   "empty config",
			config: "",
			want:   []string(nil),
		},
		{
			name:   "v2 config",
			config: v2config,
			want:   []string(nil),
		},
		{
			name:   "v1 config",
			config: v1config,
			want:   []string{"http.flux-enabled", "data.index-version", "bind-address"},
		},
		{
			name:   "mixed config",
			config: v2config + v1config,
			want:   []string{"http.flux-enabled", "data.index-version", "bind-address"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := strings.NewReader(tt.config)
			v := viper.GetViper()
			v.SetConfigType("toml")
			require.NoError(t, v.ReadConfig(r))
			got := invalidFlags(v)
			require.ElementsMatch(t, tt.want, got)
		})
	}
}
