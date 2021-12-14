package launcher

import (
	"strings"
	"testing"

	"github.com/influxdata/influxdb/v2/kit/cli"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
)

func TestInvalidFlags(t *testing.T) {
	t.Parallel()

	testOpts := []cli.Opt{
		{
			Flag: "validflag1",
		},
		{
			Flag: "validflag2",
		},
	}

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
			name:   "one invalid flag",
			config: "invalidflag: invalidValue",
			want:   []string{"invalidflag"},
		},
		{
			name:   "one valid flag",
			config: "validflag1: value",
			want:   []string(nil),
		},
		{
			name:   "multiple valid flags",
			config: "validflag1: value\nvalidflag2: value",
			want:   []string(nil),
		},
		{
			name:   "multiple invalid flags",
			config: "invalid1: value\ninvalid2: value",
			want:   []string{"invalid1", "invalid2"},
		},
		{
			name:   "mix of valid/invalid flags",
			config: "invalid1: value\ninvalid2: value\nvalidflag1: value\nvalidflag2: value",
			want:   []string{"invalid1", "invalid2"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := strings.NewReader(tt.config)
			v := viper.GetViper()
			v.SetConfigType("yaml")
			require.NoError(t, v.ReadConfig(r))
			got := invalidFlags(v, testOpts)
			require.ElementsMatch(t, tt.want, got)
		})
	}
}
