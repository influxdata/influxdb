package launcher

import (
	"strings"
	"testing"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
)

func TestInvalidFlags(t *testing.T) {
	t.Parallel()

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
			name:   "invalid flag with a dot",
			config: "meta.something: value",
			want:   []string{"meta.something"},
		},
		{
			name:   "invalid global flag",
			config: "bind-address: value",
			want:   []string{"bind-address"},
		},
		{
			name:   "invalid global and dot",
			config: "bind-address: value\nmeta.something: value",
			want:   []string{"meta.something", "bind-address"},
		},
		{
			name:   "no invalid flags",
			config: "flag1: value\nflag2: value",
			want:   []string(nil),
		},
		{
			name:   "mix of valid/invalid flags",
			config: "bind-address: value\nmeta.something: value\nflag1: value\nflag2: value",
			want:   []string{"meta.something", "bind-address"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := strings.NewReader(tt.config)
			v := viper.GetViper()
			v.SetConfigType("yaml")
			require.NoError(t, v.ReadConfig(r))
			got := invalidFlags(v)
			require.ElementsMatch(t, tt.want, got)
		})
	}
}
