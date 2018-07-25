package platform_test

import (
	"testing"

	"github.com/influxdata/platform"
)

func TestValidName(t *testing.T) {
	tests := []struct {
		arg  platform.Name
		name string
		want bool
	}{
		{
			name: "names cannot have unprintable characters",
			arg:  platform.Name([]byte{0x0D}),
			want: false,
		},
		{
			name: "names cannot have .",
			arg:  ".",
			want: false,
		},
		{
			name: "names cannot have ..",
			arg:  "..",
			want: false,
		},
		{
			name: "names cannot have /",
			arg:  "/",
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.arg.Valid(); got != tt.want {
				t.Errorf("validName() = %v, want %v", got, tt.want)
			}
		})
	}
}
