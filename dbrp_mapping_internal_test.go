package influxdb

import (
	"testing"
)

func Test_validName(t *testing.T) {
	tests := []struct {
		arg  string
		name string
		want bool
	}{
		{
			name: "names cannot have unprintable characters",
			arg:  string([]byte{0x0D}),
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
			if got := validName(tt.arg); got != tt.want {
				t.Errorf("validName() = %v, want %v", got, tt.want)
			}
		})
	}
}
