package filestore

import (
	"os"
	"testing"
)

func Test_environ(t *testing.T) {
	tests := []struct {
		name  string
		key   string
		value string
	}{
		{
			name:  "environment variable is returned",
			key:   "CHRONOGRAF_TEST_ENVIRON",
			value: "howdy",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			os.Setenv(tt.key, tt.value)
			got := environ()
			if v, ok := got[tt.key]; !ok || v != tt.value {
				t.Errorf("environ() = %v, want %v", v, tt.value)
			}
		})
	}
}
