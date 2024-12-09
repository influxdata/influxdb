package config

import (
	"reflect"
	"testing"
)

func TestParse(t *testing.T) {
	tests := []struct {
		name      string
		args      []string
		envVars   map[string]string
		expected  *Config
		expectErr bool
	}{
		{
			name:    "default values",
			args:    []string{},
			envVars: map[string]string{},
			expected: &Config{
				HTTPAddr:  ":8080",
				LogLevel:  "info",
				LogFormat: "auto",
			},
			expectErr: false,
		},
		{
			name:    "override with command-line arguments",
			args:    []string{"--http-addr", "127.0.0.1:9000", "--log-level", "debug", "--log-format", "json"},
			envVars: map[string]string{},
			expected: &Config{
				HTTPAddr:  "127.0.0.1:9000",
				LogLevel:  "debug",
				LogFormat: "json",
			},
			expectErr: false,
		},
		{
			name: "override with environment variables",
			args: []string{},
			envVars: map[string]string{
				"IFLX_PRO_LIC_HTTP_ADDR":  "192.168.1.1:8081",
				"IFLX_PRO_LIC_LOG_LEVEL":  "warn",
				"IFLX_PRO_LIC_LOG_FORMAT": "logfmt",
			},
			expected: &Config{
				HTTPAddr:  "192.168.1.1:8081",
				LogLevel:  "warn",
				LogFormat: "logfmt",
			},
			expectErr: false,
		},
		{
			name:      "invalid command-line arguments",
			args:      []string{"--unknown-flag"},
			envVars:   map[string]string{},
			expected:  nil,
			expectErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Set environment variables
			for key, value := range tt.envVars {
				t.Setenv(key, value)
			}

			// Run Parse function
			cfg, err := Parse(tt.args)

			if tt.expectErr {
				if err == nil {
					t.Errorf("expected an error but got none")
				}
				return
			}

			if err != nil {
				t.Errorf("did not expect an error but got: %v", err)
				return
			}

			// Compare the actual and expected config
			if !reflect.DeepEqual(cfg, tt.expected) {
				t.Errorf("expected %+v but got %+v", tt.expected, cfg)
			}
		})
	}
}
