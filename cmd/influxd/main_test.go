package main

import "testing"

func SliceEquals(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}

	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestParseCommandName(t *testing.T) {
	type expected struct {
		name string
		args []string
	}

	tests := []struct {
		args []string
		exp  expected
	}{
		{
		// no arguments should result in no name and no arguments
		},
		{
			args: []string{"-c", "/etc/influxdb/influxdb.conf"},
			exp: expected{
				args: []string{"-c", "/etc/influxdb/influxdb.conf"},
			},
		},
		{
			args: []string{"run", "-c", "/etc/influxdb/influxdb.conf"},
			exp: expected{
				name: "run",
				args: []string{"-c", "/etc/influxdb/influxdb.conf"},
			},
		},
		{
			args: []string{"help"},
			exp: expected{
				name: "help",
			},
		},
		{
			args: []string{"help", "run"},
			exp: expected{
				name: "run",
				args: []string{"-h"},
			},
		},
		{
			args: []string{"--help", "config"},
			exp: expected{
				name: "config",
				args: []string{"-h"},
			},
		},
		{
			args: []string{"-h", "backup"},
			exp: expected{
				name: "backup",
				args: []string{"-h"},
			},
		},
	}

	for i, tt := range tests {
		name, args := ParseCommandName(tt.args)
		if name != tt.exp.name {
			t.Errorf("%d. unexpected name. got=%s exp=%s", i, name, tt.exp.name)
		}
		if !SliceEquals(args, tt.exp.args) {
			t.Errorf("%d. unexpected args. got=%s exp=%s", i, args, tt.exp.args)
		}
	}
}
