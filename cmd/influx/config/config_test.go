package config

import (
	"bytes"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/influxdata/influxdb"
	influxtesting "github.com/influxdata/influxdb/testing"
)

func TestParseActiveConfig(t *testing.T) {
	cases := []struct {
		name   string
		hasErr bool
		src    string
		p      Config
	}{
		{
			name:   "bad src",
			src:    "bad [toml",
			hasErr: true,
		},
		{
			name:   "nothing",
			hasErr: true,
		},
		{
			name:   "conflicted",
			hasErr: true,
			src: `
			[a1]
			url = "host1"
			active =true
			[a2]
			url = "host2"
			active = true			
			`,
		},
		{
			name:   "one active",
			hasErr: false,
			src: `
			[a1]
			url = "host1"
			[a2]
			url = "host2"
			active = true
			[a3]
			url = "host3"
			[a4]
			url = "host4"				
			`,
			p: Config{
				Host:   "host2",
				Active: true,
			},
		},
	}
	for _, c := range cases {
		r := bytes.NewBufferString(c.src)
		p, err := ParseActiveConfig(r)
		if c.hasErr {
			if err == nil {
				t.Fatalf("parse active config %q failed, should have error, got nil", c.name)
			}
			continue
		}
		if diff := cmp.Diff(p, c.p); diff != "" {
			t.Fatalf("parse active config %s failed, diff %s", c.name, diff)
		}
	}
}

func TestConfigsSwith(t *testing.T) {
	cases := []struct {
		name   string
		old    Configs
		new    Configs
		target string
		err    error
	}{
		{
			name:   "not found",
			target: "p1",
			old: Configs{
				"a1": {Host: "host1"},
				"a2": {Host: "host2"},
			},
			new: Configs{
				"a1": {Host: "host1"},
				"a2": {Host: "host2"},
			},
			err: &influxdb.Error{
				Code: influxdb.ENotFound,
				Msg:  `config "p1" is not found`,
			},
		},
		{
			name:   "regular switch",
			target: "a3",
			old: Configs{
				"a1": {Host: "host1", Active: true},
				"a2": {Host: "host2"},
				"a3": {Host: "host3"},
			},
			new: Configs{
				"a1": {Host: "host1"},
				"a2": {Host: "host2"},
				"a3": {Host: "host3", Active: true},
			},
			err: nil,
		},
	}
	for _, c := range cases {
		err := c.old.Switch(c.target)
		influxtesting.ErrorsEqual(t, err, c.err)
		if diff := cmp.Diff(c.old, c.new); diff != "" {
			t.Fatalf("switch config %s failed, diff %s", c.name, diff)
		}
	}
}
