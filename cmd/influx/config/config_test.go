package config

import (
	"bytes"
	"testing"

	"github.com/influxdata/influxdb/v2/kit/platform/errors"

	"github.com/google/go-cmp/cmp"
	influxtesting "github.com/influxdata/influxdb/v2/testing"
)

func TestWriteConfigs(t *testing.T) {
	cases := []struct {
		name   string
		err    *errors.Error
		pp     Configs
		result string
	}{
		{
			name: "bad name -",
			err: &errors.Error{
				Code: errors.EInvalid,
				Msg:  `"-" is not a valid config name`,
			},
		},
		{
			name: "bad name create",
			err: &errors.Error{
				Code: errors.EInvalid,
				Msg:  `"create" is not a valid config name`,
			},
		},
		{
			name: "new config",
			pp: Configs{
				"default": Config{
					Token:  "token1",
					Org:    "org1",
					Host:   "http://localhost:8086",
					Active: true,
				},
			},
			result: `[default]
  url = "http://localhost:8086"
  token = "token1"
  org = "org1"
  active = true` + commentedStr,
		},
		{
			name: "multiple",
			pp: Configs{
				"config1": Config{
					Token: "token1",
					Host:  "host1",
				},
				"config2": Config{
					Token:  "token2",
					Host:   "host2",
					Org:    "org2",
					Active: true,
				},
				"config3": Config{
					Token: "token3",
					Host:  "host3",
					Org:   "org3",
				},
			},
			result: `[config1]
  url = "host1"
  token = "token1"
  org = ""

[config2]
  url = "host2"
  token = "token2"
  org = "org2"
  active = true

[config3]
  url = "host3"
  token = "token3"
  org = "org3"` + commentedStr,
		},
	}
	for _, c := range cases {
		var b1 bytes.Buffer
		err := (baseRW{w: &b1}).writeConfigs(c.pp)
		influxtesting.ErrorsEqual(t, err, err)
		if c.err == nil {
			if diff := cmp.Diff(c.result, b1.String()); diff != "" {
				t.Fatalf("write configs %s err, diff %s", c.name, diff)
			}
		}
	}
}

var commentedStr = `
# 
# [eu-central]
#   url = "https://eu-central-1-1.aws.cloud2.influxdata.com"
#   token = "XXX"
#   org = ""
# 
# [us-central]
#   url = "https://us-central1-1.gcp.cloud2.influxdata.com"
#   token = "XXX"
#   org = ""
# 
# [us-west]
#   url = "https://us-west-2-1.aws.cloud2.influxdata.com"
#   token = "XXX"
#   org = ""
`

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
				Name:   "a2",
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

func TestParsePreviousActiveConfig(t *testing.T) {
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
			previous =true
			[a2]
			url = "host2"
			previous = true			
			`,
		},
		{
			name:   "one previous active",
			hasErr: false,
			src: `
			[a1]
			url = "host1"
			[a2]
			url = "host2"
			previous = true
			[a3]
			url = "host3"
			[a4]
			url = "host4"				
			`,
			p: Config{
				Name:           "a2",
				Host:           "host2",
				PreviousActive: true,
			},
		},
	}
	for _, c := range cases {
		r := bytes.NewBufferString(c.src)
		p, err := (baseRW{r: r}).parsePreviousActive()
		if c.hasErr {
			if err == nil {
				t.Fatalf("parse previous active config %q failed, should have error, got nil", c.name)
			}
			continue
		}
		if diff := cmp.Diff(p, c.p); diff != "" {
			t.Fatalf("parse previous active config %s failed, diff %s", c.name, diff)
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
			err: &errors.Error{
				Code: errors.ENotFound,
				Msg:  `config "p1" is not found`,
			},
		},
		{
			name:   "regular switch",
			target: "a1",
			old: Configs{
				"a1": {Host: "host1"},
				"a2": {Host: "host2"},
				"a3": {Host: "host3", Active: true},
			},
			new: Configs{
				"a1": {Host: "host1", Active: true},
				"a2": {Host: "host2"},
				"a3": {Host: "host3", PreviousActive: true},
			},
			err: nil,
		},
		{
			name:   "regular to current active", // nothing should be changed
			target: "a3",
			old: Configs{
				"a1": {Host: "host1"},
				"a2": {Host: "host2"},
				"a3": {Host: "host3", Active: true},
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

func TestConfigCreate(t *testing.T) {
	cases := []struct {
		name   string
		exists Configs
		src    Config
		err    error
		result Config
		stored Configs
	}{
		{
			name: "invalid name",
			err: &errors.Error{
				Code: errors.EInvalid,
				Msg:  "config name is empty",
			},
		},
		{
			name: "new",
			src: Config{
				Name:  "default",
				Host:  "host1",
				Org:   "org1",
				Token: "tok1",
			},
			result: Config{
				Name:  "default",
				Host:  "host1",
				Org:   "org1",
				Token: "tok1",
			},
			stored: Configs{
				"default": {
					Name:  "default",
					Host:  "host1",
					Org:   "org1",
					Token: "tok1",
				},
			},
		},
		{
			name: "new active",
			src: Config{
				Name:   "default",
				Host:   "host1",
				Org:    "org1",
				Token:  "tok1",
				Active: true,
			},
			result: Config{
				Name:   "default",
				Host:   "host1",
				Org:    "org1",
				Token:  "tok1",
				Active: true,
			},
			stored: Configs{
				"default": {
					Name:   "default",
					Host:   "host1",
					Org:    "org1",
					Token:  "tok1",
					Active: true,
				},
			},
		},
		{
			name: "conflict",
			exists: Configs{
				"default": {
					Name: "default",
					Host: "host1",
				},
			},
			src: Config{
				Name: "default",
				Host: "host1",
			},
			err: &errors.Error{
				Code: errors.EConflict,
				Msg:  `config "default" already exists`,
			},
		},
		{
			name: "existing",
			exists: Configs{
				"default": {
					Name:   "default",
					Host:   "host1",
					Org:    "org1",
					Token:  "tok1",
					Active: true,
				},
			},
			src: Config{
				Name:   "a1",
				Host:   "host1",
				Org:    "org1",
				Token:  "tok1",
				Active: true,
			},
			result: Config{
				Name:   "a1",
				Host:   "host1",
				Org:    "org1",
				Token:  "tok1",
				Active: true,
			},
			stored: Configs{
				"default": {
					Name:           "default",
					Host:           "host1",
					Org:            "org1",
					Token:          "tok1",
					PreviousActive: true,
				},
				"a1": {
					Name:   "a1",
					Host:   "host1",
					Org:    "org1",
					Token:  "tok1",
					Active: true,
				},
			},
		},
	}
	for _, c := range cases {
		svc, store := newBufferSVC()
		_ = store.writeConfigs(c.exists)
		result, err := svc.CreateConfig(c.src)
		influxtesting.ErrorsEqual(t, err, c.err)
		if err == nil {
			if diff := cmp.Diff(result, c.result); diff != "" {
				t.Fatalf("create config %s failed, diff %s", c.name, diff)
			}
			stored, err := store.ListConfigs()
			if err != nil {
				t.Fatalf("create config %s to list result, err %s", c.name, err.Error())
			}
			if diff := cmp.Diff(stored, c.stored); diff != "" {
				t.Fatalf("create config %s failed, diff %s", c.name, diff)
			}
		}
	}
}

func TestConfigSwitch(t *testing.T) {
	cases := []struct {
		name   string
		exists Configs
		src    string
		err    error
		result Config
		stored Configs
	}{
		{
			name: "empty",
			err: &errors.Error{
				Code: errors.ENotFound,
				Msg:  `config "" is not found`,
			},
		},
		{
			name: "not found",
			src:  "default",
			err: &errors.Error{
				Code: errors.ENotFound,
				Msg:  `config "default" is not found`,
			},
		},
		{
			name: "regular",
			exists: Configs{
				"a1": {
					Name:  "a1",
					Host:  "host1",
					Org:   "org1",
					Token: "tok1",
				},
				"a2": {
					Name:   "a2",
					Host:   "host2",
					Org:    "org2",
					Token:  "tok2",
					Active: true,
				},
			},
			src: "a1",
			result: Config{
				Name:   "a1",
				Host:   "host1",
				Org:    "org1",
				Token:  "tok1",
				Active: true,
			},
			stored: Configs{
				"a1": {
					Name:   "a1",
					Host:   "host1",
					Org:    "org1",
					Token:  "tok1",
					Active: true,
				},
				"a2": {
					Name:           "a2",
					Host:           "host2",
					Org:            "org2",
					Token:          "tok2",
					PreviousActive: true,
				},
			},
		},
		{
			name: "switch back",
			exists: Configs{
				"a1": {
					Name:           "a1",
					Host:           "host1",
					Org:            "org1",
					Token:          "tok1",
					PreviousActive: true,
				},
				"a2": {
					Name:   "a2",
					Host:   "host2",
					Org:    "org2",
					Token:  "tok2",
					Active: true,
				},
			},
			src: "-",
			result: Config{
				Name:   "a1",
				Host:   "host1",
				Org:    "org1",
				Token:  "tok1",
				Active: true,
			},
			stored: Configs{
				"a1": {
					Name:   "a1",
					Host:   "host1",
					Org:    "org1",
					Token:  "tok1",
					Active: true,
				},
				"a2": {
					Name:           "a2",
					Host:           "host2",
					Org:            "org2",
					Token:          "tok2",
					PreviousActive: true,
				},
			},
		},
		{
			name: "switch back with no previous",
			exists: Configs{
				"a1": {
					Name:  "a1",
					Host:  "host1",
					Org:   "org1",
					Token: "tok1",
				},
				"a2": {
					Name:   "a2",
					Host:   "host2",
					Org:    "org2",
					Token:  "tok2",
					Active: true,
				},
			},
			src: "-",
			err: &errors.Error{
				Code: errors.ENotFound,
				Msg:  "previous activated config is not found",
			},
		},
	}
	for _, c := range cases {
		svc, store := newBufferSVC()
		_ = store.writeConfigs(c.exists)
		result, err := svc.SwitchActive(c.src)
		influxtesting.ErrorsEqual(t, err, c.err)
		if err == nil {
			if diff := cmp.Diff(result, c.result); diff != "" {
				t.Fatalf("switch config %s failed, diff %s", c.name, diff)
			}
			stored, err := store.ListConfigs()
			if err != nil {
				t.Fatalf("switch config %s to list result, err %s", c.name, err.Error())
			}
			if diff := cmp.Diff(stored, c.stored); diff != "" {
				t.Fatalf("switch config %s failed, diff %s", c.name, diff)
			}
		}
	}
}

func TestConfigUpdate(t *testing.T) {
	cases := []struct {
		name   string
		exists Configs
		src    Config
		err    error
		result Config
		stored Configs
	}{
		{
			name: "empty",
			err: &errors.Error{
				Code: errors.ENotFound,
				Msg:  `config "" is not found`,
			},
		},
		{
			name: "not found",
			src: Config{
				Name:  "default",
				Host:  "host1",
				Org:   "org1",
				Token: "tok1",
			},
			err: &errors.Error{
				Code: errors.ENotFound,
				Msg:  `config "default" is not found`,
			},
		},
		{
			name: "regular",
			exists: Configs{
				"a1": {
					Name:  "a1",
					Host:  "host1",
					Org:   "org1",
					Token: "tok1",
				},
				"a2": {
					Name:   "a2",
					Host:   "host2",
					Org:    "org2",
					Token:  "tok2",
					Active: true,
				},
			},
			src: Config{
				Name:   "a1",
				Host:   "host11",
				Org:    "org11",
				Token:  "tok11",
				Active: true,
			},
			result: Config{
				Name:   "a1",
				Host:   "host11",
				Org:    "org11",
				Token:  "tok11",
				Active: true,
			},
			stored: Configs{
				"a1": {
					Name:   "a1",
					Host:   "host11",
					Org:    "org11",
					Token:  "tok11",
					Active: true,
				},
				"a2": {
					Name:           "a2",
					Host:           "host2",
					Org:            "org2",
					Token:          "tok2",
					PreviousActive: true,
				},
			},
		},
	}
	for _, c := range cases {
		svc, store := newBufferSVC()
		_ = store.writeConfigs(c.exists)
		result, err := svc.UpdateConfig(c.src)
		influxtesting.ErrorsEqual(t, err, c.err)
		if err == nil {
			if diff := cmp.Diff(result, c.result); diff != "" {
				t.Fatalf("update config %s failed, diff %s", c.name, diff)
			}
			stored, err := store.ListConfigs()
			if err != nil {
				t.Fatalf("update config %s to list result, err %s", c.name, err.Error())
			}
			if diff := cmp.Diff(stored, c.stored); diff != "" {
				t.Fatalf("update config %s failed, diff %s", c.name, diff)
			}
		}
	}
}

func TestConfigDelete(t *testing.T) {
	cases := []struct {
		name   string
		exists Configs
		target string
		err    error
		result Config
		stored Configs
	}{
		{
			name: "empty",
			err: &errors.Error{
				Code: errors.ENotFound,
				Msg:  `config "" is not found`,
			},
		},
		{
			name:   "not found",
			target: "bad",
			exists: Configs{
				"default": {
					Name: "default",
					Host: "host1",
				},
			},
			err: &errors.Error{
				Code: errors.ENotFound,
				Msg:  `config "bad" is not found`,
			},
		},
		{
			name: "regular",
			exists: Configs{
				"default": {
					Name: "default",
					Host: "host1",
				},
			},
			target: "default",
			result: Config{
				Name: "default",
				Host: "host1",
			},
			stored: Configs{},
		},
		{
			name: "more than 1",
			exists: Configs{
				"a1": {
					Host:   "host1",
					Org:    "org1",
					Token:  "tok1",
					Active: true,
				},
				"a2": {
					Host:  "host2",
					Org:   "org2",
					Token: "tok2",
				},
			},
			target: "a1",
			result: Config{
				Name:   "a1",
				Host:   "host1",
				Org:    "org1",
				Token:  "tok1",
				Active: true,
			},
			stored: Configs{
				"a2": {
					Active: true,
					Name:   "a2",
					Host:   "host2",
					Org:    "org2",
					Token:  "tok2",
				},
			},
		},
	}
	for _, c := range cases {
		fn := func(t *testing.T) {
			svc, store := newBufferSVC()
			_ = store.writeConfigs(c.exists)
			result, err := svc.DeleteConfig(c.target)
			influxtesting.ErrorsEqual(t, err, c.err)
			if err == nil {
				if diff := cmp.Diff(result, c.result); diff != "" {
					t.Fatalf("delete config %s failed, diff %s", c.name, diff)
				}
				stored, err := store.ListConfigs()
				if err != nil {
					t.Fatalf("delete config %s to list result, err %s", c.name, err.Error())
				}
				if diff := cmp.Diff(stored, c.stored); diff != "" {
					t.Fatalf("delete config %s failed, diff %s", c.name, diff)
				}
			}
		}
		t.Run(c.name, fn)
	}
}

func newBufferSVC() (Service, *bytesStore) {
	store := new(bytesStore)
	return newConfigsSVC(store), store
}

type bytesStore struct {
	data []byte
}

func (s *bytesStore) writeConfigs(cfgs Configs) error {
	var b bytes.Buffer
	if err := (baseRW{w: &b}).writeConfigs(cfgs); err != nil {
		return err
	}
	s.data = b.Bytes()
	return nil
}

func (s *bytesStore) ListConfigs() (Configs, error) {
	return baseRW{
		r: bytes.NewBuffer(s.data),
	}.ListConfigs()
}

func (s *bytesStore) parsePreviousActive() (Config, error) {
	return (baseRW{
		r: bytes.NewBuffer(s.data),
	}).parsePreviousActive()
}
