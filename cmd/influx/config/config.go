package config

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"os"

	"github.com/BurntSushi/toml"
	"github.com/influxdata/influxdb"
)

// Config store the crendentials of influxdb host and token.
type Config struct {
	Host string `toml:"url" json:"url"`
	// Token is base64 encoded sequence.
	Token  string `toml:"token" json:"token"`
	Org    string `toml:"org" json:"org"`
	Active bool   `toml:"active" json:"active"`
}

// DefaultConfig is default config without token
var DefaultConfig = Config{
	Host:   "http://localhost:9999",
	Active: true,
}

// Configs is map of configs indexed by name.
type Configs map[string]Config

// ConfigsService is the service to list and write configs.
type ConfigsService interface {
	WriteConfigs(pp Configs) error
	ParseConfigs() (Configs, error)
}

// Switch to another config.
func (pp *Configs) Switch(name string) error {
	pc := *pp
	if _, ok := pc[name]; !ok {
		return &influxdb.Error{
			Code: influxdb.ENotFound,
			Msg:  fmt.Sprintf(`config %q is not found`, name),
		}
	}
	for k, v := range pc {
		v.Active = k == name
		pc[k] = v
	}
	return nil
}

// LocalConfigsSVC has the path and dir to write and parse configs.
type LocalConfigsSVC struct {
	Path string
	Dir  string
}

// ParseConfigs from the local path.
func (svc LocalConfigsSVC) ParseConfigs() (Configs, error) {
	r, err := os.Open(svc.Path)
	if err != nil {
		return make(Configs), nil
	}
	return ParseConfigs(r)
}

// WriteConfigs to the path.
func (svc LocalConfigsSVC) WriteConfigs(pp Configs) error {
	if err := os.MkdirAll(svc.Dir, os.ModePerm); err != nil {
		return err
	}
	var b1, b2 bytes.Buffer
	err := toml.NewEncoder(&b1).Encode(pp)
	if err != nil {
		return err
	}
	// a list cloud 2 clusters, commented out
	b1.WriteString("# \n")
	pp = map[string]Config{
		"us-central": {Host: "https://us-central1-1.gcp.cloud2.influxdata.com", Token: "XXX"},
		"us-west":    {Host: "https://us-west-2-1.aws.cloud2.influxdata.com", Token: "XXX"},
		"eu-central": {Host: "https://eu-central-1-1.aws.cloud2.influxdata.com", Token: "XXX"},
	}

	if err := toml.NewEncoder(&b2).Encode(pp); err != nil {
		return err
	}
	reader := bufio.NewReader(&b2)
	for {
		line, _, err := reader.ReadLine()

		if err == io.EOF {
			break
		}
		b1.WriteString("# " + string(line) + "\n")
	}
	return ioutil.WriteFile(svc.Path, b1.Bytes(), 0600)
}

// ParseConfigs decodes configs from io readers
func ParseConfigs(r io.Reader) (Configs, error) {
	p := make(Configs)
	_, err := toml.DecodeReader(r, &p)
	return p, err
}

// ParseActiveConfig returns the active config from the reader.
func ParseActiveConfig(r io.Reader) (Config, error) {
	pp, err := ParseConfigs(r)
	if err != nil {
		return DefaultConfig, err
	}
	var activated Config
	var hasActive bool
	for _, p := range pp {
		if p.Active && !hasActive {
			activated = p
			hasActive = true
		} else if p.Active {
			return DefaultConfig, &influxdb.Error{
				Code: influxdb.EConflict,
				Msg:  "more than one activated configs found",
			}
		}
	}
	if hasActive {
		return activated, nil
	}
	return DefaultConfig, &influxdb.Error{
		Code: influxdb.ENotFound,
		Msg:  "activated config is not found",
	}
}
