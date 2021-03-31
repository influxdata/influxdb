package config

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"os"

	"github.com/influxdata/influxdb/v2/kit/platform/errors"

	"github.com/BurntSushi/toml"
)

// Config store the crendentials of influxdb host and token.
type Config struct {
	Name string `toml:"-" json:"-"`
	Host string `toml:"url" json:"url"`
	// Token is base64 encoded sequence.
	Token          string `toml:"token" json:"token"`
	Org            string `toml:"org" json:"org"`
	Active         bool   `toml:"active,omitempty" json:"active,omitempty"`
	PreviousActive bool   `toml:"previous,omitempty" json:"previous,omitempty"`
}

// DefaultConfig is default config without token
var DefaultConfig = Config{
	Name:   "default",
	Host:   "http://localhost:8086",
	Active: true,
}

// Configs is map of configs indexed by name.
type Configs map[string]Config

// Service is the service to list and write configs.
type Service interface {
	CreateConfig(Config) (Config, error)
	DeleteConfig(name string) (Config, error)
	UpdateConfig(Config) (Config, error)
	SwitchActive(name string) (Config, error)
	ListConfigs() (Configs, error)
}

// store is the embedded store of the Config service.
type store interface {
	parsePreviousActive() (Config, error)
	ListConfigs() (Configs, error)
	writeConfigs(cfgs Configs) error
}

// Switch to another config.
func (cfgs Configs) Switch(name string) error {
	if _, ok := cfgs[name]; !ok {
		return &errors.Error{
			Code: errors.ENotFound,
			Msg:  fmt.Sprintf(`config %q is not found`, name),
		}
	}
	for k, v := range cfgs {
		v.PreviousActive = v.Active && (k != name)
		v.Active = k == name
		cfgs[k] = v
	}
	return nil
}

func (cfgs Configs) Active() Config {
	for _, cfg := range cfgs {
		if cfg.Active {
			return cfg
		}
	}
	if len(cfgs) > 0 {
		for _, cfg := range cfgs {
			return cfg
		}
	}
	return DefaultConfig
}

// localConfigsSVC has the path and dir to write and parse configs.
type localConfigsSVC struct {
	store
}

type ioStore struct {
	Path string
	Dir  string
}

// newConfigsSVC create a new localConfigsSVC.
func newConfigsSVC(s store) localConfigsSVC {
	return localConfigsSVC{
		store: s,
	}
}

// NewLocalConfigSVC create a new local config svc.
func NewLocalConfigSVC(path, dir string) Service {
	return newConfigsSVC(ioStore{
		Path: path,
		Dir:  dir,
	})
}

// ListConfigs from the local path.
func (s ioStore) ListConfigs() (Configs, error) {
	r, err := os.Open(s.Path)
	if err != nil {
		return make(Configs), nil
	}
	return (baseRW{r: r}).ListConfigs()
}

// parsePreviousActive from the local path.
func (s ioStore) parsePreviousActive() (Config, error) {
	r, err := os.Open(s.Path)
	if err != nil {
		return Config{}, nil
	}
	return (baseRW{r: r}).parsePreviousActive()
}

var badNames = map[string]bool{
	"-":      false,
	"list":   false,
	"update": false,
	"set":    false,
	"delete": false,
	"switch": false,
	"create": false,
}

func blockBadName(cfgs Configs) error {
	for n := range cfgs {
		if _, ok := badNames[n]; ok {
			return &errors.Error{
				Code: errors.EInvalid,
				Msg:  fmt.Sprintf(`%q is not a valid config name`, n),
			}
		}
	}
	return nil
}

type baseRW struct {
	r io.Reader
	w io.Writer
}

func (s baseRW) writeConfigs(cfgs Configs) error {
	if err := blockBadName(cfgs); err != nil {
		return err
	}
	var b2 bytes.Buffer
	if err := toml.NewEncoder(s.w).Encode(cfgs); err != nil {
		return err
	}
	// a list cloud 2 clusters, commented out
	s.w.Write([]byte("# \n"))
	cfgs = map[string]Config{
		"us-central": {Host: "https://us-central1-1.gcp.cloud2.influxdata.com", Token: "XXX"},
		"us-west":    {Host: "https://us-west-2-1.aws.cloud2.influxdata.com", Token: "XXX"},
		"eu-central": {Host: "https://eu-central-1-1.aws.cloud2.influxdata.com", Token: "XXX"},
	}

	if err := toml.NewEncoder(&b2).Encode(cfgs); err != nil {
		return err
	}
	reader := bufio.NewReader(&b2)
	for {
		line, _, err := reader.ReadLine()

		if err == io.EOF {
			break
		}
		s.w.Write([]byte("# " + string(line) + "\n"))
	}
	return nil
}

// ListConfigs decodes configs from io readers
func (s baseRW) ListConfigs() (Configs, error) {
	cfgs := make(Configs)
	_, err := toml.DecodeReader(s.r, &cfgs)
	for n, cfg := range cfgs {
		cfg.Name = n
		cfgs[n] = cfg
	}
	return cfgs, err
}

// CreateConfig create new config.
func (svc localConfigsSVC) CreateConfig(cfg Config) (Config, error) {
	if cfg.Name == "" {
		return Config{}, &errors.Error{
			Code: errors.EInvalid,
			Msg:  "config name is empty",
		}
	}
	cfgs, err := svc.ListConfigs()
	if err != nil {
		return Config{}, err
	}
	if _, ok := cfgs[cfg.Name]; ok {
		return Config{}, &errors.Error{
			Code: errors.EConflict,
			Msg:  fmt.Sprintf("config %q already exists", cfg.Name),
		}
	}
	cfgs[cfg.Name] = cfg
	if cfg.Active {
		if err := cfgs.Switch(cfg.Name); err != nil {
			return Config{}, err
		}
	}

	return cfgs[cfg.Name], svc.writeConfigs(cfgs)
}

// DeleteConfig will delete a config.
func (svc localConfigsSVC) DeleteConfig(name string) (Config, error) {
	cfgs, err := svc.ListConfigs()
	if err != nil {
		return Config{}, err
	}

	p, ok := cfgs[name]
	if !ok {
		return Config{}, &errors.Error{
			Code: errors.ENotFound,
			Msg:  fmt.Sprintf("config %q is not found", name),
		}
	}
	delete(cfgs, name)

	if p.Active && len(cfgs) > 0 {
		for name, cfg := range cfgs {
			cfg.Active = true
			cfgs[name] = cfg
			break
		}
	}

	return p, svc.writeConfigs(cfgs)
}

// SwitchActive will active the config by name, if name is "-", active the previous one.
func (svc localConfigsSVC) SwitchActive(name string) (Config, error) {
	var up Config
	if name == "-" {
		p0, err := svc.parsePreviousActive()
		if err != nil {
			return Config{}, err
		}
		up.Name = p0.Name
	} else {
		up.Name = name
	}
	up.Active = true
	return svc.UpdateConfig(up)
}

// UpdateConfig will update the config.
func (svc localConfigsSVC) UpdateConfig(up Config) (Config, error) {
	cfgs, err := svc.ListConfigs()
	if err != nil {
		return Config{}, err
	}
	p0, ok := cfgs[up.Name]
	if !ok {
		return Config{}, &errors.Error{
			Code: errors.ENotFound,
			Msg:  fmt.Sprintf("config %q is not found", up.Name),
		}
	}
	if up.Token != "" {
		p0.Token = up.Token
	}
	if up.Host != "" {
		p0.Host = up.Host
	}
	if up.Org != "" {
		p0.Org = up.Org
	}

	cfgs[up.Name] = p0
	if up.Active {
		if err := cfgs.Switch(up.Name); err != nil {
			return Config{}, err
		}
	}

	return cfgs[up.Name], svc.writeConfigs(cfgs)
}

// writeConfigs to the path.
func (s ioStore) writeConfigs(cfgs Configs) error {
	if err := os.MkdirAll(s.Dir, os.ModePerm); err != nil {
		return err
	}
	var b1 bytes.Buffer
	if err := (baseRW{w: &b1}).writeConfigs(cfgs); err != nil {
		return err
	}
	return ioutil.WriteFile(s.Path, b1.Bytes(), 0600)
}

// parsePreviousActive return the previous active config from the reader
func (s baseRW) parsePreviousActive() (Config, error) {
	return s.parseActiveConfig(false)
}

// ParseActiveConfig returns the active config from the reader.
func ParseActiveConfig(r io.Reader) (Config, error) {
	return (baseRW{r: r}).parseActiveConfig(true)
}

func (s baseRW) parseActiveConfig(currentOrPrevious bool) (Config, error) {
	previousText := ""
	if !currentOrPrevious {
		previousText = "previous "
	}
	cfgs, err := s.ListConfigs()
	if err != nil {
		return DefaultConfig, err
	}
	var activated Config
	var hasActive bool
	for _, cfg := range cfgs {
		check := cfg.Active
		if !currentOrPrevious {
			check = cfg.PreviousActive
		}
		if check && !hasActive {
			activated = cfg
			hasActive = true
		} else if check {
			return DefaultConfig, &errors.Error{
				Code: errors.EConflict,
				Msg:  "more than one " + previousText + "activated configs found",
			}
		}
	}
	if hasActive {
		return activated, nil
	}
	return DefaultConfig, &errors.Error{
		Code: errors.ENotFound,
		Msg:  previousText + "activated config is not found",
	}
}
