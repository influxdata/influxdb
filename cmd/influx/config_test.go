package main

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/cmd/influx/config"
	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"
)

func TestCmdConfig(t *testing.T) {

	t.Run("create", func(t *testing.T) {
		tests := []struct {
			name     string
			original config.Configs
			expected config.Config
			flags    []string
		}{
			{
				name: "basic",
				flags: []string{
					"--name", "default",
					"--org", "org1",
					"--url", "http://localhost:9999",
					"--token", "tok1",
					"--active",
				},
				original: make(config.Configs),
				expected: config.Config{
					Name:   "default",
					Org:    "org1",
					Active: true,
					Token:  "tok1",
					Host:   "http://localhost:9999",
				},
			},
			{
				name: "short",
				flags: []string{
					"-n", "default",
					"-o", "org1",
					"-u", "http://localhost:9999",
					"-t", "tok1",
					"-a",
				},
				original: make(config.Configs),
				expected: config.Config{
					Name:   "default",
					Org:    "org1",
					Active: true,
					Token:  "tok1",
					Host:   "http://localhost:9999",
				},
			},
			{
				name: "short new with existing",
				flags: []string{
					"-n", "default",
					"-o", "org1",
					"-u", "http://localhost:9999",
					"-t", "tok1",
					"-a",
				},
				original: config.Configs{
					"config1": {
						Org:    "org1",
						Active: true,
						Token:  "tok1",
						Host:   "host1",
					},
				},
				expected: config.Config{
					Name:   "default",
					Org:    "org1",
					Active: true,
					Token:  "tok1",
					Host:   "http://localhost:9999",
				},
			},
		}
		cmdFn := func(original config.Configs, expected config.Config) func(*globalFlags, genericCLIOpts) *cobra.Command {
			svc := &mockConfigService{
				CreateConfigFn: func(cfg config.Config) (config.Config, error) {
					if diff := cmp.Diff(expected, cfg); diff != "" {
						return config.Config{}, &influxdb.Error{
							Msg: fmt.Sprintf("create config failed, diff %s", diff),
						}
					}
					return expected, nil
				},
			}

			return func(g *globalFlags, opt genericCLIOpts) *cobra.Command {
				builder := cmdConfigBuilder{
					genericCLIOpts: opt,
					globalFlags:    g,
					svc:            svc,
				}
				return builder.cmd()
			}
		}
		for _, tt := range tests {
			fn := func(t *testing.T) {
				builder := newInfluxCmdBuilder(
					in(new(bytes.Buffer)),
					out(ioutil.Discard),
				)
				cmd := builder.cmd(cmdFn(tt.original, tt.expected))
				cmd.SetArgs(append([]string{"config", "create"}, tt.flags...))
				require.NoError(t, cmd.Execute())
			}
			t.Run(tt.name, fn)
		}
	})

	t.Run("switch", func(t *testing.T) {
		tests := []struct {
			name     string
			original config.Configs
			expected config.Config
			arg      string
		}{
			{
				name: "basic",
				arg:  "default",
				original: config.Configs{
					"config1": {
						Name:   "config1",
						Org:    "org2",
						Active: true,
						Token:  "tok2",
						Host:   "http://localhost:8888",
					},
					"default": {
						Name:   "default",
						Org:    "org1",
						Active: false,
						Token:  "tok1",
						Host:   "http://localhost:9999",
					},
				},
				expected: config.Config{
					Name:   "default",
					Org:    "org1",
					Active: true,
					Token:  "tok1",
					Host:   "http://localhost:9999",
				},
			},
			{
				name: "back",
				arg:  "-",
				original: config.Configs{
					"config1": {
						Name:   "config1",
						Org:    "org2",
						Active: true,
						Token:  "tok2",
						Host:   "http://localhost:8888",
					},
					"default": {
						Name:           "default",
						Org:            "org1",
						Active:         false,
						PreviousActive: true,
						Token:          "tok1",
						Host:           "http://localhost:9999",
					},
				},
				expected: config.Config{
					Name:   "default",
					Org:    "org1",
					Active: true,
					Token:  "tok1",
					Host:   "http://localhost:9999",
				},
			},
		}
		cmdFn := func(original config.Configs, expected config.Config) func(*globalFlags, genericCLIOpts) *cobra.Command {
			svc := &mockConfigService{
				SwitchActiveFn: func(name string) (config.Config, error) {
					var cfg config.Config
					for _, item := range original {
						if name == "-" && item.PreviousActive ||
							item.Name == name {
							cfg = item
							break

						}
					}
					cfg.Active = true
					cfg.PreviousActive = false
					if diff := cmp.Diff(expected, cfg); diff != "" {
						return config.Config{}, &influxdb.Error{
							Msg: fmt.Sprintf("switch config failed, diff %s", diff),
						}
					}
					return expected, nil
				},
			}

			return func(g *globalFlags, opt genericCLIOpts) *cobra.Command {
				builder := cmdConfigBuilder{
					genericCLIOpts: opt,
					globalFlags:    g,
					svc:            svc,
				}
				return builder.cmd()
			}
		}
		for _, tt := range tests {
			fn := func(t *testing.T) {
				builder := newInfluxCmdBuilder(
					in(new(bytes.Buffer)),
					out(ioutil.Discard),
				)
				cmd := builder.cmd(cmdFn(tt.original, tt.expected))
				cmd.SetArgs([]string{"config", tt.arg})
				require.NoError(t, cmd.Execute())
			}
			t.Run(tt.name, fn)
		}
	})

	t.Run("set", func(t *testing.T) {
		tests := []struct {
			name     string
			original config.Configs
			expected config.Config
			flags    []string
		}{
			{
				name: "basic",
				flags: []string{
					"--name", "default",
					"--org", "org1",
					"--url", "http://localhost:9999",
					"--token", "tok1",
					"--active",
				},
				original: config.Configs{
					"default": {
						Org:    "org2",
						Active: false,
						Token:  "tok2",
						Host:   "http://localhost:8888",
					},
				},
				expected: config.Config{
					Name:   "default",
					Org:    "org1",
					Active: true,
					Token:  "tok1",
					Host:   "http://localhost:9999",
				},
			},
			{
				name: "short",
				flags: []string{
					"-n", "default",
					"-o", "org1",
					"-u", "http://localhost:9999",
					"-t", "tok1",
					"-a",
				},
				original: config.Configs{
					"default": {
						Org:    "org2",
						Active: false,
						Token:  "tok2",
						Host:   "http://localhost:8888",
					},
				},
				expected: config.Config{
					Name:   "default",
					Org:    "org1",
					Active: true,
					Token:  "tok1",
					Host:   "http://localhost:9999",
				},
			},
		}
		cmdFn := func(original config.Configs, expected config.Config) func(*globalFlags, genericCLIOpts) *cobra.Command {
			svc := &mockConfigService{
				UpdateConfigFn: func(cfg config.Config) (config.Config, error) {
					if diff := cmp.Diff(expected, cfg); diff != "" {
						return config.Config{}, &influxdb.Error{
							Msg: fmt.Sprintf("update config failed, diff %s", diff),
						}
					}
					return expected, nil
				},
			}

			return func(g *globalFlags, opt genericCLIOpts) *cobra.Command {
				builder := cmdConfigBuilder{
					genericCLIOpts: opt,
					globalFlags:    g,
					svc:            svc,
				}
				return builder.cmd()
			}
		}
		for _, tt := range tests {
			fn := func(t *testing.T) {
				builder := newInfluxCmdBuilder(
					in(new(bytes.Buffer)),
					out(ioutil.Discard),
				)
				cmd := builder.cmd(cmdFn(tt.original, tt.expected))
				cmd.SetArgs(append([]string{"config", "set"}, tt.flags...))
				require.NoError(t, cmd.Execute())
			}
			t.Run(tt.name, fn)
		}
	})

	t.Run("delete", func(t *testing.T) {
		tests := []struct {
			name     string
			original config.Configs
			expected config.Config
			flags    []string
		}{
			{
				name: "basic",
				flags: []string{
					"--name", "default",
				},
				original: config.Configs{
					"default": {
						Name:   "default",
						Org:    "org2",
						Active: false,
						Token:  "tok2",
						Host:   "http://localhost:8888",
					},
				},
				expected: config.Config{
					Name:   "default",
					Org:    "org2",
					Active: false,
					Token:  "tok2",
					Host:   "http://localhost:8888",
				},
			},
			{
				name: "short",
				flags: []string{
					"-n", "default",
				},
				original: config.Configs{
					"default": {
						Name:   "default",
						Org:    "org2",
						Active: false,
						Token:  "tok2",
						Host:   "http://localhost:8888",
					},
				},
				expected: config.Config{
					Name:   "default",
					Org:    "org2",
					Active: false,
					Token:  "tok2",
					Host:   "http://localhost:8888",
				},
			},
		}
		cmdFn := func(original config.Configs, expected config.Config) func(*globalFlags, genericCLIOpts) *cobra.Command {
			svc := &mockConfigService{
				DeleteConfigFn: func(name string) (config.Config, error) {
					var cfg config.Config
					for _, item := range original {
						if item.Name == name {
							cfg = item
							break
						}
					}
					if diff := cmp.Diff(expected, cfg); diff != "" {
						return config.Config{}, &influxdb.Error{
							Msg: fmt.Sprintf("delete config failed, diff %s", diff),
						}
					}
					return expected, nil
				},
			}

			return func(g *globalFlags, opt genericCLIOpts) *cobra.Command {
				builder := cmdConfigBuilder{
					genericCLIOpts: opt,
					globalFlags:    g,
					svc:            svc,
				}
				return builder.cmd()
			}
		}
		for _, tt := range tests {
			fn := func(t *testing.T) {
				builder := newInfluxCmdBuilder(
					in(new(bytes.Buffer)),
					out(ioutil.Discard),
				)
				cmd := builder.cmd(cmdFn(tt.original, tt.expected))
				cmd.SetArgs(append([]string{"config", "delete"}, tt.flags...))
				require.NoError(t, cmd.Execute())
			}
			t.Run(tt.name, fn)
		}
	})

	t.Run("list", func(t *testing.T) {
		tests := []struct {
			name     string
			expected config.Configs
		}{
			{
				name: "basic",
				expected: config.Configs{
					"default": {
						Org:    "org2",
						Active: false,
						Token:  "tok2",
						Host:   "http://localhost:8888",
					},
					"kubone": {
						Org:    "org1",
						Active: false,
						Token:  "tok1",
						Host:   "http://localhost:9999",
					},
				},
			},
		}
		cmdFn := func(expected config.Configs) func(*globalFlags, genericCLIOpts) *cobra.Command {
			svc := &mockConfigService{
				ListConfigsFn: func() (config.Configs, error) {
					return expected, nil
				},
			}

			return func(g *globalFlags, opt genericCLIOpts) *cobra.Command {
				builder := cmdConfigBuilder{
					genericCLIOpts: opt,
					globalFlags:    g,
					svc:            svc,
				}
				return builder.cmd()
			}
		}
		for _, tt := range tests {
			fn := func(t *testing.T) {
				builder := newInfluxCmdBuilder(
					in(new(bytes.Buffer)),
					out(ioutil.Discard),
				)
				cmd := builder.cmd(cmdFn(tt.expected))
				cmd.SetArgs([]string{"config", "list"})
				require.NoError(t, cmd.Execute())
			}
			t.Run(tt.name, fn)
		}
	})
}

// mockConfigService mocks the ConfigService.
type mockConfigService struct {
	CreateConfigFn func(config.Config) (config.Config, error)
	DeleteConfigFn func(name string) (config.Config, error)
	UpdateConfigFn func(config.Config) (config.Config, error)
	ParseConfigsFn func() (config.Configs, error)
	SwitchActiveFn func(name string) (config.Config, error)
	ListConfigsFn  func() (config.Configs, error)
}

// ParseConfigs returns the parse fn.
func (s *mockConfigService) ParseConfigs() (config.Configs, error) {
	return s.ParseConfigsFn()
}

// CreateConfig create a config.
func (s *mockConfigService) CreateConfig(cfg config.Config) (config.Config, error) {
	return s.CreateConfigFn(cfg)
}

// DeleteConfig will delete by name.
func (s *mockConfigService) DeleteConfig(name string) (config.Config, error) {
	return s.DeleteConfigFn(name)
}

// UpdateConfig will update the config.
func (s *mockConfigService) UpdateConfig(up config.Config) (config.Config, error) {
	return s.UpdateConfigFn(up)
}

// SwitchActive active the config by name.
func (s *mockConfigService) SwitchActive(name string) (config.Config, error) {
	return s.SwitchActiveFn(name)
}

// ListConfigs lists all the configs.
func (s *mockConfigService) ListConfigs() (config.Configs, error) {
	return s.ListConfigsFn()
}
