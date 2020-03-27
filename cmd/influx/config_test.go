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
			expected config.Configs
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
				expected: config.Configs{
					"default": {
						Org:    "org1",
						Active: true,
						Token:  "tok1",
						Host:   "http://localhost:9999",
					},
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
				expected: config.Configs{
					"default": {
						Org:    "org1",
						Active: true,
						Token:  "tok1",
						Host:   "http://localhost:9999",
					},
				},
			},
		}
		cmdFn := func(orginal, expected config.Configs) func(*globalFlags, genericCLIOpts) *cobra.Command {
			svc := &config.MockConfigService{
				ParseConfigsFn: func() (config.Configs, error) {
					return orginal, nil
				},
				WriteConfigsFn: func(pp config.Configs) error {
					if diff := cmp.Diff(expected, pp); diff != "" {
						return &influxdb.Error{
							Msg: fmt.Sprintf("write configs failed, diff %s", diff),
						}
					}
					return nil
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
			expected config.Configs
			arg      string
		}{
			{
				name: "basic",
				arg:  "default",
				original: config.Configs{
					"config1": {
						Org:    "org2",
						Active: true,
						Token:  "tok2",
						Host:   "http://localhost:8888",
					},
					"default": {
						Org:    "org1",
						Active: false,
						Token:  "tok1",
						Host:   "http://localhost:9999",
					},
				},
				expected: config.Configs{
					"config1": {
						Org:    "org2",
						Active: false,
						Token:  "tok2",
						Host:   "http://localhost:8888",
					},
					"default": {
						Org:    "org1",
						Active: true,
						Token:  "tok1",
						Host:   "http://localhost:9999",
					},
				},
			},
		}
		cmdFn := func(orginal, expected config.Configs) func(*globalFlags, genericCLIOpts) *cobra.Command {
			svc := &config.MockConfigService{
				ParseConfigsFn: func() (config.Configs, error) {
					return orginal, nil
				},
				WriteConfigsFn: func(pp config.Configs) error {
					if diff := cmp.Diff(expected, pp); diff != "" {
						return &influxdb.Error{
							Msg: fmt.Sprintf("write configs failed, diff %s", diff),
						}
					}
					return nil
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
			expected config.Configs
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
				expected: config.Configs{
					"default": {
						Org:    "org1",
						Active: true,
						Token:  "tok1",
						Host:   "http://localhost:9999",
					},
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
				expected: config.Configs{
					"default": {
						Org:    "org1",
						Active: true,
						Token:  "tok1",
						Host:   "http://localhost:9999",
					},
				},
			},
		}
		cmdFn := func(orginal, expected config.Configs) func(*globalFlags, genericCLIOpts) *cobra.Command {
			svc := &config.MockConfigService{
				ParseConfigsFn: func() (config.Configs, error) {
					return orginal, nil
				},
				WriteConfigsFn: func(pp config.Configs) error {
					if diff := cmp.Diff(expected, pp); diff != "" {
						return &influxdb.Error{
							Msg: fmt.Sprintf("write configs failed, diff %s", diff),
						}
					}
					return nil
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
			expected config.Configs
			flags    []string
		}{
			{
				name: "basic",
				flags: []string{
					"--name", "default",
				},
				original: config.Configs{
					"default": {
						Org:    "org2",
						Active: false,
						Token:  "tok2",
						Host:   "http://localhost:8888",
					},
				},
				expected: make(config.Configs),
			},
			{
				name: "short",
				flags: []string{
					"-n", "default",
				},
				original: config.Configs{
					"default": {
						Org:    "org2",
						Active: false,
						Token:  "tok2",
						Host:   "http://localhost:8888",
					},
				},
				expected: make(config.Configs),
			},
		}
		cmdFn := func(orginal, expected config.Configs) func(*globalFlags, genericCLIOpts) *cobra.Command {
			svc := &config.MockConfigService{
				ParseConfigsFn: func() (config.Configs, error) {
					return orginal, nil
				},
				WriteConfigsFn: func(pp config.Configs) error {
					if diff := cmp.Diff(expected, pp); diff != "" {
						return &influxdb.Error{
							Msg: fmt.Sprintf("write configs failed, diff %s", diff),
						}
					}
					return nil
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
			svc := &config.MockConfigService{
				ParseConfigsFn: func() (config.Configs, error) {
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
