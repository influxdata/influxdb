package main

import (
	"fmt"

	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/cmd/influx/config"
	"github.com/spf13/cobra"
)

func cmdConfig(f *globalFlags, opt genericCLIOpts) *cobra.Command {
	path, dir, err := defaultConfigPath()
	if err != nil {
		panic(err)
	}
	builder := cmdConfigBuilder{
		genericCLIOpts: opt,
		globalFlags:    f,
		svc: config.LocalConfigsSVC{
			Path: path,
			Dir:  dir,
		},
	}
	builder.globalFlags = f
	return builder.cmd()
}

type cmdConfigBuilder struct {
	genericCLIOpts
	*globalFlags

	name   string
	url    string
	token  string
	active bool
	org    string

	json        bool
	hideHeaders bool

	svc config.ConfigsService
}

func (b *cmdConfigBuilder) cmd() *cobra.Command {
	cmd := b.newCmd("config", b.cmdSwitchActiveRunEFn, false)
	cmd.Short = "Config management commands"
	cmd.Args = cobra.ExactArgs(1)

	cmd.AddCommand(
		b.cmdCreate(),
		b.cmdDelete(),
		b.cmdUpdate(),
		b.cmdList(),
	)
	return cmd
}

func (b *cmdConfigBuilder) cmdSwitchActiveRunEFn(cmd *cobra.Command, args []string) error {
	pp, err := b.svc.ParseConfigs()
	if err != nil {
		return err
	}
	b.name = args[0]
	p0, ok := pp[b.name]
	if !ok {
		return &influxdb.Error{
			Code: influxdb.ENotFound,
			Msg:  fmt.Sprintf("name %q is not found", b.name),
		}
	}
	pp[b.name] = p0

	if err := pp.Switch(b.name); err != nil {
		return err
	}

	if err = b.svc.WriteConfigs(pp); err != nil {
		return err
	}

	return b.printConfigs(configPrintOpts{
		config: cfg{
			name:   b.name,
			Config: pp[b.name],
		},
	})
}

func (b *cmdConfigBuilder) cmdCreate() *cobra.Command {
	cmd := b.newCmd("create", b.cmdCreateRunEFn, false)
	cmd.Short = "Create config"

	b.registerPrintFlags(cmd)
	cmd.Flags().StringVarP(&b.name, "name", "n", "", "The config name (required)")
	cmd.MarkFlagRequired("name")
	cmd.Flags().StringVarP(&b.token, "token", "t", "", "The config token (required)")
	cmd.MarkFlagRequired("token")
	cmd.Flags().StringVarP(&b.url, "url", "u", "", "The config url (required)")
	cmd.MarkFlagRequired("url")

	cmd.Flags().BoolVarP(&b.active, "active", "a", false, "Set it to be the active config")
	cmd.Flags().StringVarP(&b.org, "org", "o", "", "The optional organization name")
	return cmd
}

func (b *cmdConfigBuilder) cmdCreateRunEFn(*cobra.Command, []string) error {
	pp, err := b.svc.ParseConfigs()
	if err != nil {
		return err
	}

	p := config.Config{
		Host:   b.url,
		Token:  b.token,
		Org:    b.org,
		Active: b.active,
	}
	if _, ok := pp[b.name]; ok {
		return &influxdb.Error{
			Code: influxdb.EConflict,
			Msg:  fmt.Sprintf("name %q already exists", b.name),
		}
	}

	pp[b.name] = p
	if p.Active {
		if err := pp.Switch(b.name); err != nil {
			return err
		}
	}

	if err = b.svc.WriteConfigs(pp); err != nil {
		return err
	}

	return b.printConfigs(configPrintOpts{
		config: cfg{
			name:   b.name,
			Config: p,
		},
	})
}

func (b *cmdConfigBuilder) cmdDelete() *cobra.Command {
	cmd := b.newCmd("delete", b.cmdDeleteRunEFn, false)
	cmd.Short = "Delete config"

	b.registerPrintFlags(cmd)
	cmd.Flags().StringVarP(&b.name, "name", "n", "", "The config name (required)")
	cmd.MarkFlagRequired("name")

	return cmd
}

func (b *cmdConfigBuilder) cmdDeleteRunEFn(cmd *cobra.Command, args []string) error {
	pp, err := b.svc.ParseConfigs()
	if err != nil {
		return err
	}

	p, ok := pp[b.name]
	if !ok {
		return &influxdb.Error{
			Code: influxdb.ENotFound,
			Msg:  fmt.Sprintf("name %q is not found", b.name),
		}
	}
	delete(pp, b.name)

	if err = b.svc.WriteConfigs(pp); err != nil {
		return err
	}

	return b.printConfigs(configPrintOpts{
		delete: true,
		config: cfg{
			name:   b.name,
			Config: p,
		},
	})
}

func (b *cmdConfigBuilder) cmdUpdate() *cobra.Command {
	cmd := b.newCmd("set", b.cmdUpdateRunEFn, false)
	cmd.Aliases = []string{"update"}
	cmd.Short = "Update config"

	b.registerPrintFlags(cmd)
	cmd.Flags().StringVarP(&b.name, "name", "n", "", "The config name (required)")
	cmd.MarkFlagRequired("name")

	cmd.Flags().StringVarP(&b.token, "token", "t", "", "The config token (required)")
	cmd.Flags().StringVarP(&b.url, "url", "u", "", "The config url (required)")
	cmd.Flags().BoolVarP(&b.active, "active", "a", false, "Set it to be the active config")
	cmd.Flags().StringVarP(&b.org, "org", "o", "", "The optional organization name")
	return cmd
}

func (b *cmdConfigBuilder) cmdUpdateRunEFn(*cobra.Command, []string) error {
	pp, err := b.svc.ParseConfigs()
	if err != nil {
		return err
	}

	p0, ok := pp[b.name]
	if !ok {
		return &influxdb.Error{
			Code: influxdb.ENotFound,
			Msg:  fmt.Sprintf("name %q is not found", b.name),
		}
	}
	if b.token != "" {
		p0.Token = b.token
	}
	if b.url != "" {
		p0.Host = b.url
	}
	if b.org != "" {
		p0.Org = b.org
	}

	pp[b.name] = p0
	if b.active {
		if err := pp.Switch(b.name); err != nil {
			return err
		}
	}

	if err = b.svc.WriteConfigs(pp); err != nil {
		return err
	}

	return b.printConfigs(configPrintOpts{
		config: cfg{
			name:   b.name,
			Config: pp[b.name],
		},
	})
}

func (b *cmdConfigBuilder) cmdList() *cobra.Command {
	cmd := b.newCmd("list", b.cmdListRunEFn, false)
	cmd.Aliases = []string{"ls"}
	cmd.Short = "List configs"
	b.registerPrintFlags(cmd)
	return cmd
}

func (b *cmdConfigBuilder) cmdListRunEFn(*cobra.Command, []string) error {
	pp, err := b.svc.ParseConfigs()
	if err != nil {
		return err
	}

	var cfgs []cfg
	for n, p := range pp {
		cfgs = append(cfgs, cfg{
			name:   n,
			Config: p,
		})
	}

	return b.printConfigs(configPrintOpts{configs: cfgs})
}

func (b *cmdConfigBuilder) registerPrintFlags(cmd *cobra.Command) {
	registerPrintOptions(cmd, &b.hideHeaders, &b.json)
}

func (b *cmdConfigBuilder) printConfigs(opts configPrintOpts) error {
	if b.json {
		var v interface{} = opts.configs
		if opts.configs == nil {
			v = opts.config
		}
		return b.writeJSON(v)
	}

	w := b.newTabWriter()
	defer w.Flush()

	w.HideHeaders(b.hideHeaders)

	headers := []string{"Active", "Name", "URL", "Org"}
	if opts.delete {
		headers = append(headers, "Deleted")
	}
	w.WriteHeaders(headers...)

	if opts.configs == nil {
		opts.configs = append(opts.configs, opts.config)
	}
	for _, c := range opts.configs {
		var active string
		if c.Active {
			active = "*"
		}
		m := map[string]interface{}{
			"Active": active,
			"Name":   c.name,
			"URL":    c.Host,
			"Org":    c.Org,
		}
		if opts.delete {
			m["Deleted"] = true
		}

		w.Write(m)
	}

	return nil
}

type (
	configPrintOpts struct {
		delete  bool
		config  cfg
		configs []cfg
	}

	cfg struct {
		name string
		config.Config
	}
)
