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

	svc config.ConfigsService
}

func (b *cmdConfigBuilder) cmd() *cobra.Command {
	cmd := b.newCmd("config", nil)
	cmd.Short = "Config management commands"
	cmd.Run = seeHelp
	cmd.AddCommand(
		b.cmdCreate(),
		b.cmdDelete(),
		b.cmdUpdate(),
		b.cmdList(),
	)
	return cmd
}

func (b *cmdConfigBuilder) cmdCreate() *cobra.Command {
	cmd := b.newCmd("create", nil)
	cmd.RunE = b.cmdCreateRunEFn
	cmd.Short = "Create config"
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
	active := ""
	if p.Active {
		active = "*"
		if err := pp.Switch(b.name); err != nil {
			return err
		}
	}
	if err = b.svc.WriteConfigs(pp); err != nil {
		return err
	}
	w := b.newTabWriter()
	w.WriteHeaders(
		"Active",
		"Name",
		"URL",
		"Org",
		"Created",
	)

	w.Write(map[string]interface{}{
		"Active":  active,
		"Name":    b.name,
		"URL":     p.Host,
		"Org":     p.Org,
		"Created": true,
	})
	w.Flush()
	return nil
}

func (b *cmdConfigBuilder) cmdDelete() *cobra.Command {
	cmd := b.newCmd("delete", nil)
	cmd.RunE = b.cmdDeleteRunEFn
	cmd.Short = "Delete config"

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
	w := b.newTabWriter()
	w.WriteHeaders(
		"Name",
		"URL",
		"Org",
		"Deleted",
	)

	w.Write(map[string]interface{}{
		"Name":    b.name,
		"URL":     p.Host,
		"Org":     p.Org,
		"Deleted": true,
	})
	w.Flush()
	return nil
}

func (b *cmdConfigBuilder) cmdUpdate() *cobra.Command {
	cmd := b.newCmd("set", b.cmdUpdateRunEFn)
	cmd.Aliases = []string{"update"}
	cmd.RunE = b.cmdUpdateRunEFn
	cmd.Short = "Update config"
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
	active := ""
	if b.active {
		active = "*"
		if err := pp.Switch(b.name); err != nil {
			return err
		}
	}
	if err = b.svc.WriteConfigs(pp); err != nil {
		return err
	}
	w := b.newTabWriter()
	w.WriteHeaders(
		"Active",
		"Name",
		"URL",
		"Org",
		"Updated",
	)

	w.Write(map[string]interface{}{
		"Active":  active,
		"Name":    b.name,
		"URL":     p0.Host,
		"Org":     p0.Org,
		"Updated": true,
	})
	w.Flush()
	return nil
}

func (b *cmdConfigBuilder) cmdList() *cobra.Command {
	cmd := b.newCmd("list", nil)
	cmd.RunE = b.cmdListRunEFn
	cmd.Aliases = []string{"ls"}
	cmd.Short = "List configs"
	return cmd
}

func (b *cmdConfigBuilder) cmdListRunEFn(*cobra.Command, []string) error {
	pp, err := b.svc.ParseConfigs()
	if err != nil {
		return err
	}
	w := b.newTabWriter()
	w.WriteHeaders(
		"Active",
		"Name",
		"URL",
		"Org",
	)
	for n, p := range pp {
		var active string
		if p.Active {
			active = "*"
		}
		w.Write(map[string]interface{}{
			"Active": active,
			"Name":   n,
			"URL":    p.Host,
			"Org":    p.Org,
		})
	}

	w.Flush()
	return nil
}
