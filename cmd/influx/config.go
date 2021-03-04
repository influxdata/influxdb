package main

import (
	"errors"
	"fmt"
	"net/url"
	"path/filepath"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/cmd/influx/config"
	"github.com/spf13/cobra"
)

func cmdConfig(f *globalFlags, opt genericCLIOpts) (*cobra.Command, error) {
	builder := cmdConfigBuilder{
		genericCLIOpts: opt,
		globalFlags:    f,
		svcFn:          newConfigService,
	}
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

	svcFn func(path string) config.Service
}

func (b *cmdConfigBuilder) cmd() (*cobra.Command, error) {
	cmd := b.newCmd("config [config name]", b.cmdSwitchActiveRunEFn, false)
	cmd.Args = cobra.ArbitraryArgs
	cmd.Short = "Config management commands"
	cmd.Long = `
	Providing no argument to the config command will print the active configuration. When
	an argument is provided, the active config will be switched to the config with a name
	matching that of the argument provided.

	Examples:
		# show active config
		influx config

		# set active config to previously active config
		influx config -

		# set active config
		influx config $CONFIG_NAME

	The influx config command displays the active InfluxDB connection configuration and
	manages multiple connection configurations stored, by default, in ~/.influxdbv2/configs.
	Each connection includes a URL, token, associated organization, and active setting.
	InfluxDB reads the token from the active connection configuration, so you don't have
	to manually enter a token to log into InfluxDB.

	For information about the config command, see
	https://docs.influxdata.com/influxdb/latest/reference/cli/influx/config/
`

	if err := b.registerFilepath(cmd); err != nil {
		return nil, err
	}
	builders := []func() (*cobra.Command, error){b.cmdCreate, b.cmdDelete, b.cmdUpdate, b.cmdList}
	for _, builder := range builders {
		subcmd, err := builder()
		if err != nil {
			return nil, err
		}
		cmd.AddCommand(subcmd)
	}
	return cmd, nil
}

func (b *cmdConfigBuilder) cmdSwitchActiveRunEFn(cmd *cobra.Command, args []string) error {
	svc := b.newConfigSVC()

	if len(args) > 0 {
		cfg, err := svc.SwitchActive(args[0])
		if err != nil {
			return err
		}

		return b.printConfigs(configPrintOpts{
			config: cfg,
		})
	}

	configs, err := svc.ListConfigs()
	if err != nil {
		return err
	}

	var active config.Config
	for _, cfg := range configs {
		if cfg.Active {
			active = cfg
			break
		}
	}
	if !active.Active {
		return nil
	}

	return b.printConfigs(configPrintOpts{
		config: active,
	})
}

func (b *cmdConfigBuilder) cmdCreate() (*cobra.Command, error) {
	cmd := b.newCmd("create", b.cmdCreateRunEFn, false)
	cmd.Short = "Create config"
	cmd.Long = `
	The influx config create command creates a new InfluxDB connection configuration
	and stores it in the configs file (by default, stored at ~/.influxdbv2/configs).

	Examples:
		# create a config and set it active
		influx config create -a -n $CFG_NAME -u $HOST_URL -t $TOKEN -o $ORG_NAME

		# create a config and without setting it active
		influx config create -n $CFG_NAME -u $HOST_URL -t $TOKEN -o $ORG_NAME

	For information about the config command, see
	https://docs.influxdata.com/influxdb/latest/reference/cli/influx/config/
	and
	https://docs.influxdata.com/influxdb/latest/reference/cli/influx/config/create/`

	if err := b.registerFilepath(cmd); err != nil {
		return nil, err
	}
	if err := b.registerPrintFlags(cmd); err != nil {
		return nil, err
	}
	if err := b.registerConfigSettingFlags(cmd); err != nil {
		return nil, err
	}
	if err := cmd.MarkFlagRequired("token"); err != nil {
		return nil, fmt.Errorf("failed to mark 'token' as required: %w", err)
	}
	if err := cmd.MarkFlagRequired("host-url"); err != nil {
		return nil, fmt.Errorf("failed to mark 'host-url' as required: %w", err)
	}
	return cmd, nil
}

func (b *cmdConfigBuilder) cmdCreateRunEFn(*cobra.Command, []string) error {
	svc := b.newConfigSVC()

	host, err := b.getValidHostURL()
	if err != nil {
		return err
	}

	cfg, err := svc.CreateConfig(config.Config{
		Name:   b.name,
		Host:   host,
		Token:  b.token,
		Org:    b.org,
		Active: b.active,
	})
	if err != nil {
		return err
	}

	return b.printConfigs(configPrintOpts{
		config: cfg,
	})
}

func (b *cmdConfigBuilder) cmdDelete() (*cobra.Command, error) {
	cmd := b.newCmd("rm [cfg_name]", b.cmdDeleteRunEFn, false)
	cmd.Aliases = []string{"delete", "remove"}
	cmd.Args = cobra.ArbitraryArgs
	cmd.Short = "Delete config"
	cmd.Long = `
	The influx config delete command deletes an InfluxDB connection configuration from
	the configs file (by default, stored at ~/.influxdbv2/configs).

	Examples:
		# delete a config
		influx config rm $CFG_NAME

		# delete multiple configs
		influx config rm $CFG_NAME_1 $CFG_NAME_2

	For information about the config command, see
	https://docs.influxdata.com/influxdb/latest/reference/cli/influx/config/
	and
	https://docs.influxdata.com/influxdb/latest/reference/cli/influx/config/rm/`

	if err := b.registerPrintFlags(cmd); err != nil {
		return nil, err
	}
	cmd.Flags().StringVarP(&b.name, "name", "n", "", "The config name (required)")
	if err := cmd.Flags().MarkDeprecated("name", "provide the name as an arg; example: influx config rm $CFG_NAME"); err != nil {
		return nil, fmt.Errorf("failed to mark 'name' as deprecated: %w", err)
	}

	return cmd, nil
}

func (b *cmdConfigBuilder) cmdDeleteRunEFn(cmd *cobra.Command, args []string) error {
	svc := b.newConfigSVC()

	deletedConfigs := make(config.Configs)
	for _, name := range append(args, b.name) {
		if name == "" {
			continue
		}

		cfg, err := svc.DeleteConfig(name)
		if influxdb.ErrorCode(err) == influxdb.ENotFound {
			continue
		}
		if err != nil {
			return err
		}
		deletedConfigs[name] = cfg
	}

	return b.printConfigs(configPrintOpts{
		delete:  true,
		configs: deletedConfigs,
	})
}

func (b *cmdConfigBuilder) cmdUpdate() (*cobra.Command, error) {
	cmd := b.newCmd("set", b.cmdUpdateRunEFn, false)
	cmd.Aliases = []string{"update"}
	cmd.Short = "Update config"
	cmd.Long = `
	The influx config set command updates information in an InfluxDB connection
	configuration in the configs file (by default, stored at ~/.influxdbv2/configs).

	Examples:
		# update a config and set active
		influx config set -a -n $CFG_NAME -u $HOST_URL -t $TOKEN -o $ORG_NAME

		# update a config and do not set to active
		influx config set -n $CFG_NAME -u $HOST_URL -t $TOKEN -o $ORG_NAME

	For information about the config command, see
	https://docs.influxdata.com/influxdb/latest/reference/cli/influx/config/
	and
	https://docs.influxdata.com/influxdb/latest/reference/cli/influx/config/set/`

	if err := b.registerPrintFlags(cmd); err != nil {
		return nil, err
	}
	if err := b.registerConfigSettingFlags(cmd); err != nil {
		return nil, err
	}
	return cmd, nil
}

func (b *cmdConfigBuilder) cmdUpdateRunEFn(*cobra.Command, []string) error {
	var host string
	if b.url != "" {
		h, err := b.getValidHostURL()
		if err != nil {
			return err
		}
		host = h
	}

	cfg, err := b.newConfigSVC().UpdateConfig(config.Config{
		Name:   b.name,
		Host:   host,
		Token:  b.token,
		Org:    b.org,
		Active: b.active,
	})
	if err != nil {
		return err
	}

	return b.printConfigs(configPrintOpts{
		config: cfg,
	})
}

func (b *cmdConfigBuilder) cmdList() (*cobra.Command, error) {
	cmd := b.newCmd("ls", b.cmdListRunEFn, false)
	cmd.Aliases = []string{"list"}
	cmd.Short = "List configs"
	cmd.Long = `
	The influx config ls command lists all InfluxDB connection configurations
	in the configs file (by default, stored at ~/.influxdbv2/configs). Each
	connection configuration includes a URL, authentication token, and active
	setting. An asterisk (*) indicates the active configuration.

	Examples:
		# list configs
		influx config ls

		# list configs with long alias
		influx config list

	For information about the config command, see
	https://docs.influxdata.com/influxdb/latest/reference/cli/influx/config/
	and
	https://docs.influxdata.com/influxdb/latest/reference/cli/influx/config/list/`
	if err := b.registerPrintFlags(cmd); err != nil {
		return nil, err
	}
	return cmd, nil
}

func (b *cmdConfigBuilder) cmdListRunEFn(*cobra.Command, []string) error {
	cfgs, err := b.newConfigSVC().ListConfigs()
	if err != nil {
		return err
	}

	return b.printConfigs(configPrintOpts{configs: cfgs})
}

func (b *cmdConfigBuilder) registerConfigSettingFlags(cmd *cobra.Command) error {
	cmd.Flags().StringVarP(&b.name, "config-name", "n", "", "The config name (required)")
	// name is required everywhere
	if err := cmd.MarkFlagRequired("config-name"); err != nil {
		return fmt.Errorf("failed to mark 'config-name' as required: %w", err)
	}

	cmd.Flags().BoolVarP(&b.active, "active", "a", false, "Set as active config")
	cmd.Flags().StringVarP(&b.url, "host-url", "u", "", "The host url (required)")
	cmd.Flags().StringVarP(&b.org, "org", "o", "", "The optional organization name")
	cmd.Flags().StringVarP(&b.token, "token", "t", "", "The token for host (required)")

	// deprecated moving forward, not explicit enough based on feedback
	// the short flags will still be respected but their long form is different.
	cmd.Flags().StringVar(&b.name, "name", "", "The config name (required)")
	if err := cmd.Flags().MarkDeprecated("name", "use the --config-name flag"); err != nil {
		return fmt.Errorf("failed to mark 'name' as deprecated: %w", err)
	}
	cmd.Flags().StringVar(&b.url, "url", "", "The host url (required)")
	if err := cmd.Flags().MarkDeprecated("url", "use the --host-url flag"); err != nil {
		return fmt.Errorf("failed to mark 'url' as deprecated: %w", err)
	}

	return nil
}

func (b *cmdConfigBuilder) registerFilepath(cmd *cobra.Command) error {
	return b.globalFlags.registerFlags(b.viper, cmd, "host", "token", "skip-verify", "trace-debug-id")
}

func (b *cmdConfigBuilder) registerPrintFlags(cmd *cobra.Command) error {
	return registerPrintOptions(b.viper, cmd, &b.hideHeaders, &b.json)
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
		opts.configs = config.Configs{
			opts.config.Name: opts.config,
		}
	}
	for _, c := range opts.configs {
		var active string
		if c.Active {
			active = "*"
		}
		m := map[string]interface{}{
			"Active": active,
			"Name":   c.Name,
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

func (b *cmdConfigBuilder) getValidHostURL() (string, error) {
	u, err := url.Parse(b.url)
	if err != nil {
		return "", err
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return "", errors.New("a scheme of HTTP(S) must be provided for host url")
	}
	return u.String(), nil
}

func (b *cmdConfigBuilder) newConfigSVC() config.Service {
	return b.svcFn(b.globalFlags.filepath)
}

func newConfigService(path string) config.Service {
	return config.NewLocalConfigSVC(path, filepath.Dir(path))
}

type configPrintOpts struct {
	delete  bool
	config  config.Config
	configs config.Configs
}
