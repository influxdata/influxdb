package launcher

import (
	"fmt"
	"io"

	"github.com/influxdata/influxdb/v2/kit/cli"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"gopkg.in/yaml.v3"
)

func NewInfluxdPrintConfigCommand(v *viper.Viper, influxdOpts []cli.Opt) (*cobra.Command, error) {

	var keyToPrint string
	printOpts := make([]cli.Opt, len(influxdOpts)+1)

	printOpts[0] = cli.Opt{
		DestP: &keyToPrint,
		Flag:  "key-name",
		Desc:  "config key name; if set, only the resolved value of that key will be printed",
	}
	for i, opt := range influxdOpts {
		printOpts[i+1] = cli.Opt{
			DestP:  opt.DestP,
			Flag:   opt.Flag,
			Hidden: true,
		}
	}

	cmd := &cobra.Command{
		Use:   "print-config",
		Short: "Print the full influxd config resolved from the current environment",
		Long: `
Print config (in YAML) that the influxd server would use if run with the current flags/env vars/config file.

The order of precedence for config options are as follows (1 highest, 3 lowest):
	1. flags
	2. env vars
	3. config file

A config file can be provided via the INFLUXD_CONFIG_PATH env var. If a file is
not provided via an env var, influxd will look in the current directory for a
config.{json|toml|yaml|yml} file. If one does not exist, then it will continue unchanged.

See 'influxd -h' for the full list of config options supported by the server.
`,
		RunE: func(cmd *cobra.Command, _ []string) error {
			var err error
			if keyToPrint == "" {
				err = printAllConfigRunE(printOpts, cmd.OutOrStdout())
			} else {
				err = printOneConfigRunE(printOpts, keyToPrint, cmd.OutOrStdout())
			}

			if err != nil {
				return fmt.Errorf("failed to print config: %w", err)
			}

			return nil
		},
		Args: cobra.NoArgs,
	}
	if err := cli.BindOptions(v, cmd, printOpts); err != nil {
		return nil, err
	}

	return cmd, nil
}

func printAllConfigRunE(configOpts []cli.Opt, out io.Writer) error {
	configMap := make(map[string]interface{}, len(configOpts))

	for _, o := range configOpts {
		configMap[o.Flag] = o.DestP
	}

	return yaml.NewEncoder(out).Encode(configMap)
}

func printOneConfigRunE(configOpts []cli.Opt, key string, out io.Writer) error {
	for _, o := range configOpts {
		if o.Flag != key {
			continue
		}
		return yaml.NewEncoder(out).Encode(o.DestP)
	}

	return fmt.Errorf("key %q not found in config", key)
}
