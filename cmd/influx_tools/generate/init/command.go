package init

import (
	"errors"
	"flag"
	"io"
	"os"

	"github.com/influxdata/influxdb/cmd/influx_tools/generate"
	"github.com/influxdata/influxdb/cmd/influx_tools/server"
)

// Command represents the program execution for "store query".
type Command struct {
	Stdin  io.Reader
	Stdout io.Writer
	Stderr io.Writer
	server server.Interface

	configPath string
	printOnly  bool
	spec       generate.StorageSpec
}

// NewCommand returns a new instance of Command.
func NewCommand(server server.Interface) *Command {
	return &Command{
		Stdin:  os.Stdin,
		Stdout: os.Stdout,
		Stderr: os.Stderr,
		server: server,
	}
}

func (cmd *Command) Run(args []string) (err error) {
	err = cmd.parseFlags(args)
	if err != nil {
		return err
	}

	err = cmd.server.Open(cmd.configPath)
	if err != nil {
		return err
	}

	plan, err := cmd.spec.Plan(cmd.server)
	if err != nil {
		return err
	}

	plan.PrintPlan(cmd.Stdout)

	if !cmd.printOnly {
		return plan.InitMetadata(cmd.server.MetaClient())
	}

	return nil
}

func (cmd *Command) parseFlags(args []string) error {
	fs := flag.NewFlagSet("gen-init", flag.ContinueOnError)
	fs.StringVar(&cmd.configPath, "config", "", "Config file")
	fs.BoolVar(&cmd.printOnly, "print", false, "Print data spec only")
	cmd.spec.AddFlags(fs)

	if err := fs.Parse(args); err != nil {
		return err
	}

	if cmd.spec.Database == "" {
		return errors.New("database is required")
	}

	if cmd.spec.Retention == "" {
		return errors.New("retention policy is required")
	}

	return nil
}
