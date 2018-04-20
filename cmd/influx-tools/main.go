// The influx-tools command displays detailed information about InfluxDB data files.
package main

import (
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/influxdata/influxdb/cmd"
	"github.com/influxdata/influxdb/cmd/influx-tools/export"
	"github.com/influxdata/influxdb/cmd/influx-tools/help"
	"github.com/influxdata/influxdb/cmd/influx-tools/importer"
	"github.com/influxdata/influxdb/cmd/influx-tools/server"
	"github.com/influxdata/influxdb/cmd/influxd/run"
	"github.com/influxdata/influxdb/services/meta"
	"github.com/influxdata/influxdb/tsdb"
	_ "github.com/influxdata/influxdb/tsdb/engine"
	"go.uber.org/zap"
)

func main() {
	m := NewMain()
	if err := m.Run(os.Args[1:]...); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

// Main represents the program execution.
type Main struct {
	Stdin  io.Reader
	Stdout io.Writer
	Stderr io.Writer
}

// NewMain returns a new instance of Main.
func NewMain() *Main {
	return &Main{
		Stdin:  os.Stdin,
		Stdout: os.Stdout,
		Stderr: os.Stderr,
	}
}

// Run determines and runs the command specified by the CLI args.
func (m *Main) Run(args ...string) error {
	name, args := cmd.ParseCommandName(args)

	// Extract name from args.
	switch name {
	case "", "help":
		if err := help.NewCommand().Run(args...); err != nil {
			return fmt.Errorf("help: %s", err)
		}
	case "export":
		c := export.NewCommand(&ossServer{logger: zap.NewNop()})
		if err := c.Run(args); err != nil {
			return fmt.Errorf("export: %s", err)
		}
	case "import":
		cmd := importer.NewCommand(&ossServer{logger: zap.NewNop()})
		if err := cmd.Run(args); err != nil {
			return fmt.Errorf("import: %s", err)
		}
	default:
		return fmt.Errorf(`unknown command "%s"`+"\n"+`Run 'influx-tools help' for usage`+"\n\n", name)
	}

	return nil
}

type ossServer struct {
	logger *zap.Logger
	config *run.Config
	client *meta.Client
}

func (s *ossServer) Open(path string) (err error) {
	s.config, err = s.parseConfig(path)
	if err != nil {
		return err
	}

	// Validate the configuration.
	if err = s.config.Validate(); err != nil {
		return fmt.Errorf("validate config: %s", err)
	}

	s.client = meta.NewClient(s.config.Meta)
	if err = s.client.Open(); err != nil {
		s.client = nil
		return err
	}
	return nil
}

func (s *ossServer) Close() {
	if s.client != nil {
		s.client.Close()
		s.client = nil
	}
}

func (s *ossServer) MetaClient() server.MetaClient { return s.client }
func (s *ossServer) TSDBConfig() tsdb.Config       { return s.config.Data }
func (s *ossServer) Logger() *zap.Logger           { return s.logger }

// ParseConfig parses the config at path.
// It returns a demo configuration if path is blank.
func (s *ossServer) parseConfig(path string) (*run.Config, error) {
	path = s.resolvePath(path)
	// Use demo configuration if no config path is specified.
	if path == "" {
		return nil, errors.New("missing config file")
	}

	config := run.NewConfig()
	if err := config.FromTomlFile(path); err != nil {
		return nil, err
	}

	return config, nil
}

func (s *ossServer) resolvePath(path string) string {
	if path != "" {
		if path == os.DevNull {
			return ""
		}
		return path
	}

	for _, p := range []string{
		os.ExpandEnv("${HOME}/.influxdb/influxdb.conf"),
		"/etc/influxdb/influxdb.conf",
	} {
		if _, err := os.Stat(p); err == nil {
			return p
		}
	}
	return ""
}
