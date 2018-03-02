// Package run is the run (default) subcommand for the influxd command.
package run

import (
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"time"

	"github.com/influxdata/influxdb/logger"
	"go.uber.org/zap"
)

const logo = `
 8888888           .d888 888                   8888888b.  888888b.
   888            d88P"  888                   888  "Y88b 888  "88b
   888            888    888                   888    888 888  .88P
   888   88888b.  888888 888 888  888 888  888 888    888 8888888K.
   888   888 "88b 888    888 888  888  Y8bd8P' 888    888 888  "Y88b
   888   888  888 888    888 888  888   X88K   888    888 888    888
   888   888  888 888    888 Y88b 888 .d8""8b. 888  .d88P 888   d88P
 8888888 888  888 888    888  "Y88888 888  888 8888888P"  8888888P"

`

// Command represents the command executed by "influxd run".
type Command struct {
	Version   string
	Branch    string
	Commit    string
	BuildTime string

	closing chan struct{}
	pidfile string
	Closed  chan struct{}

	Stdin  io.Reader
	Stdout io.Writer
	Stderr io.Writer
	Logger *zap.Logger

	Server *Server

	// How to get environment variables. Normally set to os.Getenv, except for tests.
	Getenv func(string) string
}

// NewCommand return a new instance of Command.
func NewCommand() *Command {
	return &Command{
		closing: make(chan struct{}),
		Closed:  make(chan struct{}),
		Stdin:   os.Stdin,
		Stdout:  os.Stdout,
		Stderr:  os.Stderr,
		Logger:  zap.NewNop(),
	}
}

// Run parses the config from args and runs the server.
func (cmd *Command) Run(args ...string) error {
	// Parse the command line flags.
	options, err := cmd.ParseFlags(args...)
	if err != nil {
		return err
	}

	config, err := cmd.ParseConfig(options.GetConfigPath())
	if err != nil {
		return fmt.Errorf("parse config: %s", err)
	}

	// Apply any environment variables on top of the parsed config
	if err := config.ApplyEnvOverrides(cmd.Getenv); err != nil {
		return fmt.Errorf("apply env config: %v", err)
	}

	// Validate the configuration.
	if err := config.Validate(); err != nil {
		return fmt.Errorf("%s. To generate a valid configuration file run `influxd config > influxdb.generated.conf`", err)
	}

	var logErr error
	if cmd.Logger, logErr = config.Logging.New(cmd.Stderr); logErr != nil {
		// assign the default logger
		cmd.Logger = logger.New(cmd.Stderr)
	}

	// Print sweet InfluxDB logo.
	if !config.Logging.SuppressLogo && logger.IsTerminal(cmd.Stdout) {
		fmt.Fprint(cmd.Stdout, logo)
	}

	// Mark start-up in log.
	cmd.Logger.Info("InfluxDB starting",
		zap.String("version", cmd.Version),
		zap.String("branch", cmd.Branch),
		zap.String("commit", cmd.Commit))
	cmd.Logger.Info("Go runtime",
		zap.String("version", runtime.Version()),
		zap.Int("maxprocs", runtime.GOMAXPROCS(0)))

	// If there was an error on startup when creating the logger, output it now.
	if logErr != nil {
		cmd.Logger.Error("Unable to configure logger", zap.Error(logErr))
	}

	// Write the PID file.
	if err := cmd.writePIDFile(options.PIDFile); err != nil {
		return fmt.Errorf("write pid file: %s", err)
	}
	cmd.pidfile = options.PIDFile

	if config.HTTPD.PprofEnabled {
		// Turn on block and mutex profiling.
		runtime.SetBlockProfileRate(int(1 * time.Second))
		runtime.SetMutexProfileFraction(1) // Collect every sample
	}

	// Create server from config and start it.
	buildInfo := &BuildInfo{
		Version: cmd.Version,
		Commit:  cmd.Commit,
		Branch:  cmd.Branch,
		Time:    cmd.BuildTime,
	}
	s, err := NewServer(config, buildInfo)
	if err != nil {
		return fmt.Errorf("create server: %s", err)
	}
	s.Logger = cmd.Logger
	s.CPUProfile = options.CPUProfile
	s.MemProfile = options.MemProfile
	if err := s.Open(); err != nil {
		return fmt.Errorf("open server: %s", err)
	}
	cmd.Server = s

	// Begin monitoring the server's error channel.
	go cmd.monitorServerErrors()

	return nil
}

// Close shuts down the server.
func (cmd *Command) Close() error {
	defer close(cmd.Closed)
	defer cmd.removePIDFile()
	close(cmd.closing)
	if cmd.Server != nil {
		return cmd.Server.Close()
	}
	return nil
}

func (cmd *Command) monitorServerErrors() {
	logger := log.New(cmd.Stderr, "", log.LstdFlags)
	for {
		select {
		case err := <-cmd.Server.Err():
			logger.Println(err)
		case <-cmd.closing:
			return
		}
	}
}

func (cmd *Command) removePIDFile() {
	if cmd.pidfile != "" {
		if err := os.Remove(cmd.pidfile); err != nil {
			cmd.Logger.Error("Unable to remove pidfile", zap.Error(err))
		}
	}
}

// ParseFlags parses the command line flags from args and returns an options set.
func (cmd *Command) ParseFlags(args ...string) (Options, error) {
	var options Options
	fs := flag.NewFlagSet("", flag.ContinueOnError)
	fs.StringVar(&options.ConfigPath, "config", "", "")
	fs.StringVar(&options.PIDFile, "pidfile", "", "")
	// Ignore hostname option.
	_ = fs.String("hostname", "", "")
	fs.StringVar(&options.CPUProfile, "cpuprofile", "", "")
	fs.StringVar(&options.MemProfile, "memprofile", "", "")
	fs.Usage = func() { fmt.Fprintln(cmd.Stderr, usage) }
	if err := fs.Parse(args); err != nil {
		return Options{}, err
	}
	return options, nil
}

// writePIDFile writes the process ID to path.
func (cmd *Command) writePIDFile(path string) error {
	// Ignore if path is not set.
	if path == "" {
		return nil
	}

	// Ensure the required directory structure exists.
	if err := os.MkdirAll(filepath.Dir(path), 0777); err != nil {
		return fmt.Errorf("mkdir: %s", err)
	}

	// Retrieve the PID and write it.
	pid := strconv.Itoa(os.Getpid())
	if err := ioutil.WriteFile(path, []byte(pid), 0666); err != nil {
		return fmt.Errorf("write file: %s", err)
	}

	return nil
}

// ParseConfig parses the config at path.
// It returns a demo configuration if path is blank.
func (cmd *Command) ParseConfig(path string) (*Config, error) {
	// Use demo configuration if no config path is specified.
	if path == "" {
		cmd.Logger.Info("No configuration provided, using default settings")
		return NewDemoConfig()
	}

	cmd.Logger.Info("Loading configuration file", zap.String("path", path))

	config := NewConfig()
	if err := config.FromTomlFile(path); err != nil {
		return nil, err
	}

	return config, nil
}

const usage = `Runs the InfluxDB server.

Usage: influxd run [flags]

    -config <path>
            Set the path to the configuration file.
            This defaults to the environment variable INFLUXDB_CONFIG_PATH,
            ~/.influxdb/influxdb.conf, or /etc/influxdb/influxdb.conf if a file
            is present at any of these locations.
            Disable the automatic loading of a configuration file using
            the null device (such as /dev/null).
    -pidfile <path>
            Write process ID to a file.
    -cpuprofile <path>
            Write CPU profiling information to a file.
    -memprofile <path>
            Write memory usage information to a file.
`

// Options represents the command line options that can be parsed.
type Options struct {
	ConfigPath string
	PIDFile    string
	CPUProfile string
	MemProfile string
}

// GetConfigPath returns the config path from the options.
// It will return a path by searching in this order:
//   1. The CLI option in ConfigPath
//   2. The environment variable INFLUXDB_CONFIG_PATH
//   3. The first influxdb.conf file on the path:
//        - ~/.influxdb
//        - /etc/influxdb
func (opt *Options) GetConfigPath() string {
	if opt.ConfigPath != "" {
		if opt.ConfigPath == os.DevNull {
			return ""
		}
		return opt.ConfigPath
	} else if envVar := os.Getenv("INFLUXDB_CONFIG_PATH"); envVar != "" {
		return envVar
	}

	for _, path := range []string{
		os.ExpandEnv("${HOME}/.influxdb/influxdb.conf"),
		"/etc/influxdb/influxdb.conf",
	} {
		if _, err := os.Stat(path); err == nil {
			return path
		}
	}
	return ""
}
