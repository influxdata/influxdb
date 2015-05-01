package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"

	"github.com/influxdb/influxdb/graphite"
	"github.com/influxdb/influxdb/opentsdb"
)

type RunCommand struct {
	// The logger passed to the ticker during execution.
	logWriter *os.File
	config    *Config
	hostname  string
	node      *Node
}

func NewRunCommand() *RunCommand {
	return &RunCommand{}
}

func (cmd *RunCommand) ParseConfig(path string) error {
	// Parse configuration file from disk.
	if path != "" {
		var err error
		cmd.config, err = ParseConfigFile(path)
		if err != nil {
			return fmt.Errorf("error parsing configuration %s - %s\n", path, err)
		}
		log.Printf("using configuration at: %s\n", path)
	} else {
		var err error
		cmd.config, err = NewTestConfig()

		if err != nil {
			return fmt.Errorf("error parsing default config: %s\n", err)
		}

		log.Println("no configuration provided, using default settings")
	}

	// Override config properties.
	if cmd.hostname != "" {
		cmd.config.Hostname = cmd.hostname
	}
	cmd.node.hostname = cmd.config.Hostname
	return nil
}

func (cmd *RunCommand) Run(args ...string) error {
	// Parse command flags.
	fs := flag.NewFlagSet("", flag.ExitOnError)
	var configPath, pidfile, hostname, join, cpuprofile, memprofile string

	fs.StringVar(&configPath, "config", "", "")
	fs.StringVar(&pidfile, "pidfile", "", "")
	fs.StringVar(&hostname, "hostname", "", "")
	fs.StringVar(&join, "join", "", "")
	fs.StringVar(&cpuprofile, "cpuprofile", "", "")
	fs.StringVar(&memprofile, "memprofile", "", "")

	fs.Usage = printRunUsage
	fs.Parse(args)
	cmd.hostname = hostname

	// Start profiling, if set.
	startProfiling(cpuprofile, memprofile)
	defer stopProfiling()

	// Print sweet InfluxDB logo and write the process id to file.
	fmt.Print(logo)
	writePIDFile(pidfile)

	// Set parallelism.
	runtime.GOMAXPROCS(runtime.NumCPU())
	log.Printf("GOMAXPROCS set to %d", runtime.GOMAXPROCS(0))

	// Parse config
	if err := cmd.ParseConfig(configPath); err != nil {
		log.Fatal(err)
	}

	// Use the config JoinURLs by default
	joinURLs := cmd.config.Initialization.JoinURLs

	// If a -join flag was passed, these should override the config
	if join != "" {
		joinURLs = join
	}
	cmd.CheckConfig()
	cmd.Open(cmd.config, joinURLs)

	// Wait indefinitely.
	<-(chan struct{})(nil)
	return nil
}

// CheckConfig validates the configuration
func (cmd *RunCommand) CheckConfig() {
	// Set any defaults that aren't set
	// TODO: bring more defaults in here instead of letting helpers do it

	// Normalize Graphite configs
	for i, _ := range cmd.config.Graphites {
		if cmd.config.Graphites[i].BindAddress == "" {
			cmd.config.Graphites[i].BindAddress = cmd.config.BindAddress
		}
		if cmd.config.Graphites[i].Port == 0 {
			cmd.config.Graphites[i].Port = graphite.DefaultGraphitePort
		}
	}

	// Normalize openTSDB config
	if cmd.config.OpenTSDB.Addr == "" {
		cmd.config.OpenTSDB.Addr = cmd.config.BindAddress
	}

	if cmd.config.OpenTSDB.Port == 0 {
		cmd.config.OpenTSDB.Port = opentsdb.DefaultPort
	}

	// Validate that we have a sane config
	if !(cmd.config.Data.Enabled || cmd.config.Broker.Enabled) {
		log.Fatal("Node must be configured as a broker node, data node, or as both.  Run `influxd config` to generate a valid configuration.")
	}

	if cmd.config.Broker.Enabled && cmd.config.Broker.Dir == "" {
		log.Fatal("Broker.Dir must be specified.  Run `influxd config` to generate a valid configuration.")
	}

	if cmd.config.Data.Enabled && cmd.config.Data.Dir == "" {
		log.Fatal("Data.Dir must be specified.  Run `influxd config` to generate a valid configuration.")
	}
}

func (cmd *RunCommand) Open(config *Config, join string) *Node {
	if config != nil {
		cmd.config = config
	}

	cmd.node = NewNodeWithConfig(cmd.config)
	return cmd.node.Open(join)
}

func (cmd *RunCommand) Close() {
	cmd.node.Close()
}

// write the current process id to a file specified by path.
func writePIDFile(path string) {
	if path == "" {
		return
	}

	// Ensure the required directory structure exists.
	err := os.MkdirAll(filepath.Dir(path), 0755)
	if err != nil {
		log.Fatal(err)
	}

	// Retrieve the PID and write it.
	pid := strconv.Itoa(os.Getpid())
	if err := ioutil.WriteFile(path, []byte(pid), 0644); err != nil {
		log.Fatal(err)
	}
}

// parses a comma-delimited list of URLs.
func parseURLs(s string) (a []url.URL) {
	if s == "" {
		return nil
	}

	for _, s := range strings.Split(s, ",") {
		u, err := url.Parse(s)
		if err != nil {
			log.Fatalf("cannot parse urls: %s", err)
		}
		a = append(a, *u)
	}
	return
}

// returns true if the file exists.
func fileExists(path string) bool {
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return false
	}
	return true
}

func printRunUsage() {
	log.Printf(`usage: run [flags]

run starts the broker and data node server. If this is the first time running
the command then a new cluster will be initialized unless the -join argument
is used.

        -config <path>
                          Set the path to the configuration file.

        -hostname <name>
                          Override the hostname, the 'hostname' configuration
                          option will be overridden.

        -join <url>
                          Joins the server to an existing cluster.

        -pidfile <path>
                          Write process ID to a file.
`)
}
