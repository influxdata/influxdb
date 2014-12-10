package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"code.google.com/p/log4go"
	"github.com/influxdb/influxdb"
	"github.com/influxdb/influxdb/messaging"
)

const stateFilename = "state"

// Command functionality for InfluxDB. Much of this source has been inspired
// by the Go tool set implementation.

// Command is an InfluxDB command
type Command struct {
	// Name is the name of the command
	Name string

	// Options contains a 1-line description of options
	Options string

	// Terse is the brief description of the command, suitable for top-level help.
	Terse string

	// Long is the detailed help shown in the 'influxd help <this-command>' output.
	Long string

	// Exec runs the command.
	// The args are those after the command name
	Exec func(cmd *Command, args []string) error

	// Flag captures flags specific to the command
	Flag flag.FlagSet
}

func (c *Command) Usage() {
	fmt.Fprintf(os.Stderr, "usage: %s\n\n", strings.Join([]string{c.Name, c.Options}, " "))
	fmt.Fprintf(os.Stderr, "%s\n", strings.TrimSpace(c.Long))
	os.Exit(2)
}

func init() {
	cmdCreateCluster.Exec = execCreateCluster
	cmdJoinCluster.Exec = execJoinCluster
	cmdRun.Exec = execRun
	cmdVersion.Exec = execVersion

	// Add the flags all commands have.
	addSharedFlags(cmdCreateCluster)
	addSharedFlags(cmdJoinCluster)
	addSharedFlags(cmdRun)
	addSharedFlags(cmdVersion)
}

// Stores values of shared flags.
var hostname string
var configFile string
var pidFile string

// addSharedFlags add the flags accepted by all commands
func addSharedFlags(cmd *Command) {
	cmd.Flag.StringVar(&hostname, "hostname", "", "")
	cmd.Flag.StringVar(&configFile, "config", "config.sample.toml", "")
	cmd.Flag.StringVar(&pidFile, "pidfile", "", "")
}

// Commands lists the available commands.
// The order here is the order in which they are printed by 'influxd help'.
var commands = []*Command{
	cmdCreateCluster,
	cmdJoinCluster,
	cmdRun,
	cmdVersion,
}

var cmdCreateCluster = &Command{
	Name:    "create-cluster",
	Options: "[create-cluster and shared flags]",
	Terse:   "create a new node that other nodes can join to form a new cluster",
	Long: `
create-cluster creates a completely new node that can act as the first node of a new
cluster. This node must be created as a 'combined' or 'broker' node.

	-role <role>
			Set the role to be 'combined' or 'broker'. 'broker' means it will take part
			in Raft Distributed Consensus. 'combined' means it take part in Raft and
			store time-series data. The default role is 'combined'. Any other role other
			than these two is invalid.

For more about run flags, see 'influxd help run'.
    `,
}
var createClusterRole = cmdCreateCluster.Flag.String("role", "combined", "")

var cmdJoinCluster = &Command{
	Name:    "join-cluster",
	Options: "[join-cluster and shared flags]",
	Terse:   "create a new node that will join an existing cluster",
	Long: `
join-cluster creates a completely new node that will attempt to join an existing cluster.

	-role <role>
			Set the role to be 'combined', 'broker' or 'data'. broker' means it will take
			part in Raft Distributed Consensus. 'data' means it will store time-series data.
			'combined' means it will do both. The default is 'combined'. In role other than
			these three is invalid.

	-seed-servers <servers>
			Set the list of servers the node should contact, to join the cluster. This
			should be comma-delimited list of servers, in the form host:port. This option
			is REQUIRED.

For more about run flags, see 'influxd help run'.
    `,
}
var joinClusterRole = cmdJoinCluster.Flag.String("role", "combined", "")
var joinClusterSeedServers = cmdJoinCluster.Flag.String("seed-servers", "", "")

var cmdRun = &Command{
	Name:    "run",
	Options: "[run flags and shared flags]",
	Terse:   "run node with existing configuration",
	Long: `
run starts the node with any existing cluster configuration. If no cluster configuration is
found, then the node runs in "local" mode. "Local" mode is a single-node mode, that does not
involve any Distributed Consensus. Otherwise, all functionality is in place.

	-config <path>
				Set the path to the configuration file.

	-hostname <name>
				Override the hostname, the 'hostname' configuration option will be overridden.

	-pidfile <path>
				Write process ID to a file.

    `,
}

var cmdVersion = &Command{
	Name:    "version",
	Options: "",
	Terse:   "displays the InfluxDB version",
	Long: `
version displays the InfluxDB version and git commit hash, as set at link time.

    `,
}

func execCreateCluster(cmd *Command, args []string) error {
	config, err := ParseConfigFile(configFile)
	if err != nil {
		return err
	}

	if *createClusterRole != "combined" && *createClusterRole != "broker" {
		return fmt.Errorf("New cluster node must be created as 'combined' or 'broker' role")
	}

	// Broker required -- and it must be initialized since we're creating a cluster.
	b := messaging.NewBroker()
	if err := b.Open(config.Raft.Dir); err != nil {
		return fmt.Errorf("Failed to create cluster", err.Error())
	}
	if err := b.Initialize(); err != nil {
		return fmt.Errorf("Failed to initialize cluster", err.Error())
	}
	if *createClusterRole == "combined" {
		if _, err := os.Stat(config.Cluster.Dir); err == nil {
			return fmt.Errorf("Failed to initialize cluster because storage directory exists")
		}
		// Now create the storage directory.
		if err := os.MkdirAll(config.Cluster.Dir, 0744); err != nil {
			return fmt.Errorf("failed to create cluster directory", err.Error())
		}
	}

	fmt.Println("New cluster node created as", *createClusterRole, "in", config.Raft.Dir)
	return nil
}

func execJoinCluster(cmd *Command, args []string) error {
	config, err := ParseConfigFile(configFile)
	if err != nil {
		return err
	}

	if *joinClusterSeedServers == "" {
		return fmt.Errorf("At least 1 seedserver must be supplied")
	}

	if *joinClusterRole != "combined" && *joinClusterRole != "broker" && *joinClusterRole != "data" {
		return fmt.Errorf("Node must join as 'combined', 'broker', or 'data'")
	}
	if *joinClusterRole == "combined" || *joinClusterRole == "broker" {
		// Broker required -- but don't initialize it. Joining a cluster will
		// do that.
		b := messaging.NewBroker()
		if err := b.Open(config.Raft.Dir); err != nil {
			return fmt.Errorf("Failed to prepare to join cluster", err.Error())
		}
	}
	if *joinClusterRole == "combined" || *joinClusterRole == "data" {
		// do any required data-node stuff.
	}
	fmt.Println("Joined cluster at", *joinClusterSeedServers)
	return nil
}

func execRun(cmd *Command, args []string) error {
	// Write pid file.
	if pidFile != "" {
		pid := strconv.Itoa(os.Getpid())
		if err := ioutil.WriteFile(pidFile, []byte(pid), 0644); err != nil {
			panic(err)
		}
	}

	config, err := ParseConfigFile(configFile)
	if err != nil {
		return err
	}

	// Override config properties.
	if hostname != "" {
		config.Hostname = hostname
	}

	// TODO(benbjohnson): Start admin server.

	if config.BindAddress == "" {
		log4go.Info("Starting Influx Server %s...", version)
	} else {
		log4go.Info("Starting Influx Server %s bound to %s...", version, config.BindAddress)
	}
	fmt.Printf(logo)

	// Bring up the node in the state as is on disk.
	var (
		brokerURLs []*url.URL
		client     influxdb.MessagingClient
		server     *influxdb.Server
	)

	state, err := recoverState(filepath.Join(config.Cluster.Dir, stateFilename))
	if err != nil {
		return err
	}

	if state.Mode == "local" {
		client = messaging.NewLoopbackClient()
		log4go.Info("Local messaging client created")
		server = influxdb.NewServer(client)
		err := server.Open(config.Storage.Dir)
		if err != nil {
			return fmt.Errorf("failed to open local Server", err.Error())
		}
	} else {
		// If the Broker directory exists, open a Broker on this node.
		if _, err := os.Stat(config.Raft.Dir); err == nil {
			b := messaging.NewBroker()
			if err := b.Open(config.Raft.Dir); err != nil {
				return fmt.Errorf("failed to open Broker", err.Error())
			}
		} else {
			return fmt.Errorf("failed to check for Broker directory", err.Error())
		}
		// If a Data directory exists, open a Data node.
		if _, err := os.Stat(config.Storage.Dir); err == nil {
			// Create correct client here for connecting to Broker.
			c := messaging.NewClient("XXX-CHANGEME-XXX")
			if err := c.Open(brokerURLs); err != nil {
				log4go.Error("Error opening Messaging Client: %s", err.Error())
			}
			defer c.Close()
			client = c
			log4go.Info("Cluster messaging client created")
			server = influxdb.NewServer(client)
			err = server.Open(config.Storage.Dir)
			if err != nil {
				return fmt.Errorf("failed to open data Server", err.Error())
			}
		} else {
			return fmt.Errorf("failed to check for Broker directory", err.Error())
		}
		// TODO: startProfiler()
		// TODO: -reset-root
		// Start up HTTP server with correct endpoints. TODO
		h := influxdb.NewHandler(server)
		func() { log.Fatal(http.ListenAndServe(":8086", h)) }() // TODO: Change HTTP port.
		// TODO: Start HTTPS server.
	}

	// Wait indefinitely.
	<-(chan struct{})(nil)
	return nil
}

func execVersion(cmd *Command, args []string) error {
	v := fmt.Sprintf("InfluxDB v%s (git: %s)", version, commit)
	fmt.Println(v)
	return nil
}

// recoverState returns the high-level state of the node from the path on disk.
// If not state exists, the a "local"-mode state is returned.
func recoverState(path string) (*State, error) {
	// If state doesn't exist then return a "local" state.
	f, err := os.Open(path)
	if os.IsNotExist(err) {
		return &State{Mode: "local"}, err
	} else if err != nil {
		return nil, err
	}
	defer f.Close()

	// Decode state from file and return.
	s := &State{}
	if err := json.NewDecoder(f).Decode(&s); err != nil {
		return nil, err
	}

	return s, nil
}

// State represents high-level state of the node that persists across restarts.
type State struct {
	Mode string `json:"mode"`
}
