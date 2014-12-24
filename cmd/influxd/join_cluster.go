package main

import (
	"flag"
	"log"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	"github.com/influxdb/influxdb/messaging"
)

// execJoinCluster runs the "join-cluster" command.
func execJoinCluster(args []string) {
	// Parse command flags.
	fs := flag.NewFlagSet("", flag.ExitOnError)
	var (
		configPath  = fs.String("config", configDefaultPath, "")
		role        = fs.String("role", "combined", "")
		seedServers = fs.String("seed-servers", "", "")
	)
	fs.Usage = printJoinClusterUsage
	fs.Parse(args)

	// Parse configuration.
	config, err := ParseConfigFile(*configPath)
	if err != nil {
		log.Fatalf("config: %s", err)
	}

	// Validate command line arguments.
	if *seedServers == "" {
		log.Fatalf("at least one seed server must be supplied")
	} else if *role != "combined" && *role != "broker" && *role != "data" {
		log.Fatalf("node must join as 'combined', 'broker', or 'data'")
	}

	var seedURLs []*url.URL
	for _, s := range strings.Split(*seedServers, ",") {
		u, err := url.Parse(s)
		if err != nil {
			log.Fatalf("seed server: %s", err)
		}
		seedURLs = append(seedURLs, u)
	}

	// If joining as broker then create broker.
	if *role == "combined" || *role == "broker" {
		// Broker required -- but don't initialize it.
		// Joining a cluster will do that.
		b := messaging.NewBroker()
		if err := b.Open(config.Raft.Dir, config.RaftConnectionString()); err != nil {
			log.Fatalf("join: %s", err)
		}

		// Loop through each, connecting to one must succeed.
		joined := false
		for _, s := range seedURLs {
			err := b.Join(s)
			if err != nil {
				log.Println("error: join failed to connect to", s, err)
			} else {
				log.Println("join: connected successfully to", s)
				joined = true
				break
			}
		}

		if !joined {
			log.Fatalf("join: failed to connect to any seed server")
		}
	}

	// If joining as a data node then create a data directory.
	if *role == "combined" || *role == "data" {
		if _, err := os.Stat(config.Storage.Dir); err == nil {
			log.Fatalf("join-cluster: storage directory already exists")
		}

		if err := os.MkdirAll(config.Storage.Dir, 0744); err != nil {
			log.Fatalf("join-cluster storage: %s", err.Error())
		}

		// Configure the Messaging Client.
		c := messaging.NewClient(0) // TODO: Set replica id.
		if err := c.Open(filepath.Join(config.Storage.Dir, messagingClientFile), seedURLs); err != nil {
			log.Fatalf("join-cluster open client: %s", err.Error())
		}
		if err := c.Close(); err != nil {
			log.Fatalf("join-cluster close client: %s", err.Error())
		}

	}

	log.Printf("joined cluster as '%s' at %s", *role, *seedServers)
}

func printJoinClusterUsage() {
	log.Printf(`usage: join-cluster [flags]

join-cluster creates a completely new node that will attempt to join an existing cluster.
        -config <path>
                        Set the path to the configuration file. Defaults to %s.

        -role <role>
                        Set the role to be 'combined', 'broker' or 'data'. broker' means it will take
                        part in Raft Distributed Consensus. 'data' means it will store time-series data.
                        'combined' means it will do both. The default is 'combined'. In role other than
                        these three is invalid.

        -seed-servers <servers>
                        Set the list of servers the node should contact, to join the cluster. This
                        should be comma-delimited list of servers, in the form host:port. This option
                        is REQUIRED.
\n`, configDefaultPath)
}
