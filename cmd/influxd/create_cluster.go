package main

import (
	"flag"
	"log"
	"net/url"
	"os"
	"path/filepath"

	"github.com/influxdb/influxdb/messaging"
)

// execCreateCluster runs the "create-cluster" command.
func execCreateCluster(args []string) {
	// Parse command flags.
	fs := flag.NewFlagSet("", flag.ExitOnError)
	var (
		configPath = fs.String("config", configDefaultPath, "")
		role       = fs.String("role", "combined", "")
	)
	fs.Usage = printCreateClusterUsage
	fs.Parse(args)

	// Parse the configuration file.
	config, err := ParseConfigFile(*configPath)
	if err != nil {
		log.Fatalf("config: %s", err)
	}

	// Validate the role.
	if *role != "combined" && *role != "broker" {
		log.Fatal("new cluster node must be created as 'combined' or 'broker' role")
	}

	// Create the broker.
	b := messaging.NewBroker()
	if err := b.Open(config.Raft.Dir, config.RaftConnectionString()); err != nil {
		log.Fatalf("broker: %s", err.Error())
	}

	// Initialize the log for the first time.
	if err := b.Initialize(); err != nil {
		log.Fatalf("initialize: %s", err)
	}

	// If the node is a broker and data node then create the data directory.
	if *role == "combined" {
		if _, err := os.Stat(config.Cluster.Dir); err == nil {
			log.Fatalf("create-cluster: data directory already exists")
		}

		// Now create the storage directory.
		if err := os.MkdirAll(config.Storage.Dir, 0744); err != nil {
			log.Fatalf("create-cluster storage: %s", err.Error())
		}

		// Configure the Messaging Client such that this node connects to itself.
		var seedUrls []*url.URL

		u, err := url.Parse(config.RaftListenAddr())
		if err != nil {
			log.Fatalf("create-cluster seed URLs: %s", err.Error())
		}
		seedUrls = append(seedUrls, u)

		c := messaging.NewClient(0) // TODO: Set replica id.
		if err := c.Open(filepath.Join(config.Storage.Dir, messagingClientFile), seedUrls); err != nil {
			log.Fatalf("create-cluster open client: %s", err.Error())
		}
		if err := c.Close(); err != nil {
			log.Fatalf("create-cluster close client: %s", err.Error())
		}

	}

	log.Println("new cluster node created as", *role, "in", config.Raft.Dir)
}

func printCreateClusterUsage() {
	log.Printf(`usage: create-cluster [flags]

create-cluster creates a completely new node that can act as the first node of a new
cluster. This node must be created as a 'combined' or 'broker' node.

        -config <path>
                        Set the path to the configuration file. Defaults to %s.

        -role <role>
                        Set the role to be 'combined' or 'broker'. 'broker' means it will take part
                        in Raft Distributed Consensus. 'combined' means it take part in Raft and
                        store time-series data. The default role is 'combined'. Any other role other
                        than these two is invalid.
\n`, configDefaultPath)
}
