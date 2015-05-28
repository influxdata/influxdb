package run

import (
	"log"
	"os"

	"github.com/influxdb/influxdb"
)

type RunCommand struct {
	// The logger passed to the ticker during execution.
	logWriter *os.File
	config    *influxdb.Config
	hostname  string
	node      *Node
}

func NewRunCommand() *RunCommand {
	return &RunCommand{
		node: &Node{},
	}
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
