package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	p "path"

	"github.com/influxdb/influxdb/coordinator"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, `Usage:
  go run tools/inspect-raft/main.go inspect
  go run tools/inspect-raft/main.go change`,
		)
		os.Exit(1)
	}

	inspect(os.Args[1:])
}

func inspect(args []string) {
	flagSet := flag.NewFlagSet("inspect", flag.ExitOnError)
	path := flagSet.String("path", "", "Path to the raft directory")
	log := flagSet.Bool("log", false, "By default print the snapshot. If this flag is set, print the log instead")
	flagSet.Parse(args)

	if path == nil || *path == "" {
		fmt.Fprintln(os.Stderr, "Path must be set to a value. Run inspect -h for more info")
		os.Exit(1)
	}

	_, err := ioutil.ReadDir(p.Join(*path, "snapshot"))
	if err != nil {
		panic(err)
	}

	// TODO: open the snapshot
	if !*log {
		// s := load(*path)
		return
	}

	// Open the log file and print the commands
	logFile := p.Join(*path, "log.old")
	f, err := os.Open(logFile)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	w, err := os.OpenFile(p.Join(*path, "log"), os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0755)
	if err != nil {
		panic(err)
	}

	defer w.Close()

	raftPort := 8090
	protobufPort := 8099
	i := 0

	for {
		entry := &LogEntry{}
		if err := entry.Decode(f); err != nil {
			if err == io.EOF {
				break
			}
			panic(err)
		}

		cmd := &coordinator.InfluxJoinCommand{}

		if entry.LogEntry.GetCommandName() != "join" {
			goto write
		}

		err = json.Unmarshal(entry.Command, cmd)
		if err != nil {
			panic(err)
		}

		cmd.ConnectionString = fmt.Sprintf("http://localhost:%d", raftPort+i)
		cmd.ProtobufConnectionString = fmt.Sprintf("localhost:%d", protobufPort+i)
		entry.Command, err = json.Marshal(cmd)
		if err != nil {
			panic(err)
		}
		i += 10
	write:
		if err := entry.Encode(w); err != nil {
			panic(err)
		}
	}
}
