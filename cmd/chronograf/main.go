package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"

	"github.com/influxdata/chronograf"
	"github.com/influxdata/chronograf/server"
	flags "github.com/jessevdk/go-flags"
)

// Build flags
var (
	version = ""
	commit  = ""
)

func main() {
	srv := server.Server{
		BuildInfo: server.BuildInfo{
			Version: version,
			Commit:  commit,
		},
	}

	parser := flags.NewParser(&srv, flags.Default)
	parser.ShortDescription = `Chronograf`
	parser.LongDescription = `Options for Chronograf`

	if _, err := parser.Parse(); err != nil {
		code := 1
		if fe, ok := err.(*flags.Error); ok {
			if fe.Type == flags.ErrHelp {
				code = 0
			}
		}
		os.Exit(code)
	}

	if srv.ShowVersion {
		log.Printf("Chronograf %s (git: %s)\n", version, commit)
		os.Exit(0)
	}

	if srv.NewSource != "" {
		type SourceServer struct {
			Source    chronograf.Source `json:"influxdb"`
			Kapacitor chronograf.Server `json:"kapacitor"`
		}
		var sourceServers []SourceServer
		err := json.Unmarshal([]byte(srv.NewSource), &sourceServers)
		if err != nil {
			fmt.Print(err)
		}

		/*
			// parse influxdb key from newsource into chronograf.Source struct from chronograf.go
			// parse kapacitor key from newsource into chronograf.Server struct from chronograf.go
			// open connection to boltDB
			// use sources.All to get all sources
			// if that influxdb does not exist (how to compare?)
			// use sourcesStore to add this new source
				// if successful
					// hold onto this new source id
					// if that kapacitor does not exist (how to compare?)
					// use serverStore to add new kapacitor, including new source id
				// else
					// throw error
			// else
			// do nothing
		*/
	}

	ctx := context.Background()
	if err := srv.Serve(ctx); err != nil {
		log.Fatalln(err)
	}
}
