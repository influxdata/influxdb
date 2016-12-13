package main

import (
	"log"
	"os"

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

	if err := srv.Serve(); err != nil {
		log.Fatalln(err)
	}
}
