package main

import (
	"log"
	"os"

	"github.com/influxdata/chronograf/server"
	flags "github.com/jessevdk/go-flags"
)

// Build flags
var (
	Version = ""
	Commit  = ""
)

func main() {
	srv := server.Server{
		BuildInfo: server.BuildInfo{
			Version: Version,
			Commit:  Commit,
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

	if err := srv.Serve(); err != nil {
		log.Fatalln(err)
	}
}
