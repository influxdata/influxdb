package main

import (
	"flag"
	"fmt"
	"os"
)

const logo = `
+---------------------------------------------+
|  _____        __ _            _____  ____   |
| |_   _|      / _| |          |  __ \|  _ \  |
|   | |  _ __ | |_| |_   ___  _| |  | | |_) | |
|   | | | '_ \|  _| | | | \ \/ / |  | |  _ <  |
|  _| |_| | | | | | | |_| |>  <| |__| | |_) | |
| |_____|_| |_|_| |_|\__,_/_/\_\_____/|____/  |
+---------------------------------------------+
`

// These variables are populated via the Go linker.
var (
	version string
	commit  string
)

func main() {
	if err := start(); err != nil {
		fmt.Println(err.Error())
		os.Exit(1)
	}
	os.Exit(0)
}

var usageMessageHeader = `
Configure and start an InfluxDB server.

Usage:

	influxd [[command] [arguments]]

The commands are:

`

func usage() {
	fmt.Fprintf(os.Stderr, usageMessageHeader)
	for _, c := range commands {
		fmt.Fprintf(os.Stderr, "    %-20s %-10s\n", c.Name, c.Terse)
	}
	fmt.Fprintf(os.Stderr, "\n\"run\" is the default command.\n")
	fmt.Fprintf(os.Stderr, "\nUse \"influxd help [command]\" for more information about a command.\n\n")
	os.Exit(2)
}

func help(args []string) {
	if len(args) == 0 {
		usage()
		return // Succeeded already at 'influxd help'
	}
	if len(args) != 1 {
		fmt.Fprintf(os.Stderr, "usage: influxd help command\n\nToo many arguments given.\n")
		os.Exit(2)
	}

	for _, cmd := range commands {
		if cmd.Name == args[0] {
			fmt.Fprintf(os.Stderr, "usage: %s %s\n", cmd.Name, cmd.Options)
			fmt.Fprintf(os.Stderr, "%s\n", cmd.Long)
			return // succeeded at 'influxd help command'
		}
	}

}

func start() error {
	flag.Usage = usage
	flag.Parse()

	args := flag.Args()
	if len(args) < 1 {
		args = append(args, "run")
	}

	if args[0] == "help" {
		help(args[1:])
		return nil
	}

	for _, cmd := range commands {
		if cmd.Name == args[0] {
			cmd.Flag.Usage = func() { cmd.Usage() }
			cmd.Flag.Parse(args[1:])
			args = cmd.Flag.Args()
			return cmd.Exec(cmd, args)
		}
	}

	return fmt.Errorf("influxd: unknown command %q\nRun 'influxd help' for usage", args[0])
}

type Stopper interface {
	Stop()
}
