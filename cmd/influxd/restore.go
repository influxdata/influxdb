package main

import (
	"log"
)

func Restore(path, configPath string) {
	log.Printf("influxdb archive, version %s, commit %s", version, commit)
	panic("not yet implemented") // TODO
}

func printRestoreUsage() {
	log.Printf(`usage: influxd restore [flags] PATH

restore expands an archive into the data directory specified by the config.

        -config <path>
                          The path to the configuration file.
`)
}
