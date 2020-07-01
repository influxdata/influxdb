package main

import (
	"fmt"
	"os"
	"strings"

	"github.com/influxdata/influxdb/v2/kv/migration"
	"github.com/influxdata/influxdb/v2/kv/migration/all"
)

var usageMsg = "Usage: kvmigrate create <migration name / description>"

func usage() {
	fmt.Println(usageMsg)
	os.Exit(1)
}

func main() {
	if len(os.Args) < 3 {
		usage()
	}

	if os.Args[1] != "create" {
		fmt.Printf("unrecognized command %q\n", os.Args[1])
		usage()
	}

	if err := migration.CreateNewMigration(all.Migrations[:], strings.Join(os.Args[2:], " ")); err != nil {
		panic(err)
	}
}
