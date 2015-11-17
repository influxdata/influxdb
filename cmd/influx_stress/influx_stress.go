package main

import (
	"flag"
	"fmt"
	"os"
	"runtime/pprof"

	"github.com/influxdb/influxdb/stress"
)

var (
	//database  = flag.String("database", "", "name of database")
	//address   = flag.String("addr", "", "IP address and port of database (e.g., localhost:8086)")

	config     = flag.String("config", "", "The stress test file")
	cpuprofile = flag.String("cpuprofile", "", "Write the cpu profile to `filename`")
)

func main() {

	flag.Parse()

	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			fmt.Println(err)
			return
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	c, err := stress.NewConfig(*config)
	if err != nil {
		fmt.Println(err)
		return
	}

	stress.Run(c)

	return

}
