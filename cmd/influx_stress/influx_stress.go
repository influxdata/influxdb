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
	address = flag.String("addr", "", "IP address and port of database where response times will persist (e.g., localhost:8086)")
	tags    = flag.String("tags", "", "")

	//id   = flag.String("id", "", "ID for the test that is being ran")
	//name = flag.String("name", "", "name of the test that is being ran")

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

	w := stress.NewWriter(&c.Write.PointGenerators.Basic, &c.Write.InfluxClients.Basic)
	r := stress.NewQuerier(&c.Read.QueryGenerators.Basic, &c.Read.QueryClients.Basic)
	s := stress.NewStressTest(&c.Provision.Basic, w, r)

	bw := stress.NewBroadcastChannel()
	bw.Register(stress.BasicWriteHandler)

	br := stress.NewBroadcastChannel()
	br.Register(stress.BasicReadHandler)

	s.Start(bw.Handle, br.Handle)

	return

}
