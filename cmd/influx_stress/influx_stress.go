package main

import (
	"flag"
	"fmt"
	"os"
	"runtime/pprof"
	"strings"

	"github.com/influxdb/influxdb/stress"
)

var (
	config     = flag.String("config", "", "The stress test file")
	cpuprofile = flag.String("cpuprofile", "", "Write the cpu profile to `filename`")

	// id   = flag.String("id", "", "ID for the test that is being ran")
	// name = flag.String("name", "", "name of the test that is being ran")
)

type outputConfig struct {
	tags     map[string]string
	addr     string
	database string
}

func (t *outputConfig) SetParams(addr, db string) {
	t.addr = addr
	t.database = db
}

func NewOutputConfig() *outputConfig {
	var o outputConfig
	tags := make(map[string]string)
	o.tags = tags
	database := flag.String("database", "stress", "name of database")
	address := flag.String("addr", "http://localhost:8086", "IP address and port of database where response times will persist (e.g., localhost:8086)")
	flag.Var(&o, "tags", "A comma seperated list of tags")
	flag.Parse()

	o.SetParams(*address, *database)

	return &o

}

// FIX
func (t *outputConfig) String() string {
	var s string
	for k, v := range t.tags {
		s += fmt.Sprintf("%v=%v ", k, v)
	}
	return fmt.Sprintf("%v %v %v", s, t.database, t.addr)
}

func (t *outputConfig) Set(value string) error {
	for _, s := range strings.Split(value, ",") {
		tags := strings.Split(s, "=")
		t.tags[tags[0]] = tags[1]
	}
	return nil
}

func main() {
	o := NewOutputConfig()

	fmt.Println(o)
	return
	// flag.Parse()

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
	bw.Register(stress.WriteHTTPHandler)

	br := stress.NewBroadcastChannel()
	br.Register(stress.BasicReadHandler)

	s.Start(bw.Handle, br.Handle)

	return

}
