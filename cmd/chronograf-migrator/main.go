package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"strings"

	"github.com/influxdata/influxdb/v2/chronograf"
	"github.com/influxdata/influxdb/v2/chronograf/bolt"
	"github.com/influxdata/influxdb/v2/pkger"
)

var chronografDBPath string
var outputFile string

func exec(dbPath, out string) error {
	logger := log.New(os.Stdout, "", 0)

	c := bolt.NewClient()
	c.Path = dbPath

	ctx := context.Background()

	if err := c.Open(ctx, nil, chronograf.BuildInfo{}); err != nil {
		return err
	}

	dashboardStore := c.DashboardsStore

	ds, err := dashboardStore.All(ctx)
	if err != nil {
		return err
	}

	pkg := &pkger.Pkg{
		Objects: make([]pkger.Object, 0),
	}

	hasVar := map[string]bool{}
	for _, d1 := range ds {
		d2, vs, err := Convert1To2Dashboard(d1)
		if err != nil {
			return err
		}

		pkg.Objects = append(pkg.Objects, pkger.DashboardToObject(d2.Name, d2))

		for _, v := range vs {
			name := strings.ToLower(v.Name)
			if hasVar[name] {
				// TODO(desa): not sure what we actually want to do here
				logger.Printf("Found duplicate variables with name %q skipping\n", name)
				continue
			}
			hasVar[name] = true

			pkg.Objects = append(pkg.Objects, pkger.VariableToObject(name, v))
		}
	}

	f, err := os.Create(out)
	if err != nil {
		return err
	}
	defer f.Close()

	b, err := pkg.Encode(pkger.EncodingYAML)
	if err != nil {
		return err
	}
	_, err = io.Copy(f, bytes.NewReader(b))
	return err
}

func main() {
	flag.StringVar(&chronografDBPath, "db", "", "path to the chronograf database")
	flag.StringVar(&outputFile, "output", "dashboards.yml", "path to the output yaml file")
	flag.Parse()

	if chronografDBPath == "" {
		fmt.Fprintln(os.Stdout, "must supply db flag")
		return
	}

	if err := exec(chronografDBPath, outputFile); err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
	}
}
