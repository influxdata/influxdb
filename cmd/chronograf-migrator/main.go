package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/influxdata/influxdb/chronograf"
	"github.com/influxdata/influxdb/chronograf/bolt"
	"github.com/influxdata/influxdb/pkger"
	"gopkg.in/yaml.v3"
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
		APIVersion: pkger.APIVersion,
		Kind:       pkger.KindPackage,
		Metadata: pkger.Metadata{
			Description: "Dashboards from 1.x chronograf that are migrated to the new format",
			Name:        "Migrated Dashboards",
			Version:     "1",
		},
		Spec: struct {
			Resources []pkger.Resource `yaml:"resources" json:"resources"`
		}{
			Resources: make([]pkger.Resource, 0),
		},
	}

	hasVar := map[string]bool{}
	for _, d1 := range ds {
		d2, vs, err := Convert1To2Dashboard(d1)
		if err != nil {
			return err
		}

		pkg.Spec.Resources = append(pkg.Spec.Resources, pkger.DashboardToResource(d2, d2.Name))

		for _, v := range vs {
			name := strings.ToLower(v.Name)
			if hasVar[name] {
				// TODO(desa): not sure what we actually want to do here
				logger.Printf("Found duplicate variables with name %q skipping\n", name)
				continue
			}
			hasVar[name] = true

			pkg.Spec.Resources = append(pkg.Spec.Resources, pkger.VariableToResource(v, name))
		}
	}

	f, err := os.Create(out)
	if err != nil {
		return err
	}
	defer f.Close()

	if err := yaml.NewEncoder(f).Encode(pkg); err != nil {
		return err
	}

	return nil
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
