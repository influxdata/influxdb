package main

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/influxdata/flux"
	"github.com/influxdata/flux/semantic/semantictest"
	"github.com/influxdata/platform"
	"github.com/influxdata/platform/mock"
	"github.com/influxdata/platform/query/influxql"
	platformtesting "github.com/influxdata/platform/testing"
)

func printUsage() {
	fmt.Println("usage: prepcsvtests /path/to/testfiles [testname]")
}

func main() {
	fnames := make([]string, 0)
	path := ""
	var err error
	if len(os.Args) == 3 {
		path = os.Args[1]
		fnames = append(fnames, filepath.Join(path, os.Args[2])+".flux")
	} else if len(os.Args) == 2 {
		path = os.Args[1]
		fnames, err = filepath.Glob(filepath.Join(path, "*.flux"))
		if err != nil {
			return
		}
	} else {
		printUsage()
		return
	}

	for _, fname := range fnames {
		ext := ".flux"
		testName := fname[0 : len(fname)-len(ext)]

		fluxText, err := ioutil.ReadFile(fname)
		if err != nil {
			fmt.Printf("error reading ifq	l query text: %s", err)
			return
		}

		influxqlText, err := ioutil.ReadFile(testName + ".influxql")
		if err != nil {
			fmt.Printf("error reading influxql query text: %s", err)
			return
		}

		fluxSpec, err := flux.Compile(context.Background(), string(fluxText), time.Now().UTC())
		if err != nil {
			fmt.Printf("error compiling. \n query: \n %s \n err: %s", string(fluxText), err)
			return
		}

		transpiler := influxql.NewTranspiler(dbrpMappingSvc)
		influxqlSpec, err := transpiler.Transpile(context.Background(), string(influxqlText))
		if err != nil {
			fmt.Printf("error transpiling. \n query: \n %s \n err: %s", string(influxqlText), err)
			return
		}
		var opts = append(
			semantictest.CmpOptions,
			cmp.AllowUnexported(flux.Spec{}),
			cmpopts.IgnoreUnexported(flux.Spec{}))

		difference := cmp.Diff(fluxSpec, influxqlSpec, opts...)

		fmt.Printf("compiled vs transpiled diff: \n%s", difference)
	}
}

// Setup mock DBRPMappingService to always return `db.rp`.
var dbrpMappingSvc = mock.NewDBRPMappingService()

func init() {
	mapping := platform.DBRPMapping{
		Cluster:         "cluster",
		Database:        "db",
		RetentionPolicy: "rp",
		Default:         true,
		OrganizationID:  platformtesting.MustIDBase16("aaaaaaaaaaaaaaaa"),
		BucketID:        platformtesting.MustIDBase16("bbbbbbbbbbbbbbbb"),
	}
	dbrpMappingSvc.FindByFn = func(ctx context.Context, cluster string, db string, rp string) (*platform.DBRPMapping, error) {
		return &mapping, nil
	}
	dbrpMappingSvc.FindFn = func(ctx context.Context, filter platform.DBRPMappingFilter) (*platform.DBRPMapping, error) {
		return &mapping, nil
	}
	dbrpMappingSvc.FindManyFn = func(ctx context.Context, filter platform.DBRPMappingFilter, opt ...platform.FindOptions) ([]*platform.DBRPMapping, int, error) {
		return []*platform.DBRPMapping{&mapping}, 1, nil
	}
}
