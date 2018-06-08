package main

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/influxdata/platform/query"
	"github.com/influxdata/platform/query/influxql"
	"github.com/influxdata/platform/query/semantic/semantictest"

	"github.com/google/go-cmp/cmp"
	"golang.org/x/text/unicode/norm"
)

func normalizeString(s string) []byte {
	result := norm.NFC.String(strings.TrimSpace(s))
	re := regexp.MustCompile(`\r?\n`)
	return []byte(re.ReplaceAllString(result, "\r\n"))
}

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

		fluxSpec, err := query.Compile(context.Background(), string(fluxText))
		if err != nil {
			fmt.Printf("error compiling. \n query: \n %s \n err: %s", string(fluxText), err)
			return
		}

		transpiler := influxql.NewTranspiler()
		influxqlSpec, err := transpiler.Transpile(context.Background(), string(influxqlText))
		if err != nil {
			fmt.Printf("error transpiling. \n query: \n %s \n err: %s", string(influxqlText), err)
			return
		}
		var opts = append(
			semantictest.CmpOptions,
			cmp.AllowUnexported(query.Spec{}),
			cmpopts.IgnoreUnexported(query.Spec{}))

		difference := cmp.Diff(fluxSpec, influxqlSpec, opts...)

		fmt.Printf("compiled vs transpiled diff: \n%s", difference)
	}
}
