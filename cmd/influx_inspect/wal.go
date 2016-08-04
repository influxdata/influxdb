package main

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"

	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/tsdb/engine/tsm1"
)

type exportWALOpts struct {
	path string
}

func cmdExportWAL(opts *exportWALOpts) {
	var w io.Writer

	w = os.Stdout

	names, err := filepath.Glob(filepath.Join(opts.path, fmt.Sprintf("%s*.%s", tsm1.WALFilePrefix, tsm1.WALFileExtension)))
	if err != nil {
		fmt.Fprintf(os.Stderr, err.Error())
		os.Exit(1)
	}
	sort.Strings(names)

	for _, name := range names {
		f, err := os.Open(name)
		if err != nil {
			fmt.Fprintf(os.Stderr, err.Error())
			os.Exit(1)
		}
		defer f.Close()

		r := tsm1.NewWALSegmentReader(f)
		for r.Next() {
			entry, err := r.Read()
			if err != nil {
				n := r.Count()
				fmt.Fprintf(os.Stderr, "file %s corrupt at position %d", f.Name(), n)
				break
			}

			switch t := entry.(type) {
			case *tsm1.WriteWALEntry:
				var pairs string

				for key, values := range t.Values {
					measurement, field := tsm1.SeriesAndFieldFromCompositeKey(key)
					for _, value := range values {

						switch value.Value().(type) {
						case float64:
							pairs = field + "=" + fmt.Sprintf("%v", value.Value())
						case int64:
							pairs = field + "=" + fmt.Sprintf("%vi", value.Value())
						case bool:
							pairs = field + "=" + fmt.Sprintf("%v", value.Value())
						case string:
							pairs = field + "=" + fmt.Sprintf("%q", models.EscapeStringField(fmt.Sprintf("%s", value.Value())))
						default:
							pairs = field + "=" + fmt.Sprintf("%v", value.Value())
						}
						fmt.Fprintln(w, measurement, pairs, value.UnixNano())
					}
				}
			}
		}
	}
}
