package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/tsdb/engine/tsm1"
	"github.com/retailnext/hllpp"
)

type reportOpts struct {
	dir      string
	pattern  string
	detailed bool
}

func cmdReport(opts *reportOpts) {
	start := time.Now()

	files, err := filepath.Glob(filepath.Join(opts.dir, fmt.Sprintf("*.%s", tsm1.TSMFileExtension)))
	if err != nil {
		fmt.Printf("%v\n", err)
		os.Exit(1)
	}

	var filtered []string
	if opts.pattern != "" {
		for _, f := range files {
			if strings.Contains(f, opts.pattern) {
				filtered = append(filtered, f)
			}
		}
		files = filtered
	}

	if len(files) == 0 {
		fmt.Printf("no tsm files at %v\n", opts.dir)
		os.Exit(1)
	}

	tw := tabwriter.NewWriter(os.Stdout, 8, 8, 1, '\t', 0)
	fmt.Fprintln(tw, strings.Join([]string{"File", "Series", "Load Time"}, "\t"))

	totalSeries := hllpp.New()
	tagCardialities := map[string]*hllpp.HLLPP{}
	measCardinalities := map[string]*hllpp.HLLPP{}
	fieldCardinalities := map[string]*hllpp.HLLPP{}

	ordering := make([]chan struct{}, 0, len(files))
	for range files {
		ordering = append(ordering, make(chan struct{}))
	}

	for _, f := range files {
		file, err := os.OpenFile(f, os.O_RDONLY, 0600)
		if err != nil {
			fmt.Fprintf(os.Stderr, "error: %s: %v. Skipping.\n", f, err)
			continue
		}

		loadStart := time.Now()
		reader, err := tsm1.NewTSMReader(file)
		if err != nil {
			fmt.Fprintf(os.Stderr, "error: %s: %v. Skipping.\n", file.Name(), err)
			continue
		}
		loadTime := time.Since(loadStart)

		seriesCount := reader.KeyCount()
		for i := 0; i < seriesCount; i++ {
			key, _ := reader.KeyAt(i)
			totalSeries.Add([]byte(key))

			if opts.detailed {
				sep := strings.Index(key, "#!~#")
				seriesKey, field := key[:sep], key[sep+4:]
				measurement, tags, _ := models.ParseKey(seriesKey)

				measCount, ok := measCardinalities[measurement]
				if !ok {
					measCount = hllpp.New()
					measCardinalities[measurement] = measCount
				}
				measCount.Add([]byte(key))

				fieldCount, ok := fieldCardinalities[measurement]
				if !ok {
					fieldCount = hllpp.New()
					fieldCardinalities[measurement] = fieldCount
				}
				fieldCount.Add([]byte(field))

				for t, v := range tags {
					tagCount, ok := tagCardialities[t]
					if !ok {
						tagCount = hllpp.New()
						tagCardialities[t] = tagCount
					}
					tagCount.Add([]byte(v))
				}
			}
		}
		reader.Close()

		fmt.Fprintln(tw, strings.Join([]string{
			filepath.Base(file.Name()),
			strconv.FormatInt(int64(seriesCount), 10),
			loadTime.String(),
		}, "\t"))
		tw.Flush()
	}

	tw.Flush()
	println()
	fmt.Printf("Statistics\n")
	fmt.Printf("  Series:\n")
	fmt.Printf("    Total (est): %d\n", totalSeries.Count())
	if opts.detailed {
		fmt.Printf("  Measurements (est):\n")
		for t, card := range measCardinalities {
			fmt.Printf("    %v: %d (%d%%)\n", t, card.Count(), int((float64(card.Count())/float64(totalSeries.Count()))*100))
		}

		fmt.Printf("  Fields (est):\n")
		for t, card := range fieldCardinalities {
			fmt.Printf("    %v: %d\n", t, card.Count())
		}

		fmt.Printf("  Tags (est):\n")
		for t, card := range tagCardialities {
			fmt.Printf("    %v: %d\n", t, card.Count())
		}
	}

	fmt.Printf("Completed in %s\n", time.Since(start))
}
