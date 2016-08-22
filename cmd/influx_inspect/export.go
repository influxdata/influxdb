package main

import (
	"compress/gzip"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/tsdb/engine/tsm1"
)

type cmdExport struct {
	path            string
	out             string
	db              string
	retentionPolicy string
	startTime       int64
	endTime         int64
	compress        bool

	ext   string
	files map[string][]string
}

func newCmdExport(path, out, db, retentionPolicy, startTime, endTime string, compress bool) (*cmdExport, error) {
	var start, end int64
	if startTime != "" {
		s, err := time.Parse(time.RFC3339, startTime)
		if err != nil {
			return nil, err
		}
		start = s.UnixNano()
	} else {
		start = math.MinInt64
	}
	if endTime != "" {
		e, err := time.Parse(time.RFC3339, endTime)
		if err != nil {
			return nil, err
		}
		end = e.UnixNano()
	} else {
		// set end time to max if it is not set.
		end = math.MaxInt64
	}

	return &cmdExport{
		path:            filepath.Join(path, "data"),
		out:             out,
		db:              db,
		compress:        compress,
		ext:             fmt.Sprintf(".%s", tsm1.TSMFileExtension),
		retentionPolicy: retentionPolicy,
		startTime:       start,
		endTime:         end,
		files:           make(map[string][]string),
	}, nil
}

func (c *cmdExport) validate() error {
	// validate args
	if c.retentionPolicy != "" && c.db == "" {
		return fmt.Errorf("must specify a db")
	}
	if c.startTime != 0 && c.endTime != 0 && c.endTime < c.startTime {
		return fmt.Errorf("end time before start time")
	}
	return nil
}

func (c *cmdExport) run() error {
	if err := c.validate(); err != nil {
		return err
	}

	return c.export()
}

func (c *cmdExport) export() error {
	if err := c.walkFiles(); err != nil {
		return err
	}
	return c.writeFiles()
}

func (c *cmdExport) walkFiles() error {
	err := filepath.Walk(c.path, func(path string, f os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if filepath.Ext(path) == c.ext {
			//files = append(files, path)
			relPath, _ := filepath.Rel(c.path, path)
			dirs := strings.Split(relPath, string(byte(os.PathSeparator)))
			if dirs[0] == c.db || c.db == "" {
				if dirs[1] == c.retentionPolicy || c.retentionPolicy == "" {
					key := filepath.Join(dirs[0], dirs[1])
					files := c.files[key]
					if files == nil {
						files = []string{}
					}
					c.files[key] = append(files, path)
				}
			}
		}
		return nil
	})
	if err != nil {
		log.Fatal(err)
	}
	return nil
}

func (c *cmdExport) writeFiles() error {
	// open our output file and create an output buffer
	var w io.WriteCloser
	w, err := os.Create(c.out)
	if err != nil {
		return err
	}
	defer w.Close()
	if c.compress {
		w = gzip.NewWriter(w)
		defer w.Close()
	}

	s, e := time.Unix(0, c.startTime).Format(time.RFC3339), time.Unix(0, c.endTime).Format(time.RFC3339)
	fmt.Fprintf(w, "# INFLUXDB EXPORT: %s - %s\n", s, e)

	// Write out all the DDL
	fmt.Fprintln(w, "# DDL")
	for key, _ := range c.files {
		keys := strings.Split(key, string(byte(os.PathSeparator)))
		fmt.Fprintf(w, "CREATE DATABASE %s\n", keys[0])
		fmt.Fprintf(w, "CREATE RETENTION POLICY %s ON %s DURATION inf REPLICATION 1\n", keys[1], keys[0])
	}

	fmt.Fprintln(w, "# DML")
	for key, files := range c.files {
		keys := strings.Split(key, string(byte(os.PathSeparator)))
		fmt.Fprintf(w, "# CONTEXT-DATABASE:%s\n", keys[0])
		fmt.Fprintf(w, "# CONTEXT-RETENTION-POLICY:%s\n", keys[1])
		for _, f := range files {
			// use an anonymous function here to close the files in the defers and not let them
			// accumulate in the loop
			if err := func(f string) error {
				file, err := os.OpenFile(f, os.O_RDONLY, 0600)
				if err != nil {
					return fmt.Errorf("%v", err)
				}
				defer file.Close()
				reader, err := tsm1.NewTSMReader(file)
				if err != nil {
					log.Printf("unable to read %s, skipping\n", f)
					return nil
				}
				defer reader.Close()

				if sgStart, sgEnd := reader.TimeRange(); sgStart > c.endTime || sgEnd < c.startTime {
					return nil
				}

				for i := 0; i < reader.KeyCount(); i++ {
					var pairs string
					key, typ := reader.KeyAt(i)
					values, _ := reader.ReadAll(string(key))
					measurement, field := tsm1.SeriesAndFieldFromCompositeKey(key)

					for _, value := range values {
						if (value.UnixNano() < c.startTime) || (value.UnixNano() > c.endTime) {
							continue
						}

						switch typ {
						case tsm1.BlockFloat64:
							pairs = field + "=" + fmt.Sprintf("%v", value.Value())
						case tsm1.BlockInteger:
							pairs = field + "=" + fmt.Sprintf("%vi", value.Value())
						case tsm1.BlockBoolean:
							pairs = field + "=" + fmt.Sprintf("%v", value.Value())
						case tsm1.BlockString:
							pairs = field + "=" + fmt.Sprintf("%q", models.EscapeStringField(fmt.Sprintf("%s", value.Value())))
						default:
							pairs = field + "=" + fmt.Sprintf("%v", value.Value())
						}

						fmt.Fprintln(w, string(measurement), pairs, value.UnixNano())
					}
				}
				return nil
			}(f); err != nil {
				return err
			}
		}
		_ = key
	}
	return nil
}
