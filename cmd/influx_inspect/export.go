package main

import (
	"bufio"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"

	"github.com/influxdata/influxdb/tsdb/engine/tsm1"
)

func cmdExport(path string) {
	dataPath := filepath.Join(path, "data")

	// No need to do this in a loop
	ext := fmt.Sprintf(".%s", tsm1.TSMFileExtension)

	// Get all TSM files by walking through the data dir
	files := []string{}
	err := filepath.Walk(dataPath, func(path string, f os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if filepath.Ext(path) == ext {
			files = append(files, path)
		}
		return nil
	})
	if err != nil {
		panic(err)
	}

	// Loop through each file and output all of the data
	for _, f := range files {
		file, err := os.OpenFile(f, os.O_RDONLY, 0600)

		relPath, _ := filepath.Rel(dataPath, f)
		dirs := strings.Split(relPath, string(byte(os.PathSeparator)))
		database := dirs[0]
		fmt.Println("Opening", f, "in database", database)
		temp, _ := ioutil.TempDir("/tmp", "influxdb")

		if err != nil {
			fmt.Printf("%v", err)
			os.Exit(1)
		}

		reader, err := tsm1.NewTSMReader(file)

		if err != nil {
			fmt.Printf("%v", err)
			os.Exit(1)
		}

		filename := temp + "/" + database + ".lp"

		of, _ := os.Create(filename)
		defer of.Close()
		w := bufio.NewWriter(of)

		fmt.Println("Writing", reader.KeyCount(), "series to", filename)

		for i := 0; i < reader.KeyCount(); i++ {
			var pairs string
			key, typ := reader.KeyAt(i)
			values, _ := reader.ReadAll(key)
			split := strings.Split(key, "#!~#")
			measurement, field := split[0], split[1]

			for _, value := range values {
				switch typ {
				case tsm1.BlockFloat64:
					pairs = field + "=" + fmt.Sprintf("%v", value.Value())
				case tsm1.BlockInteger:
					pairs = field + "=" + fmt.Sprintf("%vi", value.Value())
				case tsm1.BlockBoolean:
					pairs = field + "=" + fmt.Sprintf("%v", value.Value())
				case tsm1.BlockString:
					pairs = field + "=" + fmt.Sprintf("\"%v\"", value.Value())
				default:
					pairs = field + "=" + fmt.Sprintf("%v", value.Value())
				}

				fmt.Fprintln(w, measurement, pairs, value.UnixNano())
			}

			w.Flush()
		}
	}
}
