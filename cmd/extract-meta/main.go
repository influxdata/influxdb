// extract-meta extracts the v1_tsm1_metadata value from an InfluxDB BoltDB file
// and writes it to a file in binary format, or optionally as JSON.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"

	"github.com/influxdata/influxdb/v2/v1/services/meta"
	bolt "go.etcd.io/bbolt"
)

func main() {
	var (
		boltPath   string
		outputPath string
		asJSON     bool
	)

	flag.StringVar(&boltPath, "bolt", "", "path to the BoltDB file (e.g., influxd.bolt)")
	flag.StringVar(&outputPath, "out", "", "output file path; use - for stdout (default: stdout for JSON, meta.db.bin for binary)")
	flag.BoolVar(&asJSON, "json", false, "output as JSON instead of binary")
	flag.Parse()

	if boltPath == "" {
		fmt.Fprintln(os.Stderr, "error: -bolt flag is required")
		flag.Usage()
		os.Exit(1)
	}

	if err := run(boltPath, outputPath, asJSON); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}

func run(boltPath, outputPath string, asJSON bool) error {
	db, err := bolt.Open(boltPath, 0600, &bolt.Options{ReadOnly: true})
	if err != nil {
		return fmt.Errorf("failed to open bolt db %s: %w", boltPath, err)
	}
	defer db.Close()

	var data []byte

	err = db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(meta.BucketName)
		if bucket == nil {
			return fmt.Errorf("bucket %q not found", string(meta.BucketName))
		}

		val := bucket.Get([]byte(meta.Filename))
		if val == nil {
			return fmt.Errorf("key %q not found in bucket %q", meta.Filename, string(meta.BucketName))
		}

		// Copy the value since it's only valid for the duration of the transaction
		data = make([]byte, len(val))
		copy(data, val)

		return nil
	})
	if err != nil {
		return err
	}

	if asJSON {
		// Unmarshal the protobuf data
		var metaData meta.Data
		if err := metaData.UnmarshalBinary(data); err != nil {
			return fmt.Errorf("failed to unmarshal metadata: %w", err)
		}

		// Marshal to JSON
		jsonData, err := json.MarshalIndent(&metaData, "", "  ")
		if err != nil {
			return fmt.Errorf("failed to marshal to JSON: %w", err)
		}

		if outputPath == "" || outputPath == "-" {
			// Write to stdout
			if _, err = os.Stdout.Write(jsonData); err != nil {
				return fmt.Errorf("failed to write JSON to stdout: %w", err)
			}
		} else {
			if err := os.WriteFile(outputPath, jsonData, 0644); err != nil {
				return fmt.Errorf("failed to write output file %s: %w", outputPath, err)
			}
			fmt.Fprintf(os.Stderr, "wrote JSON to %s\n", outputPath)
		}
	} else {
		// Binary output
		if outputPath == "-" {
			// Write to stdout
			if _, err := os.Stdout.Write(data); err != nil {
				return fmt.Errorf("failed to write binary to stdout: %w", err)
			}
		} else {
			if outputPath == "" {
				outputPath = "meta.db.bin"
			}
			if err := os.WriteFile(outputPath, data, 0644); err != nil {
				return fmt.Errorf("failed to write output file %s: %w", outputPath, err)
			}
			fmt.Printf("extracted %d bytes from %s[%s][%s] to %s\n",
				len(data), boltPath, string(meta.BucketName), meta.Filename, outputPath)
		}
	}

	return nil
}
