package tests

import (
	"io/ioutil"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	influx "github.com/influxdata/influxdb/cmd/influx/cli"
	"github.com/influxdata/influxdb/cmd/influx_inspect/export"
	"strconv"
)

func TestServer_ExportAndImport(t *testing.T) {
	t.Parallel()

	config := NewConfig()

	config.HTTPD.BindAddress = freePort()

	config.Data.Engine = "tsm1"
	config.Data.CacheSnapshotMemorySize = 1

	s := OpenServer(config)
	defer s.Close()

	if _, ok := s.(*RemoteServer); ok {
		t.Skip("Skipping. Cannot influx_inspect export on remote server")
	}

	exportDir, _ := ioutil.TempDir("", "backup")
	defer os.RemoveAll(exportDir)
	exportFile := filepath.Join(exportDir, "export")

	func() {
		tests := make([]Test, 2)
		tests = append(tests, Test{
			db: "db0",
			rp: "rp0",
			writes: Writes{
				&Write{data: "myseries,host=A value=23 1000000"},
				&Write{data: "myseries,host=B value=24 5000000"},
				&Write{data: "myseries,host=C value=25 9000000"},
			},
			queries: []*Query{
				&Query{
					name:    "Test data should from db0 be present",
					command: `SELECT * FROM "db0"."rp0"."myseries"`,
					exp:     `{"results":[{"statement_id":0,"series":[{"name":"myseries","columns":["time","host","value"],"values":[["1970-01-01T00:00:00.001Z","A",23],["1970-01-01T00:00:00.005Z","B",24],["1970-01-01T00:00:00.009Z","C",25]]}]}]}`,
				},
			},
		})

		tests = append(tests, Test{
			db: "db1",
			rp: "rp1",
			writes: Writes{
				&Write{data: "myseries,host=D value=13 1000000"},
				&Write{data: "myseries,host=E value=14 5000000"},
				&Write{data: "myseries,host=F value=15 9000000"},
			},
			queries: []*Query{
				&Query{
					name:    "Test data should from db1 be present",
					command: `SELECT * FROM "db1"."rp1"."myseries"`,
					exp:     `{"results":[{"statement_id":0,"series":[{"name":"myseries","columns":["time","host","value"],"values":[["1970-01-01T00:00:00.001Z","D",13],["1970-01-01T00:00:00.005Z","E",14],["1970-01-01T00:00:00.009Z","F",15]]}]}]}`,
				},
			},
		})

		for _, test := range tests {
			for _, query := range test.queries {
				t.Run(query.name, func(t *testing.T) {
					if !test.initialized {
						if err := test.init(s); err != nil {
							t.Fatalf("test init failed: %s", err)
						}
					}
					if query.skip {
						t.Skipf("SKIP:: %s", query.name)
					}
					if err := query.Execute(s); err != nil {
						t.Error(query.Error(err))
					} else if !query.success() {
						t.Error(query.failureMessage())
					}
				})
			}
		}
	}()

	// wait for the snapshot to write
	// Don't think we necessarily need this since influx_inspect export will
	// write from the WAL file as well.
	time.Sleep(time.Second)

	// Export the data we just wrote
	exportCmd := export.NewCommand()
	if err := exportCmd.Run("-out", exportFile, "-datadir", config.Data.Dir, "-waldir", config.Data.WALDir); err != nil {
		t.Fatalf("error exporting: %s", err.Error())
	}

	// Drop the database to start anew
	_, err := s.Query("DROP DATABASE db0")
	if err != nil {
		t.Fatalf("error dropping database: %s", err.Error())
	}

	// Import with influx -import

	// Nasty code to get the httpd service listener port
	host, port, err := net.SplitHostPort(config.HTTPD.BindAddress)
	if err != nil {
		t.Fatal(err)
	}
	portInt, err := strconv.Atoi(port)
	if err != nil {
		t.Fatal(err)
	}

	importCmd := influx.New("unknown")

	importCmd.Host = host
	importCmd.Port = portInt
	importCmd.Import = true
	importCmd.ClientConfig.Precision = "ns"
	importCmd.ImporterConfig.Path = exportFile

	if err := importCmd.Run(); err != nil {
		t.Fatalf("error importing: %s", err.Error())
	}

	func() {
		tests := make([]Test, 2)
		tests = append(tests, Test{
			db: "db0",
			rp: "rp0",
			queries: []*Query{
				&Query{
					name:    "Test data from db0 should have been imported",
					command: `SELECT * FROM "db0"."rp0"."myseries"`,
					exp:     `{"results":[{"statement_id":0,"series":[{"name":"myseries","columns":["time","host","value"],"values":[["1970-01-01T00:00:00.001Z","A",23],["1970-01-01T00:00:00.005Z","B",24],["1970-01-01T00:00:00.009Z","C",25]]}]}]}`,
				},
			},
		})

		tests = append(tests, Test{
			db: "db1",
			rp: "rp1",
			queries: []*Query{
				&Query{
					name:    "Test data from db1 should have been imported",
					command: `SELECT * FROM "db1"."rp1"."myseries"`,
					exp:     `{"results":[{"statement_id":0,"series":[{"name":"myseries","columns":["time","host","value"],"values":[["1970-01-01T00:00:00.001Z","D",13],["1970-01-01T00:00:00.005Z","E",14],["1970-01-01T00:00:00.009Z","F",15]]}]}]}`,
				},
			},
		})

		for _, test := range tests {
			for _, query := range test.queries {
				t.Run(query.name, func(t *testing.T) {
					if !test.initialized {
						if err := test.init(s); err != nil {
							t.Fatalf("test init failed: %s", err)
						}
					}
					if query.skip {
						t.Skipf("SKIP:: %s", query.name)
					}
					if err := query.Execute(s); err != nil {
						t.Error(query.Error(err))
					} else if !query.success() {
						t.Error(query.failureMessage())
					}
				})
			}
		}
	}()
}
